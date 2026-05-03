#include "ublkpp/drivers/iscsi_disk.hpp"

#include <coroutine>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublksrv.h>

extern "C" {
#include <iscsi/iscsi.h>
#include <iscsi/scsi-lowlevel.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>
}

#include <ublkpp/lib/cqe_state.hpp>
#include <ublkpp/lib/disk_task.hpp>
#include "lib/logging.hpp"

SISL_LOGGING_DEF(libiscsi)

namespace ublkpp {

/// TODO Should be discoverable from Inquiry Pages
constexpr auto k_physical_block_size = 4 * Ki;

// Bit-63 marker on SQE user_data identifies a target-owned CQE (cqe_state*); matches the
// encoding in src/target/ublkpp_tgt.cpp.
static constexpr uint64_t k_target_bit = 1ULL << 63;

struct iscsi_session {
    ~iscsi_session() {
        if (url) iscsi_destroy_url(url);
        if (ctx) {
            iscsi_logout_sync(ctx);
            iscsi_destroy_context(ctx);
        }
    }

    iscsi_context* ctx{nullptr};
    iscsi_url* url{nullptr};
};

// Per-queue async service: owns one libiscsi context, an eventfd used to wake the service-loop
// coroutine when async_iov queues new work, the cqe_state the loop awaits on, and the suspended
// coroutine handle itself.
//
// Lifetime: created lazily in iSCSIDisk::prepare(q) on the queue thread, destroyed by the
// iSCSIDisk destructor (which runs after queue threads have joined). Destroying the suspended
// coroutine_handle frees the frame; iscsi_logout_sync drives its own internal poll loop and
// does not depend on the service loop coroutine.
struct queue_service {
    iscsi_context* ctx{nullptr};
    int evfd{-1};
    cqe_state poll_state{};
    std::coroutine_handle<> service_handle{};

    queue_service() = default;
    queue_service(queue_service const&) = delete;
    queue_service& operator=(queue_service const&) = delete;

    ~queue_service() {
        // Destroy the suspended service-loop frame first; safe because per-IO are already drained
        // (umount-gated del_dev) and the kernel silently cancels the in-flight POLL_ADD when the
        // io_uring is torn down by ublksrv_queue_deinit before this destructor runs.
        if (service_handle) service_handle.destroy();
        if (ctx) {
            iscsi_logout_sync(ctx);
            iscsi_destroy_context(ctx);
        }
        if (evfd >= 0) close(evfd);
    }
};

// libiscsi level scheme (per its public iscsi.h):
//   0 disabled, 1 errors only, 2 connection info, 3 user vars, 4+ function calls
// Level 1 surfaces SCSI sense returns including benign UNIT_ATTENTION/BUS_RESET
// (asserted by every target on session establishment); treat as WARN, not ERROR.
// Real fatal errors are reported separately via iscsi_get_error() at our call sites.
static void iscsi_log(int level, const char* message) {
    if (1 >= level) {
        LOGWARNMOD(libiscsi, "{}", message);
    } else if (2 == level) {
        LOGINFOMOD(libiscsi, "{}", message);
    } else if (3 == level) {
        LOGDEBUGMOD(libiscsi, "{}", message);
    } else {
        LOGTRACEMOD(libiscsi, "{}", message);
    }
}

static std::unique_ptr< iscsi_session > create_iscsi_session(std::string const& url) {
    auto session = std::make_unique< iscsi_session >();
    DEBUG_ASSERT(session, "Failed to allocate iSCSI session!");

    if (session->ctx = iscsi_create_context("iqn.2002-10.com.ublkpp:client"); !session->ctx) {
        DLOGE("failed to init context")
        return nullptr;
    }
    iscsi_set_log_level(session->ctx, (spdlog::level::level_enum::critical - module_level_ublk_drivers) * 2);
    iscsi_set_log_fn(session->ctx, iscsi_log);

    if (iscsi_set_alias(session->ctx, "ublkpp")) return nullptr;

    session->url = iscsi_parse_full_url(session->ctx, url.data());
    if (!session->url) {
        DLOGE("{}", iscsi_get_error(session->ctx))
        return nullptr;
    }

    return session;
}

static bool iscsi_login(iscsi_session& session) {
    iscsi_set_session_type(session.ctx, ISCSI_SESSION_NORMAL);
    iscsi_set_header_digest(session.ctx, ISCSI_HEADER_DIGEST_NONE);
    iscsi_set_targetname(session.ctx, session.url->target);

    if (0 != iscsi_full_connect_sync(session.ctx, session.url->portal, session.url->lun)) {
        DLOGE("{}", iscsi_get_error(session.ctx))
        return false;
    }
    return (0 != iscsi_is_logged_in(session.ctx));
}

static std::pair< uint64_t, uint32_t > probe_topology(iscsi_session& session) {
    auto task = iscsi_readcapacity16_sync(session.ctx, session.url->lun);
    if (!task || task->status != SCSI_STATUS_GOOD) {
        DLOGE("Failed to send readcapacity16 command");
        if (task) { scsi_free_scsi_task(task); }
        return std::make_pair(0UL, 0U);
    }

    uint64_t capacity{0UL};
    uint32_t block_size{0U};
    if (auto rc16 = static_cast< scsi_readcapacity16* >(scsi_datain_unmarshall(task)); rc16) {
        capacity = rc16->block_length * (rc16->returned_lba + 1);
        block_size = rc16->block_length;
        DLOGD("Logged into LUN with [sz:{}|bs:{}]", capacity, block_size)
    } else
        DLOGE("Failed to unmarshall ReadCapacity16 data")
    scsi_free_scsi_task(task);
    return std::make_pair(capacity, block_size);
}

iSCSIDisk::iSCSIDisk(std::string const& url) {
    direct_io = true;
    uses_queue_uring = true; // service loop and async_iov stage SQEs into q->ring_ptr

    if (_sync_session = create_iscsi_session(url); !_sync_session)
        throw std::runtime_error(fmt::format("Failed to attach iSCSI target: {}", url));
    if (!iscsi_login(*_sync_session)) throw std::runtime_error("Could not login to target");
    _lun = _sync_session->url->lun;

    auto const [capacity, lba_size] = probe_topology(*_sync_session);
    if (0 == capacity) throw std::runtime_error("Could not probe LUN to discover capacity");
    auto const block_shift = ilog2(lba_size);

    auto& our_params = *params();
    our_params.basic.logical_bs_shift = static_cast< uint8_t >(block_shift);
    our_params.basic.physical_bs_shift = static_cast< uint8_t >(block_shift);
    our_params.basic.dev_sectors = capacity >> SECTOR_SHIFT;

    // TODO Implement discard
    our_params.types |= ~UBLK_PARAM_TYPE_DISCARD;

    DLOGD("iSCSI device [{}:{}:{}]!", _sync_session->url->target, lba_size, k_physical_block_size)
}

iSCSIDisk::~iSCSIDisk() = default;

std::string iSCSIDisk::id() const noexcept { return _sync_session->url->target; }

queue_service* iSCSIDisk::__get_service(ublksrv_queue const* q) const {
    auto lk = std::shared_lock< std::shared_mutex >(_services_mtx);
    auto it = _services.find(q->q_id);
    return (it == _services.end()) ? nullptr : it->second.get();
}

static inline auto next_sqe(ublksrv_queue const* q) {
    auto r = q->ring_ptr;
    if (0 == io_uring_sq_space_left(r)) [[unlikely]]
        io_uring_submit(r); // LCOV_EXCL_LINE
    return io_uring_get_sqe(r);
}

// Service-loop coroutine: pumps libiscsi events through io_uring POLL_ADD on the queue thread.
//
// Behaviour:
//   - Compute iscsi_which_events(); if non-zero, POLL_ADD on the libiscsi socket fd.
//   - If zero, POLL_ADD on the queue_service's eventfd to be woken by async_iov.
//   - Suspend on cqe_state until run_queue_loop dispatches the CQE.
//   - On wake: drain eventfd if applicable, then call iscsi_service(revents) to advance libiscsi.
//
// This coroutine never returns; it is owned by queue_service::service_handle and destroyed
// (frame freed) by the queue_service destructor at iSCSIDisk teardown. It uses cqe_state
// with _owner = nullptr (the stand-alone form documented in cqe_state.hpp); errors thrown
// out of resume are swallowed by the catch in run_queue_loop because _owner is null.
static disk_task< int > __service_loop(ublksrv_queue const* q, queue_service* qs) {
    int const ifd = iscsi_get_fd(qs->ctx);
    while (true) {
        int events = iscsi_which_events(qs->ctx);
        int wait_fd;
        int wait_events;
        bool waiting_on_evfd;
        if (0 == events) {
            wait_fd = qs->evfd;
            wait_events = POLLIN;
            waiting_on_evfd = true;
        } else {
            wait_fd = ifd;
            wait_events = events;
            waiting_on_evfd = false;
        }

        qs->poll_state._result = 0;
        qs->poll_state._result_ready = false;
        qs->poll_state._waiter = {};

        auto sqe = next_sqe(q);
        io_uring_prep_poll_add(sqe, wait_fd, wait_events);
        sqe->user_data = reinterpret_cast< uint64_t >(&qs->poll_state) | k_target_bit;

        int const revents = co_await qs->poll_state;

        if (waiting_on_evfd) {
            uint64_t v;
            if (auto r = read(qs->evfd, &v, sizeof(v)); sizeof(v) != static_cast< size_t >(r)) {
                DLOGT("eventfd read returned {}", r);
            }
            continue; // re-evaluate iscsi_which_events
        }

        if (revents > 0) {
            if (iscsi_service(qs->ctx, revents) < 0) {
                DLOGE("iSCSI service failed: {}", iscsi_get_error(qs->ctx));
                co_return -1;
            }
        }
    }
}

// Initialize per-queue iSCSI context + eventfd + service-loop coroutine.
// Each queue gets its own libiscsi context (multi-queue forward-compat); login is sync.
std::list< int > iSCSIDisk::prepare(ublksrv_queue const* q, int const) {
    auto qs = std::make_unique< queue_service >();

    // Each queue gets its own libiscsi context. iscsi_url is owned by _sync_session — we only
    // borrow target/portal/lun strings from it here.
    qs->ctx = iscsi_create_context("iqn.2002-10.com.ublkpp:client");
    if (!qs->ctx) throw std::runtime_error("Failed to init per-queue iSCSI context");
    iscsi_set_log_level(qs->ctx, (spdlog::level::level_enum::critical - module_level_ublk_drivers) * 2);
    iscsi_set_log_fn(qs->ctx, iscsi_log);
    if (iscsi_set_alias(qs->ctx, "ublkpp")) throw std::runtime_error("Failed to set per-queue iSCSI alias");
    iscsi_set_session_type(qs->ctx, ISCSI_SESSION_NORMAL);
    iscsi_set_header_digest(qs->ctx, ISCSI_HEADER_DIGEST_NONE);
    iscsi_set_targetname(qs->ctx, _sync_session->url->target);
    if (0 != iscsi_full_connect_sync(qs->ctx, _sync_session->url->portal, _sync_session->url->lun))
        throw std::runtime_error(fmt::format("Per-queue iSCSI login failed: {}", iscsi_get_error(qs->ctx)));

    qs->evfd = eventfd(0, EFD_NONBLOCK);
    if (0 > qs->evfd) throw std::runtime_error(fmt::format("Could not initialize eventfd: {}", strerror(errno)));

    // Build service-loop coroutine and start it. Resuming runs synchronously to first co_await
    // (POLL_ADD setup), staging an SQE in q->ring_ptr; the queue's submit_and_wait_timeout picks
    // it up on the next iteration.
    auto task = __service_loop(q, qs.get());
    qs->service_handle = std::exchange(task._coro, {}); // steal the handle; task dtor is now no-op
    qs->service_handle.resume();

    {
        auto lk = std::unique_lock< std::shared_mutex >(_services_mtx);
        _services.emplace(q->q_id, std::move(qs));
    }
    return {};
}

io_result iSCSIDisk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    auto const lba = addr >> params()->basic.logical_bs_shift;

    DLOGT("{} : [INTERNAL] ublk io [lba:{:#0x}|len:{:#0x}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", lba, len)

    auto task = (UBLK_IO_OP_READ == op)
        ? iscsi_read16_iov_sync(_sync_session->ctx, _lun, lba, len, block_size(), 0, 0, 0, 0, 0,
                                reinterpret_cast< scsi_iovec* >(iovecs), nr_vecs)
        : iscsi_write16_iov_sync(_sync_session->ctx, _lun, lba, NULL, len, block_size(), 0, 0, 0, 0, 0,
                                 reinterpret_cast< scsi_iovec* >(iovecs), nr_vecs);
    if (!task) return std::unexpected(std::make_error_condition(std::errc::not_enough_memory));
    io_result res = len;
    if (SCSI_STATUS_GOOD != task->status) {
        DLOGW("iSCSI cmd returned error: [status:{}] iscsi_err: ", task->status, iscsi_get_error(_sync_session->ctx));
        if (SCSI_SENSE_ILLEGAL_REQUEST == task->sense.key) {
            if (SCSI_SENSE_ASCQ_LOGICAL_UNIT_NOT_SUPPORTED == task->sense.ascq) {
                res = std::unexpected(std::make_error_condition(std::errc::resource_unavailable_try_again));
            }
        }
        res = std::unexpected(std::make_error_condition(std::errc::io_error));
    }
    scsi_free_scsi_task(task);
    return res;
}

// Per-IO callback bound to libiscsi via iscsi_*_async. Runs from inside iscsi_service() which
// itself runs from the service-loop coroutine on the queue thread. We write the result to the
// per-IO cqe_state and resume the awaiting coroutine via symmetric transfer (handled by
// run_queue_loop's CQE dispatch path, except here we resume directly because we already have
// the state pointer).
struct iscsi_cb_data {
    cqe_state* state;
    int len;
    scsi_iovec io_vec[16]{{0, 0}};
};

void iSCSIDisk::__iscsi_rw_cb(iscsi_context* ctx, int status, void* data, void* private_data) {
    auto cb = reinterpret_cast< iscsi_cb_data* >(private_data);
    int result = cb->len;
    if (SCSI_STATUS_GOOD != status) [[unlikely]] {
        result = -EIO;
        auto task = reinterpret_cast< scsi_task* >(data);
        DLOGW("iSCSI cmd returned error: [status:{}|key:{:#0x}|ascq:{:#0x}] iscsi_err: {}", status,
              (uint8_t)task->sense.key, task->sense.ascq, iscsi_get_error(ctx));
        if (SCSI_SENSE_ILLEGAL_REQUEST == task->sense.key) {
            if (SCSI_SENSE_ASCQ_LOGICAL_UNIT_NOT_SUPPORTED == task->sense.ascq) { result = -EAGAIN; }
        }
    } else
        DLOGT("Got iSCSI completion: status: {}", status);

    cb->state->_result = result;
    cb->state->_result_ready = true;
    if (auto h = std::exchange(cb->state->_waiter, {})) h.resume();

    scsi_free_scsi_task(reinterpret_cast< scsi_task* >(data));
    delete cb;
}

disk_task< int > iSCSIDisk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                                      uint64_t addr) {
    auto* qs = __get_service(q);
    if (!qs) co_return -EIO; // prepare() must have populated this

    auto const op = ublksrv_get_op(data->iod);
    if (op == UBLK_IO_OP_FLUSH) co_return 0;
    if (op == UBLK_IO_OP_DISCARD || op == UBLK_IO_OP_WRITE_ZEROES) co_return -ENOTSUP; // TODO discard

    int const len = __iovec_len(iovecs, iovecs + nr_vecs);
    auto const lba = addr >> params()->basic.logical_bs_shift;

    DLOGT("{} : [tag:{:#0x}] ublk io [lba:{:#0x}|len:{:#0x}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag, lba,
          len)

    // Allocate per-IO cqe_state in the slot's async_io::_pool. _owner points at the slot so the
    // exception path in run_queue_loop can complete the IO with -EIO if the per-IO coroutine
    // throws.
    auto [state, _] = build_cqe_state_data(data);

    // libiscsi captures the iovec base pointers internally; iov_base lives in the kernel-allocated
    // ublk IO buffer and is stable for the IO's lifetime. We copy the iovec descriptors only
    // because libiscsi's API takes scsi_iovec by reference; the actual data buffers are not
    // copied. TODO: drop this copy if libiscsi guarantees iov-array stability.
    auto cb = new iscsi_cb_data{};
    cb->state = state;
    cb->len = len;
    for (auto i = 0U; nr_vecs > i; ++i) {
        cb->io_vec[i].iov_base = iovecs[i].iov_base;
        cb->io_vec[i].iov_len = iovecs[i].iov_len;
    }

    auto task = (UBLK_IO_OP_READ == op) ? iscsi_read16_iov_task(qs->ctx, _lun, lba, len, block_size(), 0, 0, 0, 0, 0,
                                                                __iscsi_rw_cb, cb, cb->io_vec, nr_vecs)
                                        : iscsi_write16_iov_task(qs->ctx, _lun, lba, NULL, len, block_size(), 0, 0, 0,
                                                                 0, 0, __iscsi_rw_cb, cb, cb->io_vec, nr_vecs);
    if (!task) {
        DLOGE("Failed {} to iSCSI LUN. {}", op == UBLK_IO_OP_READ ? "READ" : "WRITE", iscsi_get_error(qs->ctx));
        delete cb;
        co_return -ENOMEM;
    }

    // Push the queued PDU through the socket synchronously where possible. The common path is
    // a single non-blocking write that fully drains; the service loop then awaits POLLIN for
    // the response. TODO: handle the buffer-full case (libiscsi keeps POLLOUT pending and the
    // service loop, currently waiting on POLLIN only, would not re-arm) via cancel-and-rearm.
    if (int events = iscsi_which_events(qs->ctx); events & POLLOUT) {
        if (iscsi_service(qs->ctx, POLLOUT) < 0) {
            DLOGE("iSCSI inline service failed: {}", iscsi_get_error(qs->ctx));
            // The callback may not have fired; cb->state may still be unresumed. Best effort:
            // mark the state ready with an error so the await below returns instead of hanging.
            state->_result = -EIO;
            state->_result_ready = true;
            // cb is still owned by libiscsi (will be freed via __iscsi_rw_cb on its eventual call,
            // or leaked on context destroy). Don't double-free here.
            co_return -EIO;
        }
    }

    // Wake the service loop if it is suspended on the eventfd (events==0 branch). If it is
    // suspended on the libiscsi-fd POLL_ADD, this write accumulates harmlessly until the next
    // events==0 iteration drains it.
    uint64_t one = 1;
    if (auto r = write(qs->evfd, &one, sizeof(one)); sizeof(one) != static_cast< size_t >(r)) {
        DLOGT("eventfd write returned {}", r);
    }

    co_return co_await *state;
}

} // namespace ublkpp
