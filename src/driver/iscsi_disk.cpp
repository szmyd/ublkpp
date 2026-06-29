#include "ublkpp/drivers.hpp"

#include <coroutine>
#include <memory>
#include <mutex>
#include <vector>

#include <ublkpp/lib/ublk_disk.hpp>

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

#include "lib/common.hpp"
#include "lib/logging.hpp"

SISL_LOGGING_DEF(libiscsi)

namespace ublkpp {

/// TODO Should be discoverable from Inquiry Pages
constexpr auto k_physical_block_size = 4 * Ki;

struct iscsi_session {
    ~iscsi_session() {
        if (url) iscsi_destroy_url(url);
        if (ctx) {
            // Skip logout on a dead ctx: target is unreachable so the LOGOUT PDU would block
            // the destructor on libiscsi's internal poll loop until scsi_timeout (or forever).
            if (!dead) iscsi_logout_sync(ctx);
            iscsi_destroy_context(ctx);
        }
    }

    iscsi_context* ctx{nullptr};
    iscsi_url* url{nullptr};
    // Set to true after a sync iSCSI op fails with the underlying ctx in deferred-reconnect
    // state (max_retries=0 + first failure cancels all PDUs and freezes the ctx). The next
    // sync_iov call observes this and tears down + rebuilds the ctx via iscsi_full_connect_sync
    // before retrying.
    bool dead{false};
};

// Per-queue async service: owns one libiscsi context, an eventfd used to wake the service-loop
// coroutine when async_iov queues new work, the cqe_state the loop awaits on, and the suspended
// coroutine handle itself.
//
// Lifetime: created lazily in iSCSIDisk::prepare(q) on the queue thread, destroyed by the
// iSCSIDisk destructor (which runs after queue threads have joined). Destroying the suspended
// coroutine_handle frees the frame; iscsi_logout_sync drives its own internal poll loop and
// does not depend on the service loop coroutine.
// Session-loss state machine.
//   alive       -> normal operation; service loop polls libiscsi socket fd.
//   dead        -> iscsi_service returned -1 (max_retries=0 + first reconnect failure cancelled
//                  all in-flight PDUs and set reconnect_deferred). ctx is permanently unusable;
//                  service loop suspends on evfd waiting for a recovery trigger.
//   rebuilding  -> recovery requested. Service loop will (next iteration) destroy the dead ctx,
//                  create a fresh one, and call iscsi_full_connect_async. Login completion fires
//                  rebuild_login_cb which transitions to alive (success) or back to dead (fail).
//
// All transitions happen on the queue thread. Cross-thread (sync_iov on resync/probe thread)
// triggers recovery only by writing to evfd; the queue's service loop interprets that wake as
// "advance dead -> rebuilding". No atomic needed.
enum class qs_state_t { alive, dead, rebuilding };

struct queue_service {
    iscsi_context* ctx{nullptr};
    int evfd{-1};
    cqe_state poll_state{};
    std::coroutine_handle<> service_handle{};
    // Set by the service loop before each suspend; read by async_iov to decide whether the
    // eventfd write is needed. When the loop is suspended on the libiscsi socket fd, a freshly
    // submitted PDU's response will wake it via POLLIN naturally -- no evfd kick required.
    // Plain bool: async_iov and the service loop body both run on the queue thread, never
    // concurrently (coroutines on a single thread), so no atomicity needed.
    bool waiting_on_evfd{false};

    qs_state_t state{qs_state_t::alive};
    // True between the service loop calling iscsi_full_connect_async and rebuild_login_cb firing.
    // Prevents the loop from re-kicking another rebuild while one is in flight.
    bool rebuild_kicked_off{false};

    // Captured at prepare() time; needed during rebuild because the dead ctx (which sourced these)
    // is destroyed before the new ctx is built. Owned strings, lifetime = queue_service lifetime.
    std::string portal;
    std::string target;
    int lun{0};

    queue_service() = default;
    queue_service(queue_service const&) = delete;
    queue_service& operator=(queue_service const&) = delete;

    ~queue_service() {
        // Destroy the suspended service-loop frame first; safe because per-IO are already drained
        // (umount-gated del_dev) and the kernel silently cancels the in-flight POLL_ADD when the
        // io_uring is torn down by ublksrv_queue_deinit before this destructor runs.
        if (service_handle) service_handle.destroy();
        if (ctx) {
            // Skip logout if ctx is dead/rebuilding: socket is gone or still connecting, the
            // LOGOUT PDU would block on libiscsi's internal poll until scsi_timeout (or never).
            if (state == qs_state_t::alive) iscsi_logout_sync(ctx);
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

// max_retries=0 makes libiscsi treat the first reconnect failure as terminal: it cancels every
// in-flight PDU (callbacks fire with SCSI_STATUS_CANCELLED) and marks the ctx reconnect_deferred,
// so iscsi_service returns -1 from then on. Recovery is then our job (destroy + recreate ctx);
// see queue_service / __service_loop. Without this, libiscsi auto-replays PDUs on reconnect,
// which is unsafe when the caller (RAID1) is also retrying.
static constexpr int k_iscsi_reconnect_max_retries = 0;

static bool iscsi_login(iscsi_session& session) {
    iscsi_set_session_type(session.ctx, ISCSI_SESSION_NORMAL);
    iscsi_set_header_digest(session.ctx, ISCSI_HEADER_DIGEST_NONE);
    iscsi_set_targetname(session.ctx, session.url->target);
    iscsi_set_reconnect_max_retries(session.ctx, k_iscsi_reconnect_max_retries);

    if (0 != iscsi_full_connect_sync(session.ctx, session.url->portal, session.url->lun)) {
        DLOGE("{}", iscsi_get_error(session.ctx))
        return false;
    }
    return (0 != iscsi_is_logged_in(session.ctx));
}

// Configure a freshly-created per-queue iscsi_context. Used by both prepare() (sync login path)
// and the async rebuild path inside the service loop. Throws on alias-set failure since the
// caller treats per-queue setup as fatal.
static void setup_qs_ctx(iscsi_context* ctx, char const* target) {
    iscsi_set_log_level(ctx, (spdlog::level::level_enum::critical - module_level_ublk_drivers) * 2);
    iscsi_set_log_fn(ctx, iscsi_log);
    if (iscsi_set_alias(ctx, "ublkpp")) throw std::runtime_error("Failed to set per-queue iSCSI alias");
    iscsi_set_session_type(ctx, ISCSI_SESSION_NORMAL);
    iscsi_set_header_digest(ctx, ISCSI_HEADER_DIGEST_NONE);
    iscsi_set_targetname(ctx, target);
    iscsi_set_reconnect_max_retries(ctx, k_iscsi_reconnect_max_retries);
}

// iscsi_full_connect_async completion callback used by the rebuild path. Runs synchronously
// inside iscsi_service() on the queue thread (same thread as the service loop), so direct
// state mutation is safe -- no atomics needed.
static void __rebuild_login_cb(iscsi_context* ctx, int status, void* /*data*/, void* private_data) {
    auto qs = reinterpret_cast< queue_service* >(private_data);
    qs->rebuild_kicked_off = false;
    if (SCSI_STATUS_GOOD == status) {
        qs->state = qs_state_t::alive;
        DLOGD("iSCSI session rebuild complete")
    } else {
        qs->state = qs_state_t::dead;
        DLOGW("iSCSI session rebuild failed: {}", iscsi_get_error(ctx))
    }
}

// Tear down a dead sync ctx and rebuild it via iscsi_full_connect_sync. Caller must hold
// _sync_mutex. Returns true on success; on failure, session.ctx is left null and session.dead
// stays true so the next sync_iov retries.
static bool rebuild_sync_session(iscsi_session& session) {
    DLOGD("Rebuilding sync iSCSI session for target {}", session.url->target)
    if (session.ctx) {
        // Skip logout: ctx is in deferred-reconnect state, LOGOUT PDU would hang.
        iscsi_destroy_context(session.ctx);
        session.ctx = nullptr;
    }
    session.ctx = iscsi_create_context("iqn.2002-10.com.ublkpp:client");
    if (!session.ctx) {
        DLOGE("Sync session rebuild: failed to allocate context")
        return false;
    }
    iscsi_set_log_level(session.ctx, (spdlog::level::level_enum::critical - module_level_ublk_drivers) * 2);
    iscsi_set_log_fn(session.ctx, iscsi_log);
    if (iscsi_set_alias(session.ctx, "ublkpp")) {
        DLOGE("Sync session rebuild: failed to set alias")
        iscsi_destroy_context(session.ctx);
        session.ctx = nullptr;
        return false;
    }
    if (!iscsi_login(session)) {
        // iscsi_login already logged the error
        iscsi_destroy_context(session.ctx);
        session.ctx = nullptr;
        return false;
    }
    session.dead = false;
    return true;
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

// File-local concrete ublk_disk; constructed only via the make_iscsi_disk factory below. The
// public header exposes only the factory; raid/composite consumers cast back to ublk_disk.
class iSCSIDisk : public ublk_disk {
    // Setup-time iSCSI session: used for sync_iov (called by Raid1 resync_task on its own thread,
    // by the avail-probe path, by superblock/bitmap I/O at construction, and by the destructor's
    // logout). Stays alive across the disk lifetime; libiscsi contexts are not thread-safe so this
    // session is independent of the per-queue async sessions.
    std::unique_ptr< iscsi_session > _sync_session;
    // Serialises concurrent sync_iov callers (resync vs. probe vs. raid1 read-fallback). libiscsi
    // contexts are not thread-safe, and the rebuild-on-failure path mutates _sync_session->ctx.
    std::mutex _sync_mutex;
    int _lun{0};

    // Per-queue async services. Slot vector sized to MAX_NR_HW_QUEUES at construction; populated
    // in prepare() (queue thread, exactly once per q_id) and read lock-free from async_iov on the
    // same thread. Slot ownership is unique to its queue thread, so no synchronisation is needed
    // beyond the construction-time sizing. Each queue_service owns its own iscsi_context, eventfd,
    // and service-loop coroutine handle; see the dispatch protocol described above.
    std::vector< std::unique_ptr< queue_service > > _services;

public:
    explicit iSCSIDisk(std::string const& url, std::string const& parent_id = "");
    ~iSCSIDisk() override;

    std::string id() const noexcept override;
    prepare_result prepare(ublksrv_queue const*, int const) override;

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;

    static void __iscsi_rw_cb(iscsi_context*, int, void*, void*);
};

iSCSIDisk::iSCSIDisk(std::string const& url, std::string const& /* TODO: metrics */) : _services(MAX_NR_HW_QUEUES) {
    _direct_io = true;

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

// Service-loop coroutine: pumps libiscsi events through io_uring POLL_ADD on the queue thread.
//
// Steady-state behaviour:
//   - Compute iscsi_which_events(); if non-zero, POLL_ADD on the libiscsi socket fd.
//   - If zero, POLL_ADD on the queue_service's eventfd to be woken by async_iov.
//   - Suspend on cqe_state until run_queue_loop dispatches the CQE.
//   - On wake: drain eventfd if applicable, then call iscsi_service(revents) to advance libiscsi.
//
// Recovery behaviour (qs->state machine, see qs_state_t):
//   - iscsi_service returning -1 (max_retries=0 + reconnect failure) sets state=dead.
//   - In dead state we wait on evfd only -- iscsi_get_fd would return a stale/closed fd and
//     polling on it would tight-loop with POLLNVAL; iscsi_which_events would return 0 anyway.
//   - Any evfd kick while dead advances to rebuilding (the kick is the recovery trigger from
//     async_iov on this queue, or sync_iov broadcasting from RAID1's probe/resync thread).
//   - In rebuilding state, on the first iteration we destroy the dead ctx, build a fresh one,
//     and call iscsi_full_connect_async(__rebuild_login_cb). Subsequent iterations poll the new
//     socket fd normally; iscsi drives the connect/login PDU exchange via iscsi_service. When
//     login completes, __rebuild_login_cb sets state=alive (or back to dead on failure).
//
// This coroutine never returns; it is owned by queue_service::service_handle and destroyed
// (frame freed) by the queue_service destructor at iSCSIDisk teardown. It uses cqe_state
// with _owner = nullptr (the stand-alone form documented in cqe_state.hpp); errors thrown
// out of resume are swallowed by the catch in run_queue_loop because _owner is null.
static disk_task< int > __service_loop(ublksrv_queue const* q, queue_service* qs) {
    while (true) {
        // Rebuild kickoff: dead ctx is gone, allocate fresh, set max_retries=0 so the new ctx
        // has the same fail-fast semantics, kick async login. login_cb flips state on completion.
        if (qs->state == qs_state_t::rebuilding && !qs->rebuild_kicked_off) {
            if (qs->ctx) {
                // No iscsi_logout_sync: ctx is in deferred-reconnect state, the LOGOUT PDU would
                // hang on libiscsi's internal poll. iscsi_destroy_context is sufficient because
                // max_retries=0 already cancelled all in-flight PDUs (callbacks fired with
                // SCSI_STATUS_CANCELLED, our cb_data freed) when the ctx transitioned to dead.
                iscsi_destroy_context(qs->ctx);
                qs->ctx = nullptr;
            }
            qs->ctx = iscsi_create_context("iqn.2002-10.com.ublkpp:client");
            if (!qs->ctx) {
                DLOGE("iSCSI rebuild: failed to allocate new context")
                qs->state = qs_state_t::dead;
            } else {
                try {
                    setup_qs_ctx(qs->ctx, qs->target.c_str());
                } catch (std::exception const& e) {
                    DLOGE("iSCSI rebuild: setup failed: {}", e.what())
                    iscsi_destroy_context(qs->ctx);
                    qs->ctx = nullptr;
                    qs->state = qs_state_t::dead;
                }
                if (qs->ctx) {
                    // Set kicked_off=true BEFORE the call: __rebuild_login_cb may fire
                    // synchronously inside iscsi_full_connect_async on local errors and would
                    // reset the flag; we must not then overwrite that with true.
                    qs->rebuild_kicked_off = true;
                    if (0 != iscsi_full_connect_async(qs->ctx, qs->portal.c_str(), qs->lun, __rebuild_login_cb, qs)) {
                        DLOGE("iSCSI rebuild: full_connect_async failed: {}", iscsi_get_error(qs->ctx))
                        qs->rebuild_kicked_off = false;
                        qs->state = qs_state_t::dead;
                    }
                }
            }
        }

        // In dead state we cannot poll on iscsi_get_fd -- the underlying socket is closed and
        // POLL_ADD on it would deliver POLLNVAL immediately, tight-looping. Wait on evfd only;
        // any kick (from async_iov on this queue, or sync_iov from RAID1's probe thread) is the
        // signal to retry. In alive/rebuilding state, iscsi_which_events tells us what the lib
        // wants to wait on.
        int wait_fd, wait_events;
        bool const can_poll_socket = (qs->state != qs_state_t::dead) && qs->ctx;
        int const events = can_poll_socket ? iscsi_which_events(qs->ctx) : 0;
        if (0 == events) {
            wait_fd = qs->evfd;
            wait_events = POLLIN;
            qs->waiting_on_evfd = true;
        } else {
            // iscsi_get_fd is recomputed each iteration: it changes after rebuild (new socket).
            wait_fd = iscsi_get_fd(qs->ctx);
            wait_events = events;
            qs->waiting_on_evfd = false;
        }

        qs->poll_state._result = 0;
        qs->poll_state._result_ready = false;
        qs->poll_state._waiter = {};

        auto sqe = next_sqe(q);
        io_uring_prep_poll_add(sqe, wait_fd, wait_events);
        sqe->user_data = reinterpret_cast< uint64_t >(&qs->poll_state) | sisl::async::k_managed_bit;

        int const revents = co_await qs->poll_state;

        if (qs->waiting_on_evfd) {
            uint64_t v;
            if (auto r = read(qs->evfd, &v, sizeof(v)); sizeof(v) != static_cast< size_t >(r)) {
                DLOGT("eventfd read returned {}", r);
            }
            // Trigger recovery if we were sitting in dead. The kick may have come from async_iov
            // (this queue) or from sync_iov (cross-thread, after a successful sync probe). Both
            // mean "try again"; the next iteration's rebuild kickoff handles the rest.
            if (qs->state == qs_state_t::dead) qs->state = qs_state_t::rebuilding;
            continue;
        }

        if (revents > 0 && qs->ctx) {
            if (iscsi_service(qs->ctx, revents) < 0) {
                // libiscsi side-effect (iscsi_defer_reconnect) has already cancelled all in-flight
                // PDUs as SCSI_STATUS_CANCELLED -> our cb fires -EIO -> waiters resumed. The ctx
                // is now permanently in reconnect_deferred state; only destroy+recreate revives it.
                DLOGW("iSCSI service failed: {}", iscsi_get_error(qs->ctx))
                qs->state = qs_state_t::dead;
            }
        }
    }
}

// Initialize per-queue iSCSI context + eventfd + service-loop coroutine.
// Each queue gets its own libiscsi context (multi-queue forward-compat); login is sync.
ublk_disk::prepare_result iSCSIDisk::prepare(ublksrv_queue const* q, int const) {
    auto qs = std::make_unique< queue_service >();

    // Stash portal/target/lun for the rebuild path. The rebuild destroys the old ctx (which
    // owns the libiscsi-internal copies) before creating the new one, so we need our own copies.
    qs->portal = _sync_session->url->portal;
    qs->target = _sync_session->url->target;
    qs->lun = _sync_session->url->lun;

    qs->ctx = iscsi_create_context("iqn.2002-10.com.ublkpp:client");
    if (!qs->ctx) throw std::runtime_error("Failed to init per-queue iSCSI context");
    setup_qs_ctx(qs->ctx, qs->target.c_str());
    if (0 != iscsi_full_connect_sync(qs->ctx, qs->portal.c_str(), qs->lun))
        throw std::runtime_error(fmt::format("Per-queue iSCSI login failed: {}", iscsi_get_error(qs->ctx)));

    qs->evfd = eventfd(0, EFD_NONBLOCK);
    if (0 > qs->evfd) throw std::runtime_error(fmt::format("Could not initialize eventfd: {}", strerror(errno)));

    // Build service-loop coroutine and start it. Resuming runs synchronously to first co_await
    // (POLL_ADD setup), staging an SQE in q->ring_ptr; the queue's submit_and_wait_timeout picks
    // it up on the next iteration.
    auto task = __service_loop(q, qs.get());
    qs->service_handle = std::exchange(task._coro, {}); // steal the handle; task dtor is now no-op
    qs->service_handle.resume();

    // Slot is owned by this queue thread; vector was sized at construction and slot indices are
    // unique to their queue thread, so no lock is needed.
    if (q->q_id < 0 || static_cast< size_t >(q->q_id) >= _services.size())
        throw std::runtime_error(fmt::format("q_id {} out of range (max {})", q->q_id, _services.size()));
    _services[q->q_id] = std::move(qs);
    return {.max_sqes_per_io = 0}; // Everything submitted through libiscsi
}

io_result iSCSIDisk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    auto const len = iovec_len(iovecs, iovecs + nr_vecs);
    auto const lba = addr >> params()->basic.logical_bs_shift;

    DLOGT("{} : [INTERNAL] ublk io [lba:{:#0x}|len:{:#0x}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", lba, len)

    // Serialise concurrent callers (RAID1 probe vs. resync vs. read-fallback). libiscsi contexts
    // are not thread-safe; the rebuild path mutates _sync_session->ctx; both must be locked.
    std::lock_guard< std::mutex > lk(_sync_mutex);

    // Rebuild a previously-dead sync session before issuing IO. Failure here is reported as
    // not-connected so callers (RAID1) can distinguish it from a SCSI-level error.
    if (_sync_session->dead) {
        if (!rebuild_sync_session(*_sync_session))
            return std::unexpected(std::make_error_condition(std::errc::not_connected));
    }

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

    // If the ctx fell out of logged-in state during the call, max_retries=0 has placed it in
    // permanent reconnect_deferred. Mark dead so the next sync_iov rebuilds the ctx end-to-end.
    if (!iscsi_is_logged_in(_sync_session->ctx)) _sync_session->dead = true;

    // "Sync corrects async": on a successful sync IO the target is reachable, so kick every
    // per-queue async service whose ctx is not currently logged in. The kick wakes their service
    // loops; each loop interprets the wake as "advance dead -> rebuilding" and starts an async
    // login. evfd writes are kernel-atomic and harmless if the loop is on the socket fd.
    if (res) {
        for (auto& qs_ptr : _services) {
            auto* qs = qs_ptr.get();
            if (!qs || qs->evfd < 0) continue;
            if (qs->ctx && iscsi_is_logged_in(qs->ctx)) continue;
            uint64_t one = 1;
            if (auto r = write(qs->evfd, &one, sizeof(one)); sizeof(one) != static_cast< size_t >(r)) {
                DLOGT("sync_iov evfd kick returned {}", r)
            }
        }
    }

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
    // Lock-free: slot was populated by prepare() on this same queue thread.
    auto* qs = _services[q->q_id].get();
    if (!qs) co_return -EIO; // prepare() must have populated this

    // Fail-fast on a downed session. RAID1 above us marks the leg dirty on -ENOTCONN; recovery is
    // driven by sync_iov (probe/resync) rebuilding the qs ctx and broadcasting to all queue evfds.
    // Kick our evfd here so the service loop, if currently parked in dead state, advances to
    // rebuilding without waiting for the next sync_iov broadcast.
    if (!iscsi_is_logged_in(qs->ctx)) {
        if (qs->waiting_on_evfd) {
            uint64_t one = 1;
            if (auto r = write(qs->evfd, &one, sizeof(one)); sizeof(one) != static_cast< size_t >(r)) {
                DLOGT("eventfd kick on dead session returned {}", r);
            }
        }
        co_return -ENOTCONN;
    }

    auto const op = ublksrv_get_op(data->iod);
    if (op == UBLK_IO_OP_FLUSH) co_return 0;
    if (op == UBLK_IO_OP_DISCARD || op == UBLK_IO_OP_WRITE_ZEROES) co_return -ENOTSUP; // TODO discard

    int const len = iovec_len(iovecs, iovecs + nr_vecs);
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

    // Wake the service loop only if it is suspended on the eventfd. If it is suspended on the
    // libiscsi socket fd, the response we just submitted will arrive there and wake it via
    // POLLIN -- no kick needed. The flag is set by the loop before each suspend; same-thread
    // coroutines mean we always read the value matching the loop's current wait_fd.
    if (qs->waiting_on_evfd) {
        uint64_t one = 1;
        if (auto r = write(qs->evfd, &one, sizeof(one)); sizeof(one) != static_cast< size_t >(r)) {
            DLOGT("eventfd write returned {}", r);
        }
    }

    co_return co_await *state;
}

std::shared_ptr< ublk_disk > make_iscsi_disk(std::string const& url, std::string const& parent_id) {
    return std::make_shared< iSCSIDisk >(url, parent_id);
}

} // namespace ublkpp
