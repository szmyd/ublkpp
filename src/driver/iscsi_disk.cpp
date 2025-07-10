#include "ublkpp/drivers/iscsi_disk.hpp"

#include <sisl/logging/logging.h>
#include <ublksrv.h>

extern "C" {
#include <iscsi/iscsi.h>
#include <iscsi/scsi-lowlevel.h>
#include <poll.h>
}

#include "lib/logging.hpp"

SISL_LOGGING_DEF(libiscsi)

namespace ublkpp {

/// TODO Should be discoverable from Inquiry Pages
constexpr auto k_physical_block_size = 4 * Ki;

struct iscsi_session {
    ~iscsi_session() {
        if (logged_in) iscsi_logout_sync(ctx);
        if (logged_in || attached) iscsi_disconnect(ctx);
        if (url) iscsi_destroy_url(url);
        if (ctx) iscsi_destroy_context(ctx);
    }

    iscsi_context* ctx{nullptr};
    iscsi_url* url{nullptr};
    bool attached{false};
    bool logged_in{false};
};

static void iscsi_log(int level, const char* message) {
    if (1 >= level) {
        LOGERRORMOD(libiscsi, "{}", message);
    } else if (2 == level) {
        LOGINFOMOD(libiscsi, "{}", message);
    } else if (3 == level) {
        LOGDEBUGMOD(libiscsi, "{}", message);
    } else {
        LOGTRACEMOD(libiscsi, "{}", message);
    }
}

static std::unique_ptr< iscsi_session > iscsi_connect(std::string const& url) {
    auto session = std::make_unique< iscsi_session >();
    DEBUG_ASSERT(session, "Failed to allocate iSCSI session!");

    if (session->ctx = iscsi_create_context("iqn.2002-10.com.ublkpp:client"); !session->ctx) {
        DLOGE("failed to init context")
        return nullptr;
    }
    iscsi_set_log_level(session->ctx, (spdlog::level::level_enum::critical - module_level_ublk_drivers) * 2);
    iscsi_set_log_fn(session->ctx, iscsi_log);

    if (iscsi_set_alias(session->ctx, "ublkpp")) return nullptr;

    // Attempt to parse the URL
    session->url = iscsi_parse_full_url(session->ctx, url.data());
    if (!session->url) {
        DLOGE("{}", iscsi_get_error(session->ctx))
        return nullptr;
    }

    if (session->attached = (0 == iscsi_connect_sync(session->ctx, session->url->portal)); !session->attached) {
        return nullptr;
    }

    iscsi_set_session_type(session->ctx, ISCSI_SESSION_DISCOVERY);
    if (session->logged_in = (0 == iscsi_login_sync(session->ctx)); !session->logged_in) {
        DLOGE("{}", iscsi_get_error(session->ctx))
        return nullptr;
    }

    auto discovery_addr = iscsi_discovery_sync(session->ctx);
    if (!discovery_addr) {
        DLOGE("{}", iscsi_get_error(session->ctx))
        return nullptr;
    }
    session->logged_in = !(0 == iscsi_logout_sync(session->ctx));
    session->attached = !(0 == iscsi_disconnect(session->ctx));

    strcpy(session->url->portal, discovery_addr->portals->portal);
    if (0 != strcmp(session->url->target, discovery_addr->target_name)) {
        DLOGE("Discovered a different target than expected: [{}] discovered: [{}]", session->url->target,
              discovery_addr->target_name);
        return nullptr;
    }
    return session;
}

static bool iscsi_login(std::unique_ptr< iscsi_session >& session) {
    iscsi_set_session_type(session->ctx, ISCSI_SESSION_NORMAL);
    iscsi_set_header_digest(session->ctx, ISCSI_HEADER_DIGEST_NONE);
    iscsi_set_targetname(session->ctx, session->url->target);

    if (session->logged_in = (0 == iscsi_full_connect_sync(session->ctx, session->url->portal, session->url->lun));
        !session->logged_in) {
        DLOGE("{}", iscsi_get_error(session->ctx))
        return false;
    }
    return session->logged_in;
}

static std::pair< uint64_t, uint32_t > probe_topology(std::unique_ptr< iscsi_session >& session) {
    auto task = iscsi_readcapacity16_sync(session->ctx, session->url->lun);
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
    uses_ublk_iouring = false;

    // Establish iSCSI login
    if (_session = iscsi_connect(url); !_session)
        throw std::runtime_error(fmt::format("Failed to attach iSCSI target: {}", url));
    if (!iscsi_login(_session)) throw std::runtime_error("Could not login to target");

    auto const [capacity, lba_size] = probe_topology(_session);
    if (0 == capacity) throw std::runtime_error("Could not probe LUN to discover capacity");
    auto const block_shift = ilog2(lba_size);

    auto& our_params = *params();
    our_params.basic.logical_bs_shift = static_cast< uint8_t >(block_shift);
    our_params.basic.physical_bs_shift = static_cast< uint8_t >(block_shift);
    our_params.basic.dev_sectors = capacity >> SECTOR_SHIFT;

    // TODO Implement discard
    our_params.types |= ~UBLK_PARAM_TYPE_DISCARD;
    DLOGD("iSCSI device [{}:{}:{}]!", _session->url->target, lba_size, k_physical_block_size)
}

iSCSIDisk::~iSCSIDisk() = default;

// Initialize our event loop before we start getting I/O
std::list< int > iSCSIDisk::open_for_uring(int const) {
    std::thread([ctx = _session->ctx] {
        auto pfd = pollfd{.fd = iscsi_get_fd(ctx), .events = 0, .revents = 0};
        while (true) {
            pfd.events = iscsi_which_events(ctx);
            if (poll(&pfd, 1, -1) < 0) {
                DLOGE("Poll failed: {}", strerror(errno))
                break;
            }
            if (iscsi_service(ctx, pfd.revents) < 0) {
                DLOGE("iSCSI failed: {}", iscsi_get_error(ctx))
                break;
            }
        }
    }).detach();
    return {};
}

void iSCSIDisk::collect_async(ublksrv_queue const*, std::list< async_result >& completed) {
    auto lck = std::scoped_lock< std::mutex >(pending_results_lck);
    completed.splice(completed.end(), std::move(pending_results));
}

void iSCSIDisk::async_complete(ublksrv_queue const* q, async_result const& result) {
    {
        auto lck = std::scoped_lock< std::mutex >(pending_results_lck);
        pending_results.emplace_back(result);
    }
    ublksrv_queue_send_event(q);
}

io_result iSCSIDisk::handle_flush(ublksrv_queue const*, ublk_io_data const* ublk_io, sub_cmd_t sub_cmd) {
    DLOGT("Flush : [tag:{}] ublk io [sub_cmd:{:b}]", ublk_io->tag, sub_cmd)
    if (direct_io) return 0;
    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
}

io_result iSCSIDisk::handle_discard(ublksrv_queue const*, ublk_io_data const* ublk_io, sub_cmd_t sub_cmd, uint32_t len,
                                    uint64_t addr) {
    DLOGD("DISCARD : [tag:{}] ublk io [sector:{}|len:{}|sub_cmd:{:b}]", ublk_io->tag, addr >> SECTOR_SHIFT, len,
          sub_cmd)
    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
}

struct iscsi_cb_data {
    ublk_io_data const* io;
    int tag;
    sub_cmd_t sub_cmd;
    std::shared_ptr< iSCSIDisk > device;
    ublksrv_queue const* queue;
    int len;
    scsi_iovec io_vec[16]{{0, 0}};
};

void iSCSIDisk::__iscsi_rw_cb(iscsi_context*, int status, void* data, void* private_data) {
    auto cb_data = reinterpret_cast< iscsi_cb_data* >(private_data);
    DLOGT("Got iSCSI completion: [tag:{}], status: {}", cb_data->tag, status);
    cb_data->device->async_complete(
        cb_data->queue,
        async_result{cb_data->io, cb_data->sub_cmd, (SCSI_STATUS_GOOD != status) ? -EIO : cb_data->len});
    scsi_free_scsi_task(reinterpret_cast< scsi_task* >(data));
    delete cb_data;
}

io_result iSCSIDisk::async_iov(ublksrv_queue const* q, ublk_io_data const* ublk_io, sub_cmd_t sub_cmd, iovec* iovecs,
                               uint32_t nr_vecs, uint64_t addr) {
    auto const op = ublksrv_get_op(ublk_io->iod);
    int const len = __iovec_len(iovecs, iovecs + nr_vecs);

    // Convert the absolute address to an LBA offset
    auto const lba = addr >> params()->basic.logical_bs_shift;

    DLOGT("{} : [tag:{}] ublk io [lba:{}|len:{}|sub_cmd:{:b}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", ublk_io->tag,
          lba, len, sub_cmd)

    // We copy the iovec here since libiscsi does not make it stable
    auto cb_data = new iscsi_cb_data(ublk_io, ublk_io->tag, sub_cmd,
                                     dynamic_pointer_cast< iSCSIDisk >(shared_from_this()), q, len);
    if (!cb_data) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    for (auto i = 0U; nr_vecs > i; ++i) {
        cb_data->io_vec[i].iov_base = iovecs[i].iov_base;
        cb_data->io_vec[i].iov_len = iovecs[i].iov_len;
    }

    scsi_task* task{nullptr};
    switch (op) {
    case UBLK_IO_OP_READ: {
        task = iscsi_read16_iov_task(_session->ctx, _session->url->lun, lba, len, block_size(), 0, 0, 0, 0, 0,
                                     __iscsi_rw_cb, cb_data, cb_data->io_vec, nr_vecs);
    } break;
    case UBLK_IO_OP_WRITE: {
        task = iscsi_write16_iov_task(_session->ctx, _session->url->lun, lba, NULL, len, block_size(), 0, 0, 0, 0, 0,
                                      __iscsi_rw_cb, cb_data, cb_data->io_vec, nr_vecs);
    } break;
    default: {
        return folly::makeUnexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    }

    if (!task) {
        DLOGE("Failed {} to iSCSI LUN. {}", op == UBLK_IO_OP_READ ? "READ" : "WRITE", iscsi_get_error(_session->ctx));
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    }
    return 1;
}

io_result iSCSIDisk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);

    // Convert the absolute address to an LBA offset
    auto const lba = addr >> params()->basic.logical_bs_shift;

    DLOGT("{} : [INTERNAL] ublk io [lba:{}|len:{}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", lba, len)

    scsi_task* task{nullptr};
    switch (op) {
    case UBLK_IO_OP_READ: {
        task = iscsi_read16_iov_sync(_session->ctx, _session->url->lun, lba, len, block_size(), 0, 0, 0, 0, 0,
                                     reinterpret_cast< scsi_iovec* >(iovecs), nr_vecs);
    } break;
    case UBLK_IO_OP_WRITE: {
        task = iscsi_write16_iov_sync(_session->ctx, _session->url->lun, lba, NULL, len, block_size(), 0, 0, 0, 0, 0,
                                      reinterpret_cast< scsi_iovec* >(iovecs), nr_vecs);
    } break;
    default: {
        DLOGE("Unknown SYNC operation: [op:{}]", op);
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    }
    }
    if (!task || task->status != SCSI_STATUS_GOOD) {
        DLOGE("Failed to {} to iSCSI LUN. {}", op == UBLK_IO_OP_READ ? "READ" : "WRITE",
              iscsi_get_error(_session->ctx));
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    }
    return len;
}
} // namespace ublkpp
