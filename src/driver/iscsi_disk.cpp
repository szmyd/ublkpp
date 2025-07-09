#include "ublkpp/drivers/iscsi_disk.hpp"

#include <sisl/logging/logging.h>
#include <ublksrv.h>

extern "C" {
#include <iscsi/iscsi.h>
#include <iscsi/scsi-lowlevel.h>
}

#include "lib/logging.hpp"

namespace ublkpp {

/// TODO These should be discoverable from Inquiry Pages
constexpr auto k_logical_block_size = 4 * Ki;
constexpr auto k_physical_block_size = k_logical_block_size;

struct iscsi_session {
    ~iscsi_session() {
        if (logged_in) iscsi_logout_sync(ctx);
        if (url) iscsi_destroy_url(url);
        if (ctx) iscsi_destroy_context(ctx);
    }

    iscsi_context* ctx{nullptr};
    iscsi_url* url{nullptr};
    bool logged_in{false};
};

static std::unique_ptr< iscsi_session > iscsi_init(std::string const& url) {
    auto session = std::make_unique< iscsi_session >();
    DEBUG_ASSERT(session, "Failed to allocate iSCSI session!");

    if (session->ctx = iscsi_create_context("ublk_nublox"); !session->ctx) {
        DLOGE("failed to init context")
        return nullptr;
    }

    // Attempt to parse the URL
    session->url = iscsi_parse_full_url(session->ctx, url.data());
    if (!session->url) {
        DLOGE("Could not parse [{}] as an iSCSI URL! Expected format: "
              "iscsi://[<username>[%<password>]@]<host>[:<port>]/<target-iqn>/<lun>",
              url)
        return nullptr;
    }
    DLOGI("logging in to: [ip:{}|target:{}|lun:{}]", session->url->portal, session->url->target, session->url->lun);

    iscsi_set_session_type(session->ctx, ISCSI_SESSION_NORMAL);
    iscsi_set_header_digest(session->ctx, ISCSI_HEADER_DIGEST_NONE_CRC32C);

    if (session->logged_in = (0 == iscsi_full_connect_sync(session->ctx, session->url->portal, session->url->lun));
        !session->logged_in) {
        DLOGE("{}", iscsi_get_error(session->ctx))
        return nullptr;
    }

    if (iscsi_mt_service_thread_start(session->ctx)) {
        DLOGE("failed to start service thread");
        return nullptr;
    }
    return session;
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
        DLOGD("Discovered LUN with [sz:{}|bs:{}]", capacity, block_size)
    } else
        DLOGE("failed to unmarshall readcapacity16 data")
    scsi_free_scsi_task(task);
    return std::make_pair(capacity, block_size);
}

iSCSIDisk::iSCSIDisk(std::string const& url) {
    direct_io = true;
    uses_ublk_iouring = false;

    // Try to do iSCSI Login
    if (_session = iscsi_init(url); !_session)
        throw std::runtime_error(fmt::format("Failed to attach iSCSI target: {}", url));

    auto const [capacity, block_size] = probe_topology(_session);
    if (0 == capacity) throw std::runtime_error("Could not probe LUN to discover capacity");
    auto const block_shift = ilog2(block_size);

    auto& our_params = *params();
    our_params.basic.logical_bs_shift = static_cast< uint8_t >(block_shift);
    our_params.basic.physical_bs_shift = static_cast< uint8_t >(block_shift);
    our_params.basic.dev_sectors = capacity >> SECTOR_SHIFT;

    // TODO Implement discard
    our_params.types |= ~UBLK_PARAM_TYPE_DISCARD;
    DLOGD("iSCSI device [{}:{}:{}]!", _session->url->target, k_logical_block_size, k_physical_block_size)
}

iSCSIDisk::~iSCSIDisk() = default;

void iSCSIDisk::handle_event(ublksrv_queue const* q) {
    decltype(pending_results) completed_results;
    {
        auto lck = std::scoped_lock< std::mutex >(pending_results_lck);
        completed_results.swap(pending_results);
    }
    ublksrv_queue_handled_event(q);
    for (auto& i : completed_results) {
        DLOGT("Completing [tag:{}|result:{}]", i.tag, i.result);
        ublksrv_complete_io(q, i.tag, i.result);
    }
}

io_result iSCSIDisk::handle_flush(ublksrv_queue const*, ublk_io_data const* data, sub_cmd_t sub_cmd) {

    DLOGT("Flush : [tag:{}] ublk io [sub_cmd:{:b}]", data->tag, sub_cmd)
    if (direct_io) return 0;
    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
}

io_result iSCSIDisk::handle_discard(ublksrv_queue const*, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                                    uint64_t addr) {
    DLOGD("DISCARD : [tag:{}] ublk io [sector:{}|len:{}|sub_cmd:{:b}]", data->tag, addr >> SECTOR_SHIFT, len, sub_cmd)
    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
}

struct iscsi_cb_data {
    int tag;
    std::shared_ptr< iSCSIDisk > device;
    ublksrv_queue const* queue;
    uint64_t len;
};

void iSCSIDisk::__rw_async_cb(ublksrv_queue const* q, int tag, int status, int res) {
    {
        auto lck = std::scoped_lock< std::mutex >(pending_results_lck);
        pending_results.emplace_back(req_result{tag, (SCSI_STATUS_GOOD != status) ? -EIO : res});
    }

    ublksrv_queue_send_event(q);
}

void iscsi_rw_cb(iscsi_context*, int status, void* data, void* private_data) {
    auto cb_data = reinterpret_cast< iscsi_cb_data* >(private_data);
    cb_data->device->__rw_async_cb(cb_data->queue, cb_data->tag, status, cb_data->len);
    scsi_free_scsi_task(reinterpret_cast< scsi_task* >(data));
}

io_result iSCSIDisk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                               uint32_t nr_vecs, uint64_t addr) {
    auto const op = ublksrv_get_op(data->iod);
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);

    // Convert the absolute address to an LBA offset
    auto const lba = addr >> params()->basic.logical_bs_shift;

    DLOGT("{} : [tag:{}] ublk io [lba:{}|len:{}|sub_cmd:{:b}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag,
          lba, len, sub_cmd)

    auto cb_data = new iscsi_cb_data(data->tag, dynamic_pointer_cast< iSCSIDisk >(shared_from_this()), q, len);
    switch (op) {
    case UBLK_IO_OP_READ: {
        if (auto task =
                iscsi_write16_iov_task(_session->ctx, _session->url->lun, lba, NULL, len, block_size(), 0, 0, 0, 0, 0,
                                       iscsi_rw_cb, cb_data, reinterpret_cast< scsi_iovec* >(iovecs), nr_vecs);
            !task) {
            DLOGE("Failed to read16 to iSCSI LUN. {}", iscsi_get_error(_session->ctx));
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        }
    } break;
    case UBLK_IO_OP_WRITE: {
        if (auto task =
                iscsi_write16_iov_task(_session->ctx, _session->url->lun, lba, NULL, len, block_size(), 0, 0, 0, 0, 0,
                                       iscsi_rw_cb, cb_data, reinterpret_cast< scsi_iovec* >(iovecs), nr_vecs);
            !task) {
            DLOGE("Failed to write16 to iSCSI LUN. {}", iscsi_get_error(_session->ctx));
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        }
    } break;
    default: {
        return folly::makeUnexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    }
    return folly::makeUnexpected(std::make_error_condition(std::errc::operation_in_progress));
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
        DLOGE("Failed to write16 to iSCSI LUN. {}", iscsi_get_error(_session->ctx));
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    }
    return 0;
}
} // namespace ublkpp
