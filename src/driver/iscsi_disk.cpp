#include "ublkpp/drivers/iscsi_disk.hpp"

#include <sisl/logging/logging.h>
#include <ublksrv.h>

extern "C" {
#include <iscsi/scsi-lowlevel.h>
}

#include "lib/logging.hpp"

namespace ublkpp {

/// TODO These should be discoverable from Inquiry Pages
constexpr auto k_logical_block_size = 4 * Ki;
constexpr auto k_physical_block_size = k_logical_block_size;

struct iscsi_session {
    ~iscsi_session() {
        if (ctx) iscsi_destroy_context(ctx);
        if (logged_in) iscsi_logout_sync(ctx);
    }
    iscsi_context* ctx;
    bool logged_in{false};
    uint64_t capacity{0UL};
    uint32_t block_size{0U};
};

static std::unique_ptr< iscsi_session > iscsi_init(iscsi_url& url) {
    auto session = std::make_unique< iscsi_session >();
    DEBUG_ASSERT(session, "Failed to allocate iSCSI session!");

    if (session->ctx = iscsi_create_context("ublk_nublox"); !session->ctx) {
        DLOGE("failed to init context");
        return nullptr;
    }

    iscsi_set_session_type(session->ctx, ISCSI_SESSION_NORMAL);
    iscsi_set_header_digest(session->ctx, ISCSI_HEADER_DIGEST_NONE_CRC32C);

    if (session->logged_in = (0 == iscsi_full_connect_sync(session->ctx, url.portal, url.lun)); !session->logged_in) {
        DLOGE("Failed to connect to iSCSI LUN : {}", iscsi_get_error(session->ctx));
        return nullptr;
    }

    auto task = iscsi_readcapacity16_sync(session->ctx, url.lun);
    if (!task || task->status != SCSI_STATUS_GOOD) {
        DLOGE("Failed to send readcapacity16 command");
        if (task) { scsi_free_scsi_task(task); }
        return nullptr;
    }

    auto rc16 = static_cast< scsi_readcapacity16* >(scsi_datain_unmarshall(task));
    if (!rc16) {
        DLOGE("failed to unmarshall readcapacity16 data");
        scsi_free_scsi_task(task);
        return nullptr;
    }
    session->capacity = rc16->block_length * (rc16->returned_lba + 1);
    session->block_size = rc16->block_length;
    scsi_free_scsi_task(task);

    if (iscsi_mt_service_thread_start(session->ctx)) {
        DLOGE("failed to start service thread");
        return nullptr;
    }
    return session;
}

iSCSIDisk::iSCSIDisk(iscsi_url const& url) : _url(url) {
    direct_io = true;
    uses_ublk_iouring = false;

    _session = std::move(iscsi_init(_url));
    if (!_session) throw std::runtime_error(fmt::format("Failed to attach iSCSI target: {}", _url.target));

    auto& our_params = *params();
    our_params.basic.logical_bs_shift = static_cast< uint8_t >(ilog2(_session->block_size));
    our_params.basic.physical_bs_shift = static_cast< uint8_t >(ilog2(_session->block_size));
    our_params.basic.dev_sectors = _session->capacity >> SECTOR_SHIFT;

    // TODO Fix
    our_params.types |= ~UBLK_PARAM_TYPE_DISCARD;
    DLOGD("iSCSI device [{}:{}:{}]!", _url.target, k_logical_block_size, k_physical_block_size)
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

    DLOGT("{} : [tag:{}] ublk io [sector:{}|len:{}|sub_cmd:{:b}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag,
          addr >> SECTOR_SHIFT, len, sub_cmd)

    auto cb_data = new iscsi_cb_data(data->tag, dynamic_pointer_cast< iSCSIDisk >(shared_from_this()), q, len);
    switch (op) {
    case UBLK_IO_OP_READ: {
        if (auto task = iscsi_write16_iov_task(_session->ctx, _url.lun, addr >> ilog2(block_size()), NULL, len,
                                               _session->block_size, 0, 0, 0, 0, 0, iscsi_rw_cb, cb_data,
                                               reinterpret_cast< scsi_iovec* >(iovecs), nr_vecs);
            !task) {
            DLOGE("Failed to read16 to iSCSI LUN. {}", iscsi_get_error(_session->ctx));
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        }
    } break;
    case UBLK_IO_OP_WRITE: {
        if (auto task = iscsi_write16_iov_task(_session->ctx, _url.lun, addr >> ilog2(block_size()), NULL, len,
                                               _session->block_size, 0, 0, 0, 0, 0, iscsi_rw_cb, cb_data,
                                               reinterpret_cast< scsi_iovec* >(iovecs), nr_vecs);
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

io_result iSCSIDisk::sync_iov(uint8_t, iovec*, uint32_t, off_t) noexcept {
    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
}

} // namespace ublkpp
