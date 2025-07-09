#include "ublkpp/drivers/iscsi_disk.hpp"

#include <sisl/logging/logging.h>
#include <ublksrv.h>

extern "C" {
#include <iscsi/iscsi.h>
#include <iscsi/scsi-lowlevel.h>
}

#include "lib/logging.hpp"

namespace ublkpp {

iSCSIDisk::iSCSIDisk() : UblkDisk() {}

iSCSIDisk::~iSCSIDisk() = default;

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

io_result iSCSIDisk::async_iov(ublksrv_queue const*, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                               uint32_t nr_vecs, uint64_t addr) {
    auto const op = ublksrv_get_op(data->iod);
    DLOGT("{} : [tag:{}] ublk io [sector:{}|len:{}|sub_cmd:{:b}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag,
          addr >> SECTOR_SHIFT, __iovec_len(iovecs, iovecs + nr_vecs), sub_cmd)
    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
}

io_result iSCSIDisk::sync_iov(uint8_t, iovec*, uint32_t, off_t) noexcept {
    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
}

} // namespace ublkpp
