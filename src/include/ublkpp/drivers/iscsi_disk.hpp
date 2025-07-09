#pragma once

#include <ublkpp/lib/ublk_disk.hpp>

namespace ublkpp {

class iSCSIDisk : public UblkDisk {
public:
    iSCSIDisk();
    ~iSCSIDisk() override;

    std::string type() const override { return "iSCSIDisk"; }
    std::list< int > open_for_uring(int const) override { return {}; }

    io_result handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) override;
    io_result handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                             uint64_t addr) override;

    io_result async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                        uint32_t nr_vecs, uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
};
} // namespace ublkpp
