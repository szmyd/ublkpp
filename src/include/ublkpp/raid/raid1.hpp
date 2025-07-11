#pragma once

#include <boost/uuid/uuid.hpp>
#include <ublkpp/lib/ublk_disk.hpp>

namespace ublkpp {

namespace raid1 {
struct SuperBlock;
}

class Raid1Disk : public UblkDisk {
    std::shared_ptr< UblkDisk > _device_a;
    std::shared_ptr< UblkDisk > _device_b;
    raid1::SuperBlock* _sb;

    // Initially we issue OPs to DevA first, this flag switchs the order
    bool _route_to_b{false};
    uint64_t _degraded_ops{0UL};

    bool __dirty_bitmap(sub_cmd_t sub_cmd);
    io_result __failover_read(sub_cmd_t sub_cmd, auto&& func);
    io_result __replicate(sub_cmd_t sub_cmd, auto&& func);

public:
    Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a, std::shared_ptr< UblkDisk > dev_b);
    ~Raid1Disk() override;

    std::string type() const override { return "Raid1"; }
    std::list< int > open_for_uring(int const iouring_device) override;

    uint8_t route_size() const override { return 1; }

    void collect_async(ublksrv_queue const*, std::list< async_result >& compl_list) override;
    // RAID-1 Devices can not sit on-top of non-O_DIRECT devices, so there's nothing to flush
    io_result handle_flush(ublksrv_queue const*, ublk_io_data const*, sub_cmd_t) override { return 0; }
    io_result handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                             uint64_t addr) override;

    io_result async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                        uint32_t nr_vecs, uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
};
} // namespace ublkpp
