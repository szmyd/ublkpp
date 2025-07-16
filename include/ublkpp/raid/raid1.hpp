#pragma once

#include <boost/uuid/uuid.hpp>
#include <sisl/utility/enum.hpp>
#include <ublkpp/lib/ublk_disk.hpp>

namespace ublkpp {

namespace raid1 {
struct SuperBlock;
ENUM(read_route, int8_t, EITHER = -1, DEVA = 0, DEVB = 1);
} // namespace raid1

class Raid1Disk : public UblkDisk {
    std::string _str_uuid;

    std::shared_ptr< UblkDisk > _device_a;
    std::shared_ptr< UblkDisk > _device_b;

    // Persistent state
    std::shared_ptr< raid1::SuperBlock > _sb;
    std::map< uint32_t, std::shared_ptr< uint64_t > > _dirty_pages;

    /// Some runtime parameters
    //  =======================
    uint32_t _chunk_size{0};            // Size each bit in the BITMAP represents
    bool const _read_from_dirty{false}; // Read from a device we *know* is dirty
    //  =======================

    // The current route to read consistently
    raid1::read_route _read_route{raid1::read_route::EITHER};

    // Counter for testing availability changes
    uint64_t _degraded_ops{0UL};

    // Asynchronous replies that did not go through io_uring
    std::map< ublksrv_queue const*, std::list< async_result > > _pending_results;

    // Internal routines
    io_result __dirty_bitmap(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q = nullptr,
                             ublk_io_data const* async_data = nullptr);
    io_result __dirty_pages(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                            ublk_io_data const* data);
    io_result __failover_read(sub_cmd_t sub_cmd, auto&& func);
    io_result __handle_async_retry(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                   ublk_io_data const* async_data);
    io_result __replicate(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len, ublksrv_queue const* q = nullptr,
                          ublk_io_data const* async_data = nullptr);

public:
    Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a, std::shared_ptr< UblkDisk > dev_b);
    ~Raid1Disk() override;

    /// UBlkDisk Interface Overrides
    /// ============================
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
    /// ============================
};
} // namespace ublkpp
