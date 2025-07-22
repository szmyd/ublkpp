#pragma once

#include <atomic>
#include <boost/uuid/uuid.hpp>
#include <sisl/utility/enum.hpp>
#include <ublkpp/lib/ublk_disk.hpp>

namespace ublkpp {

namespace raid1 {
class Bitmap;
struct SuperBlock;
ENUM(read_route, uint8_t, EITHER = 0, DEVA = 1, DEVB = 2);
} // namespace raid1

class Raid1Disk : public UblkDisk {
    std::string _str_uuid;

    std::shared_ptr< UblkDisk > _device_a;
    std::shared_ptr< UblkDisk > _device_b;

    // Persistent state
    std::atomic_flag _is_degraded;
    std::shared_ptr< raid1::SuperBlock > _sb;
    std::unique_ptr< raid1::Bitmap > _dirty_bitmap;

    // For implementing round-robin reads
    raid1::read_route _last_read{raid1::read_route::DEVB};

    // Active Re-Sync Task
    std::atomic< uint8_t > _resync_state;

    // Asynchronous replies that did not go through io_uring
    std::map< ublksrv_queue const*, std::list< async_result > > _pending_results;

    // Internal routines
    io_result __become_clean();
    io_result __become_degraded(sub_cmd_t sub_cmd);
    io_result __clean_pages(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                            ublk_io_data const* data);
    io_result __dirty_pages(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                            ublk_io_data const* data);
    io_result __failover_read(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len);
    io_result __handle_async_retry(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                   ublk_io_data const* async_data);
    io_result __replicate(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len, ublksrv_queue const* q = nullptr,
                          ublk_io_data const* async_data = nullptr);
    void __resync_task();

public:
    Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a, std::shared_ptr< UblkDisk > dev_b);
    ~Raid1Disk() override;

    /// UBlkDisk Interface Overrides
    /// ============================
    std::string type() const override { return "Raid1"; }
    std::list< int > open_for_uring(int const iouring_device) override;

    uint8_t route_size() const override { return 1; }

    void idle_transition(ublksrv_queue const*, bool) override;

    io_result handle_internal(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovec,
                              uint32_t nr_vecs, uint64_t addr, int res) override;
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
