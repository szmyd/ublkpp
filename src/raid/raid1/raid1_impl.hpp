#pragma once

#include <map>
#include <memory>

#include "ublkpp/raid/raid1.hpp"
#include "metrics/ublk_raid_metrics.hpp"
#include "raid1_superblock.hpp"

namespace ublkpp {

struct UblkSystemMetrics;

namespace raid1 {

// Forward declarations
class Bitmap;
class Raid1ResyncTask;

struct MirrorDevice {
    MirrorDevice(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > device);
    std::shared_ptr< UblkDisk > const disk;
    std::shared_ptr< SuperBlock > sb; // Only used during load_superblock time
    std::atomic_flag unavail;

    bool new_device{true};
};

class Raid1DiskImpl : public UblkDisk {
    boost::uuids::uuid const _uuid;
    std::string const _str_uuid;
    uint64_t reserved_size{0UL};

    std::shared_ptr< MirrorDevice > _device_a;
    std::shared_ptr< MirrorDevice > _device_b;

    // Persistent state
    std::shared_ptr< raid1::SuperBlock > _sb;
    std::shared_ptr< raid1::Bitmap > _dirty_bitmap;

    // Runtime cached state (to avoid races on _sb bitfields)
    std::atomic_uint8_t _read_route_cache{static_cast< uint8_t >(raid1::read_route::EITHER)};

    // For implementing round-robin reads
    raid1::read_route _last_read{raid1::read_route::DEVB};

    // Metrics
    std::shared_ptr< ublkpp::UblkRaidMetrics > _raid_metrics;
    // Asynchronous replies that did not go through io_uring
    std::map< ublksrv_queue const*, std::list< async_result > > _pending_results;

    // Active Re-Sync Task
    bool _resync_enabled{true};
    std::shared_ptr< Raid1ResyncTask > _resync_task;

    // Internal routines
    io_result __become_clean();
    io_result __become_degraded(sub_cmd_t sub_cmd, bool spawn_resync = true);
    io_result __failover_read(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len);
    io_result __handle_async_retry(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                   ublk_io_data const* async_data);
    io_result __replicate(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len, ublksrv_queue const* q = nullptr,
                          ublk_io_data const* async_data = nullptr, MirrorDevice* active_dev = nullptr,
                          MirrorDevice* backup_dev = nullptr, sub_cmd_t active_subcmd = 0U);
    bool __swap_device(std::string const& outgoing_device_id, std::shared_ptr< MirrorDevice >& incoming_mirror);

    raid1::read_route __get_read_route() const {
        return static_cast< raid1::read_route >(_read_route_cache.load(std::memory_order_acquire));
    }
    bool __set_read_route(uint8_t& old_route, uint8_t new_route) {
        return _read_route_cache.compare_exchange_strong(old_route, new_route);
    }

public:
    Raid1DiskImpl(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a, std::shared_ptr< UblkDisk > dev_b,
                  std::string const& parent_id = "");
    ~Raid1DiskImpl() override;

    /// Raid1Disk API
    /// =============
    std::shared_ptr< UblkDisk > swap_device(std::string const& old_device_id, std::shared_ptr< UblkDisk > new_device);
    raid1::array_state replica_states() const;
    uint64_t get_reserved_size() const { return reserved_size; }
    void toggle_resync(bool t);
    std::pair< std::shared_ptr< UblkDisk >, std::shared_ptr< UblkDisk > > replicas() const;
    /// =============

    /// UBlkDisk Interface Overrides
    /// ============================
    std::string id() const override { return "RAID1"; }
    std::list< int > open_for_uring(int const iouring_device) override;

    uint8_t route_size() const override { return 1; }

    void on_io_complete(ublk_io_data const* data, sub_cmd_t sub_cmd, int res) override;

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

} // namespace raid1

} // namespace ublkpp
