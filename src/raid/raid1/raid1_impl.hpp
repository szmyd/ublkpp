#pragma once

#include <memory>

#include "ublkpp/raid/raid1.hpp"
#include "metrics/ublk_raid_metrics.hpp"
#include "raid1_avail_probe.hpp"
#include "raid1_superblock.hpp"

namespace ublkpp {

namespace raid1 {

// Forward declarations
class Bitmap;
class Raid1ResyncTask;
struct RouteState;

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
    std::atomic< raid1::read_route > _read_route_cache{raid1::read_route::EITHER};

    // Metrics
    std::shared_ptr< ublkpp::UblkRaidMetrics > _raid_metrics;
    // Active Re-Sync Task
    std::atomic< bool > _resync_enabled{true};
    std::shared_ptr< Raid1ResyncTask > _resync_task;

    // Guards: (1) swap_device() - serializes concurrent callers on _device_a/_device_b mutations.
    //         (2) _pending_results - serializes prepare() insertions across queue threads.
    std::mutex _ctrl_lock;

    // Multi-queue idle tracking: probe starts when all queues are idle, stops on any active transition
    std::atomic_uint16_t _nr_hw_queues{0};
    std::atomic_uint16_t _idle_queue_count{0};

    // Idle-scoped periodic health monitors
    std::mutex _idle_probe_lock;
    Raid1AvailProbeTask _idle_probe_a;
    Raid1AvailProbeTask _idle_probe_b;

    // Internal routines
    io_result __become_clean();
    io_result __become_degraded(bool failed_is_active, RouteState const* state, bool spawn_resync = true);
    disk_task< int > __failover_read_async(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                           uint32_t nr_vecs, uint64_t addr, uint32_t len);
    bool __swap_device(std::string const& outgoing_device_id, std::shared_ptr< MirrorDevice >& incoming_mirror,
                       raid1::read_route const& cur_route);

    // Constructor helpers
    void __init_params(std::shared_ptr< UblkDisk > const& dev_a, std::shared_ptr< UblkDisk > const& dev_b);
    void __load_and_select_superblock(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a,
                                      std::shared_ptr< UblkDisk > dev_b, std::string const& parent_id);
    void __init_bitmap_and_degraded_route();
    void __become_active();

    // ☠️ ☠️ ☠️  DANGER: LOCK-FREE SYNCHRONIZATION - DO NOT MODIFY  ☠️ ☠️ ☠️
    //
    // This function uses a CAREFULLY DESIGNED lock-free read-retry pattern with
    // application-level validation. Modifications can introduce:
    // - Use-after-free bugs (torn shared_ptr reads)
    // - ABA problems (if validation is weakened)
    // - Memory corruption (if retry logic is broken)
    //
    // The code is INTENTIONALLY UNSAFE by C++ standard (data race on shared_ptr)
    // but SAFE in practice (on x86-64) due to:
    // 1. Read-validate-retry loop catches torn reads
    // 2. Pointer-sized reads are atomic on x86-64
    // 3. We never use inconsistent data (validation ensures this)
    //
    // TSAN correctly flags this as a data race - suppression file required.
    // DO NOT TOUCH unless you fully understand lock-free memory models.
    //
    // ☠️ ☠️ ☠️  YOU HAVE BEEN WARNED  ☠️ ☠️ ☠️
    // clang-format off
#ifndef NDEBUG
    __attribute__((noinline, no_sanitize_thread))
#endif
    RouteState __capture_route_state() const;
    // clang-format on

public:
    Raid1DiskImpl(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a, std::shared_ptr< UblkDisk > dev_b,
                  std::string const& parent_id = "");
    ~Raid1DiskImpl() override;

    /// Raid1Disk API
    /// =============
    std::shared_ptr< UblkDisk > swap_device(std::string const& old_device_id, std::shared_ptr< UblkDisk > new_device);
    raid1::array_state replica_states() const noexcept;
    uint64_t get_reserved_size() const noexcept { return reserved_size; }
    void toggle_resync(bool t);
    std::pair< std::shared_ptr< UblkDisk >, std::shared_ptr< UblkDisk > > replicas() const noexcept;
    /// =============

    /// UBlkDisk Interface Overrides
    /// ============================
    std::string id() const noexcept override { return "RAID1"; }
    std::list< int > prepare(ublksrv_queue const* q, int const iouring_device) override;
    void idle_transition(ublksrv_queue const* q, bool enter) noexcept override;

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override;

    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
    /// ============================
};

} // namespace raid1

} // namespace ublkpp
