#pragma once

#include <map>
#include <memory>

#include "ublkpp/raid/raid1.hpp"
#include "metrics/ublk_raid_metrics.hpp"
#include "raid1_avail_probe.hpp"
#include "raid1_superblock.hpp"

namespace ublkpp {

struct UblkSystemMetrics;

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
    std::atomic_uint8_t _read_route_cache{static_cast< uint8_t >(raid1::read_route::EITHER)};

    // Metrics
    std::shared_ptr< ublkpp::UblkRaidMetrics > _raid_metrics;
    // Asynchronous replies that did not go through io_uring
    std::map< ublksrv_queue const*, std::list< async_result > > _pending_results;

    // Active Re-Sync Task
    bool _resync_enabled{true};
    std::shared_ptr< Raid1ResyncTask > _resync_task;

    // Ensure exclusivity in __swap_device
    std::mutex _swap_lock;

    // Idle-scoped periodic health monitors
    Raid1AvailProbeTask _idle_probe_a;
    Raid1AvailProbeTask _idle_probe_b;

    // Internal routines
    io_result __become_clean();
    io_result __become_degraded(sub_cmd_t failed_path, RouteState const* state, bool spawn_resync = true);
    io_result __failover_read(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len,
                              RouteState const* state = nullptr);
    io_result __handle_async_retry(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                   ublk_io_data const* async_data);
    io_result __replicate(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len, ublksrv_queue const* q = nullptr,
                          ublk_io_data const* async_data = nullptr, RouteState* state = nullptr);
    bool __swap_device(std::string const& outgoing_device_id, std::shared_ptr< MirrorDevice >& incoming_mirror,
                       raid1::read_route const& cur_route);

    // Constructor helpers
    void __init_params(std::shared_ptr< UblkDisk > const& dev_a, std::shared_ptr< UblkDisk > const& dev_b);
    void __load_and_select_superblock(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a,
                                      std::shared_ptr< UblkDisk > dev_b, std::string const& parent_id);
    void __init_bitmap_and_degraded_route();
    void __become_active();

    raid1::read_route __get_read_route() const noexcept {
        return static_cast< raid1::read_route >(_read_route_cache.load(std::memory_order_acquire));
    }

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
    RouteState __capture_route_state(sub_cmd_t sub_cmd = 0) const;
    // clang-format on

    // CAS with uint8_t (for when caller already has uint8_t)
    bool __set_read_route(uint8_t& old_route, uint8_t new_route) noexcept {
        return _read_route_cache.compare_exchange_strong(old_route, new_route);
    }

    // CAS with read_route (convenience wrapper - eliminates casting at call sites)
    bool __set_read_route(raid1::read_route& old_route, raid1::read_route new_route) noexcept {
        auto old_val = static_cast< uint8_t >(old_route);
        bool result = __set_read_route(old_val, static_cast< uint8_t >(new_route));
        old_route = static_cast< raid1::read_route >(old_val); // Update with actual value read
        return result;
    }

    // Direct store (for initialization/non-CAS updates)
    void __store_read_route(raid1::read_route route) noexcept {
        _read_route_cache.store(static_cast< uint8_t >(route), std::memory_order_release);
    }

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
    std::list< int > open_for_uring(int const iouring_device) override;
    void idle_transition(ublksrv_queue const* q, bool enter) noexcept override;

    uint8_t route_size() const noexcept override { return 1; }

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
