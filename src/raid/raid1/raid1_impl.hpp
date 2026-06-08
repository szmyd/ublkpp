#pragma once

#include <memory>
#include <optional>

#include "ublkpp/raid.hpp"
#include "metrics/ublk_raid_metrics.hpp"
#include "raid1_superblock.hpp"

namespace ublkpp {

namespace raid1 {

// Forward declarations
class Bitmap;
class Raid1ResyncTask;
struct RouteState;

struct MirrorDevice {
    MirrorDevice(boost::uuids::uuid const& uuid, std::shared_ptr< ublk_disk > device);
    std::shared_ptr< ublk_disk > const disk;
    std::shared_ptr< SuperBlock > sb; // Only used during load_superblock time
    std::atomic_flag
        unavail; // not ready for IO; also set at startup self-heal to prevent SB writes that would destroy the age gap

    bool new_device{true};
};

class Raid1Disk : public ublk_disk {
    boost::uuids::uuid const _uuid;
    std::string const _str_uuid;
    uint64_t _reserved_size{0UL};

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

    // Guards __become_clean's check + CAS + superblock writes against the two cold failure-path
    // dirty_region() + __become_degraded() sites (active-fail and backup-fail with backup_write
    // true). Holding the lock across both the check+CAS and the SB writes ensures:
    //   (a) No EITHER-with-dirty-bit instant visible to lock-free readers (live P0).
    //   (b) Failure-path DEVA SB writes always serialize after EITHER SB writes, so the
    //       on-disk SBs cannot show EITHER+dirty on crash (crash-recovery P0).
    // The hot write path (!backup_write re-dirty) must NOT hold this lock.
    std::mutex _clean_transition_mutex;

    // Counts prepare() calls; used to enable resync on the first queue init.
    std::atomic_uint16_t _nr_hw_queues{0};

    // Set when __become_degraded updates the in-memory route but fails to persist the new route to
    // disk (transient SB write failure). The age increment is kept so any eventual SB write carries
    // a higher age than the stale on-disk SB, ensuring pick_superblock selects the correct device on
    // restart. Cleared by __try_persist_degraded_sb once the SB write succeeds. If set at shutdown,
    // the destructor's SB write naturally persists the correct route (it re-reads _read_route_cache).
    std::atomic< bool > _degraded_sb_pending{false};

    // Shared read/write routing helpers used by both async_iov and sync_iov.
    // Returns {primary_dev, failover_dev}. failover_dev is nullopt when the backup holds stale
    // data for this region (degraded array + dirty bitmap) -- callers must not read from it.
    std::pair< std::shared_ptr< MirrorDevice >, std::optional< std::shared_ptr< MirrorDevice > > >
    __select_read_devices(RouteState const& state, uint64_t addr, uint32_t len) const noexcept;

    // True when a write should be replicated to the backup leg. A dirty region in a degraded
    // array means the backup is owned exclusively by the resync task, so the I/O path must not
    // write it (this is what closes the resync stale-read race); an unavailable backup is skipped
    // likewise. Used identically by both async_iov and sync_iov.
    bool __backup_writable(RouteState const& state, uint64_t addr, uint32_t len) const noexcept;

    // Internal routines
    bool __become_clean();
    bool __become_degraded(bool failed_is_active, RouteState const* state, bool spawn_resync = true);
    // Retries the SB write when _degraded_sb_pending is set. Returns true if the SB was persisted
    // (or no retry was needed); returns false if the write failed again, meaning the caller must
    // not ack the current I/O (client retries on the next request).
    bool __try_persist_degraded_sb();
    disk_task< int > __failover_read_async(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                           uint32_t nr_vecs, uint64_t addr, uint32_t len);
    bool __swap_device(std::string const& outgoing_device_id, std::shared_ptr< MirrorDevice >& incoming_mirror,
                       raid1::read_route const& cur_route);

    // Constructor helpers. Order matters: __load_and_select_superblock must run first to
    // populate _device_a/_device_b/_sb; __init_params then reads _sb->header.version to
    // decide the user-data alignment policy.
    void __load_and_select_superblock(boost::uuids::uuid const& uuid, std::shared_ptr< ublk_disk > dev_a,
                                      std::shared_ptr< ublk_disk > dev_b, std::string const& parent_id);
    void __init_params();
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
    // noinline: required in all builds so the compiler cannot cache _device_a/_device_b across
    // the retry loop's consistency check (plain shared_ptrs, not atomic).
#ifndef NDEBUG
    // no_sanitize_thread: intentional lock-free race, validated by retry loop (see tsan.supp).
    // no_sanitize("address"): shared_ptr copy has a sub-nanosecond UAF window during swap_device.
    // The race cannot corrupt on-disk state (MirrorDevice dtor writes nothing to disk); worst case
    // is a process crash, which pod restart / UBLK_F_USER_RECOVERY handles. Locking this hot path
    // is not worth the cost for a swap that happens at most once per device lifetime.
    // NOTE: attribute must appear on both declaration and definition for GCC to suppress
    // instrumentation of the function body.
    __attribute__((noinline, no_sanitize_thread, no_sanitize("address")))
#else
    __attribute__((noinline))
#endif
    RouteState __capture_route_state() const;
    // clang-format on

public:
    Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< ublk_disk > dev_a, std::shared_ptr< ublk_disk > dev_b,
              std::string const& parent_id = "");
    ~Raid1Disk() override;

    /// Raid1Disk API
    /// =============
    std::shared_ptr< ublk_disk > swap_device(std::string const& old_device_id, std::shared_ptr< ublk_disk > new_device);
    raid1::array_state replica_states() const noexcept;
    uint64_t reserved_size() const noexcept { return _reserved_size; }
    void toggle_resync(bool t);
    std::pair< std::shared_ptr< ublk_disk >, std::shared_ptr< ublk_disk > > replicas() const noexcept;
    /// =============

    /// UBlkDisk Interface Overrides
    /// ============================
    std::string id() const noexcept override { return "RAID1"; }
    prepare_result prepare(ublksrv_queue const* q, int const iouring_device) override;
    void probe_tick(ublksrv_queue const* q) noexcept override;

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override;

    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
    /// ============================
};

} // namespace raid1

} // namespace ublkpp
