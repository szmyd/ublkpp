#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <sys/uio.h>
#include <thread>

extern "C" {
#include <liburing.h>
}

#include "metrics/ublk_raid_metrics.hpp"
#include "raid1_superblock.hpp"
#include "region_tracker.hpp"
#include "resync_constants.hpp"
#include "ublkpp/raid.hpp"

namespace ublkpp::raid1 {

using namespace std::chrono_literals;
constexpr uint32_t k_default_slot_count = 256;

class Bitmap;
class MirrorDevice;

// One async copy operation in flight: READ from clean → WRITE to dirty.
// Only used when both disks support prep_iov_sqe(); otherwise sync_iov is used directly.
struct ResyncSlot {
    enum class Phase : uint8_t { FREE, READ_PENDING, WRITE_PENDING };
    Phase phase{Phase::FREE};
    uint64_t lba{0};
    uint32_t len{0};
    uint64_t gen_before{0}; // RegionTracker generation snapshot before the READ SQE was submitted
    void* buf{nullptr};     // posix_memalign'd I/O buffer; owned by the slot
    iovec iov{};
};

class Raid1ResyncTask {

    // Global counter for active resyncs across all RAID1 devices
    static inline std::atomic_uint32_t s_active_resyncs{0};

    // Number of times a resync sweep has yielded; tests poll this to wait for ≥1 sweep
    // without relying on wall-clock timing.
    std::atomic< uint64_t > _yield_count{0};

    std::shared_ptr< raid1::Bitmap > const _dirty_bitmap;
    std::shared_ptr< ublkpp::UblkRaidMetrics > const _metrics;

    // Smallest I/O both devices support (alignment for posix_memalign).
    uint32_t const _io_size;
    // Largest I/O both devices support (max chunk size per copy op).
    uint32_t const _max_size;
    // Byte offset added to every logical address before issuing I/O; skips the on-disk bitmap.
    uint64_t const _offset;

    RegionTracker _region_tracker;

    // Protects _resync_task.joinable() check in launch() against concurrent callers.
    std::mutex _launch_lock;
    std::thread _resync_task;

    // Set to true by stop() to request the resync thread to exit.
    std::atomic< bool > _stop{false};

    // io_uring ring used by the async path; owned exclusively by the resync thread.
    io_uring _ring{};
    bool _ring_initialized{false};

    // Fixed-size slot array for the async pipeline.
    std::array< ResyncSlot, k_resync_slots > _slots{};
    uint32_t _in_flight{0}; // count of READ_PENDING + WRITE_PENDING slots

    // ── Private helpers ───────────────────────────────────────────────────────────────────────
    void _run(std::shared_ptr< MirrorDevice > clean, std::shared_ptr< MirrorDevice > dirty,
              std::function< void() > complete);

    // Initialise the io_uring ring and per-slot buffers.
    // Returns true if both disks support prep_iov_sqe() and the ring was set up successfully.
    // On any failure the ring is left uninitialized and the sync fallback is used.
    bool _init_ring(ublk_disk& clean_disk, ublk_disk& dirty_disk) noexcept;

    // Async (io_uring) copy loop.  Called when _init_ring() returns true.
    void _run_uring(MirrorDevice& clean, MirrorDevice& dirty);

    // Fills free async slots with READ SQEs for the next dirty chunks starting at cursor.
    // cursor_{lba,sz,skip_from} are updated in place as slots are filled.
    void _fill_slots(MirrorDevice& clean, uint64_t& cursor_lba, uint32_t& cursor_sz,
                     uint64_t& cursor_skip_from) noexcept;

    // Harvests all available CQEs; submits WRITE SQEs for completed READs and calls
    // clean_region() for completed WRITEs that pass the Phase-2 check.
    void _process_cqes(MirrorDevice& clean, MirrorDevice& dirty) noexcept;

    // Submits a cancel-all SQE, drains all CQEs to zero, then calls io_uring_queue_exit().
    void _drain_and_exit() noexcept;

    // Synchronous (sync_iov) copy loop.  Used as fallback when prep_iov_sqe() is unavailable
    // (e.g. TestDisk, composite disks).  Behaves identically to the old __run() but without
    // the SLEEPING/STOPPING state machine — stop() is detected via _stop.load() checks.
    void _run_sync(MirrorDevice& clean, MirrorDevice& dirty);

    // Sleeps for `dur`, checking _stop every 500µs. Returns false if _stop was set.
    bool _sleep_check_stop(std::chrono::microseconds dur) noexcept;

public:
    Raid1ResyncTask(std::shared_ptr< raid1::Bitmap >& bitmap, uint64_t offset, uint32_t io_size, uint32_t max_io,
                    uint32_t slot_count = k_default_slot_count, uint32_t chunk_size = k_min_chunk_size,
                    std::shared_ptr< ublkpp::UblkRaidMetrics > metrics = nullptr);
    ~Raid1ResyncTask() noexcept;

    // Probe a mirror device: reads at reserved_size, clears unavail on success,
    // sets unavail on failure. Returns true if device is available.
    static bool probe_mirror(MirrorDevice& mirror, uint64_t reserved_size) noexcept;

    void clean_region(uint64_t addr, uint32_t len, MirrorDevice& clean_device);

    // Spawn a background resync thread if none is running. No-op if one is already active.
    void launch(std::string const& str_uuid, std::shared_ptr< MirrorDevice > clean_mirror,
                std::shared_ptr< MirrorDevice > dirty_mirror, std::function< void() >&& complete);

    // Signal the resync thread to stop and block until it exits.
    void stop() noexcept;

    void enqueue_write(uint64_t lba, uint32_t len) noexcept { _region_tracker.track(lba, len); }

    void dequeue_write(uint64_t lba, uint32_t len) noexcept { _region_tracker.untrack(lba, len); }

    // Number of completed sweeps. Tests poll this to wait for ≥N sweeps without wall-clock guessing.
    uint64_t yield_count() const noexcept { return _yield_count.load(std::memory_order_acquire); }
};

// RAII guard that calls enqueue_write() on construction and dequeue_write() on destruction.
// Call release() to dequeue early (e.g. at a specific co_await point in a coroutine).
class ResyncWriteGuard {
public:
    ResyncWriteGuard(Raid1ResyncTask& task, uint64_t lba, uint32_t len) noexcept : _task(&task), _lba(lba), _len(len) {
        _task->enqueue_write(_lba, _len);
    }
    ~ResyncWriteGuard() noexcept {
        if (_task) _task->dequeue_write(_lba, _len);
    }
    void release() noexcept {
        if (_task) {
            _task->dequeue_write(_lba, _len);
            _task = nullptr;
        }
    }
    ResyncWriteGuard(ResyncWriteGuard&&) = delete;
    ResyncWriteGuard(ResyncWriteGuard const&) = delete;
    ResyncWriteGuard& operator=(ResyncWriteGuard&&) = delete;
    ResyncWriteGuard& operator=(ResyncWriteGuard const&) = delete;

private:
    Raid1ResyncTask* _task;
    uint64_t _lba;
    uint32_t _len;
};

} // namespace ublkpp::raid1
