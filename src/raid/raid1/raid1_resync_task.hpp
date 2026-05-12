#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <sys/uio.h>
#include <thread>

#include <liburing.h>

#include "metrics/ublk_raid_metrics.hpp"
#include "raid1_superblock.hpp"
#include "region_tracker.hpp"
#include "ublkpp/raid.hpp"

namespace ublkpp::raid1 {

using namespace std::chrono_literals;
constexpr auto k_state_spin_time = 50us;
constexpr uint32_t k_default_slot_count = 256;
// Depth of the dedicated resync io_uring ring: 2 SQEs per linked READ→WRITE pair + headroom.
constexpr uint32_t k_resync_ring_depth = 4;

class Bitmap;
class MirrorDevice;

// State transitions:
//   IDLE → ACTIVE (launch())
//   ACTIVE → STOPPING (stop() requested; io_uring wait_timeout provides the yield point)
//   STOPPING → IDLE (shutdown complete)
ENUM(resync_state, uint32_t, IDLE = 0, ACTIVE = 1, STOPPING = 2);

// State transition actions for __transition_to helper
enum class transition_action : uint8_t {
    RETRY,            // Continue CAS loop immediately
    RETRY_WITH_SLEEP, // Sleep k_state_spin_time then retry
    SUCCESS,          // Exit loop (CAS succeeded or found target state)
    EARLY_EXIT        // Exit caller function immediately
};

// Result from state transition handler callback
struct transition_result {
    resync_state next_state; // State to expect on next retry
    transition_action action;
};

class Raid1ResyncTask {

    // Global counter for active resyncs across all RAID1 devices
    static inline std::atomic_uint32_t s_active_resyncs{0};

    // Number of times __yield() has been called; used by tests to wait for at least one sweep
    // without relying on wall-clock timing.
    std::atomic< uint64_t > _yield_count{0};

    std::shared_ptr< raid1::Bitmap > const _dirty_bitmap;
    std::shared_ptr< ublkpp::UblkRaidMetrics > const _metrics;

    // The smallest I/O both devices support (RAID logical block size)
    uint32_t const _io_size;
    // The largest I/O both devices support
    uint32_t const _max_size;
    // This is the offset we should copy the disks @ to avoid writing on the BITMAP itself.
    uint64_t const _offset;

    std::atomic< resync_state > _state{resync_state::IDLE};
    static_assert(std::atomic< resync_state >::is_always_lock_free);

    // Tracks the LBA range of each in-flight write. Resync checks for overlap before
    // and after each copy so it only skips regions that actually conflict with a write;
    // unrelated regions proceed without any global pause.
    RegionTracker _region_tracker;

    std::mutex _launch_lock;
    std::thread _resync_task;

    // State access helpers
    resync_state __load_state() const noexcept { return _state.load(std::memory_order_acquire); }

    bool __cas_state(resync_state& expected, resync_state desired) noexcept {
        return _state.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire);
    }

    resync_state __run(auto& clean_mirror, auto& dirty_mirror, iovec* iov, io_uring* ring, int clean_fd,
                       int dirty_fd) noexcept;

    // Submits a linked READ_FIXED → WRITE_FIXED pair to the dedicated resync ring and waits
    // for both CQEs. The kernel executes the write immediately after the read without a
    // userspace round-trip. buf must be the buffer registered at index 0 via
    // io_uring_register_buffers.
    io_result __copy_region_async(io_uring& ring, int clean_fd, int dirty_fd, void* buf, uint32_t len,
                                  uint64_t addr) noexcept;

    // Generic state transition helper - reduces duplication across launch/stop.
    // noinline: gcov attributes inlined template instructions to the call-site line numbers
    // rather than to the template body, making the entire retry loop appear uncovered.
    template < typename StateHandler >
    [[gnu::noinline]] bool __transition_to(resync_state initial, resync_state target, StateHandler&& handler) noexcept;

    void _start(std::string str_uuid, std::shared_ptr< MirrorDevice >& clean_mirror,
                std::shared_ptr< MirrorDevice >& dirty_mirror, std::function< void() >&& complete);

    // Increments _yield_count (so tests can observe sweep completion) and returns the current
    // state. No sleep, no state transition — the io_uring submit_and_wait_timeout call in the
    // copy loop provides natural I/O backpressure.
    resync_state __yield() noexcept;

public:
    Raid1ResyncTask(std::shared_ptr< raid1::Bitmap >& bitmap, uint64_t offset, uint32_t io_size, uint32_t max_io,
                    uint32_t slot_count = k_default_slot_count, uint32_t chunk_size = k_min_chunk_size,
                    std::shared_ptr< ublkpp::UblkRaidMetrics > metrics = nullptr);
    ~Raid1ResyncTask() noexcept;

    // Probe a mirror device: reads at reserved_size, clears unavail on success,
    // sets unavail on failure. Returns true if device is available.
    static bool probe_mirror(MirrorDevice& mirror, uint64_t reserved_size) noexcept;

    void clean_region(uint64_t addr, uint32_t len, MirrorDevice& clean_device);

    void launch(std::string const& str_uuid, std::shared_ptr< MirrorDevice > clean_mirror,
                std::shared_ptr< MirrorDevice > dirty_mirror, std::function< void() >&& complete);

    // Generic method to move Resync StateMachine to STOPPING
    void stop() noexcept;

    void enqueue_write(uint64_t lba, uint32_t len) noexcept { _region_tracker.track(lba, len); }

    void dequeue_write(uint64_t lba, uint32_t len) noexcept { _region_tracker.untrack(lba, len); }

    // Number of times __yield() has been called. Tests poll this to wait for at least one
    // resync sweep without relying on wall-clock timing.
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
