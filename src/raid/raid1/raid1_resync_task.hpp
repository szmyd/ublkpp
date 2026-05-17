#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <optional>
#include <sys/uio.h>
#include <thread>
#include <vector>

#include <exec/task.hpp>
#include <liburing.h>
#include <ublksrv.h>

#include "lib/resync_dispatch.hpp"
#include "metrics/ublk_raid_metrics.hpp"
#include "raid1_superblock.hpp"
#include "region_tracker.hpp"
#include "ublkpp/lib/cqe_state.hpp"
#include "ublkpp/lib/disk_task.hpp"
#include "ublkpp/raid.hpp"

namespace ublkpp::raid1 {

using namespace std::chrono_literals;
constexpr auto k_state_spin_time = 50us;
constexpr uint32_t k_default_slot_count = 256;
// Number of concurrent async I/O slots in the target-level resync ring.
constexpr uint32_t k_resync_slots = 8;
// Ring depth: 2× slots provides headroom for READ and WRITE SQEs in flight simultaneously.
constexpr uint32_t k_resync_ring_depth = k_resync_slots * 2;

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

// One in-flight async copy slot. Each slot tracks a single READ→WRITE resync operation via the
// target-level resync ring. The fake_iod/fake_data fields let us call disk->async_iov() directly
// without a real ublksrv_queue; only ring_ptr is accessed by next_sqe() and async_iov().
struct ResyncSlot {
    async_io io{};              // per-slot cqe_state pool (reserved for 1 state per phase)
    ublksrv_io_desc fake_iod{}; // op_flags set to READ or WRITE before each async_iov call
    ublk_io_data fake_data{};   // fake_data.iod = &fake_iod; fake_data.private_data = &io
    iovec slot_iov{};           // iov_base points into _slot_buf_base; iov_len set per chunk
    std::optional< hot_task< int > > task{};
    uint64_t lba{0};
    uint32_t len{0};
    uint64_t gen_before{0}; // RegionTracker generation snapshot for Phase 2 check
    enum class Phase : uint8_t { FREE, READ_PENDING, WRITE_PENDING } phase{Phase::FREE};
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

    // Fallback ring + queue used when no target-level queue is provided to launch().
    // Lazily initialized by __ensure_ring() on the first _start() entry if _resync_queue is null.
    io_uring _own_ring{};
    ublksrv_queue _own_queue{};
    // Active queue for this resync task. Points to _own_queue (standalone/test fallback) or to a
    // target-level queue forwarded through Raid1Disk::toggle_resync → launch(). null until
    // first launch(); __ensure_ring() initializes it.
    ublksrv_queue* _resync_queue{nullptr};
    // Non-null in ublkpp_tgt context: launch() posts the resync coroutine factory here so the
    // per-volume run_resync_queue_loop can spawn it into the target's exec::async_scope.
    ResyncDispatcher* _resync_dispatch{nullptr};
    void* _slot_buf_base{nullptr}; // aligned buffer pool: k_resync_slots × _max_size bytes
    std::vector< ResyncSlot > _slots;

    // done_promise/done_future: signaled by __run_coro() on exit so stop() can block until the
    // coroutine has fully wound down (analogous to thread join() in the fallback path).
    std::promise< void > _done_promise;
    std::future< void > _done_future;

    std::mutex _launch_lock;
    std::thread _resync_task;

    // State access helpers
    resync_state __load_state() const noexcept { return _state.load(std::memory_order_acquire); }

    bool __cas_state(resync_state& expected, resync_state desired) noexcept {
        return _state.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire);
    }

    resync_state __run(auto& clean_mirror, auto& dirty_mirror) noexcept;

    // Coroutine version of _start() + __run(). Used by the dispatch (coroutine) path when a
    // ResyncDispatcher is available: run_resync_queue_loop spawns this into its async_scope and
    // drives CQEs from the shared _resync_ring. On exit, transitions state to IDLE and signals
    // _done_promise so stop() can return.
    exec::task< void > __run_coro(std::shared_ptr< MirrorDevice > clean_mirror,
                                  std::shared_ptr< MirrorDevice > dirty_mirror, std::function< void() > complete,
                                  std::string uuid);

    // Drives a CQE to the waiting cqe_state coroutine. Extracts the cqe_state* from
    // user_data (bit 63 tag + pointer), writes the result, marks it ready, and resumes
    // the suspended async_iov coroutine. After this call, slot.task->done() is true.
    void __process_cqe(io_uring_cqe* cqe) noexcept;

    // Returns true if any slot is not FREE (i.e. there is I/O in flight).
    [[nodiscard]] bool has_in_flight() const noexcept;

    // Ensures _resync_queue is set. If launch() already set it (production path), this is a no-op.
    // Otherwise creates _own_ring/_own_queue for standalone/test use.
    // Returns false if ring creation fails; _start() aborts the resync in that case.
    [[nodiscard]] bool __ensure_ring() noexcept;

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

    // resync_q: target-level queue forwarded from Raid1Disk::toggle_resync (null in tests → __ensure_ring fallback).
    // dispatch: if non-null, use the coroutine dispatch path (run_resync_queue_loop drives the ring);
    //           if null, fall back to the standalone thread path (__ensure_ring + _start()).
    void launch(std::string const& str_uuid, std::shared_ptr< MirrorDevice > clean_mirror,
                std::shared_ptr< MirrorDevice > dirty_mirror, std::function< void() >&& complete,
                ublksrv_queue* resync_q = nullptr, ResyncDispatcher* dispatch = nullptr);

    // Generic method to move Resync StateMachine to STOPPING
    void stop() noexcept;

    // Drains all pending CQEs from the resync ring and delivers each to its waiting coroutine.
    // Used by the dispatch-path test in place of run_resync_queue_loop.
    void drain_cqes() noexcept;

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
