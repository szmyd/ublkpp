#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <optional>
#include <sys/uio.h>
#include <vector>

#include <liburing.h>
#include <ublksrv.h>

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
// Number of concurrent async I/O slots in the resync ring.
constexpr uint32_t k_resync_slots = 8;
// Ring depth: each slot issues at most 1 SQE at a time (READ, WRITE, and bitmap-flush are
// sequential within the merged coroutine). k_resync_slots active SQEs + k_resync_slots headroom
// for the next batch + 1 for the STOPPING cancel-all SQE.
constexpr uint32_t k_resync_ring_depth = k_resync_slots * 2 + 1;

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

// One in-flight async copy slot. Each slot holds a single merged coroutine that performs
// the full READ → Phase-2-check → WRITE → bitmap-flush sequence. The fake_iod/fake_data
// fields let async_iov() be called directly without a real ublksrv_queue; only ring_ptr is
// accessed by next_sqe() and async_iov(). A slot is free when !task || task->done().
struct ResyncSlot {
    async_io io{};              // per-slot cqe_state pool (capacity 1; cleared before each async_iov)
    ublksrv_io_desc fake_iod{}; // op_flags set to READ, WRITE, or WRITE (bitmap) inside the coroutine
    ublk_io_data fake_data{};   // fake_data.iod = &fake_iod; fake_data.private_data = &io
    iovec slot_iov{};           // iov_base points into _slot_buf_base; iov_len set per chunk
    std::optional< hot_task< int > > task{};
    uint64_t lba{0};
    uint32_t len{0};
    uint64_t gen_before{0}; // RegionTracker generation snapshot for Phase 2 check
};

// Thread model: tick(), drain_cqes(), and all ResyncSlot state are owned exclusively by I/O
// queue thread 0 (via Raid1Disk::resync_tick()). RegionTracker (enqueue/dequeue_write) is
// lock-free and safe to call from any I/O queue thread concurrently. For nr_hw_queues > 1,
// prepare() is called from multiple queue threads but toggle_resync(true) fires only on the
// first call — Raid1Disk's _nr_hw_queues atomic ensures this.
//
// Lifecycle: launch() initialises session state and CASes IDLE→ACTIVE. Queue thread 0 calls
// tick() after every CQE batch; each tick() does one non-blocking sweep (submit pending SQEs,
// peek available CQEs, fill new slots). When stop() CASes ACTIVE→STOPPING, the next tick()
// submits IORING_ASYNC_CANCEL_ANY and drains until all slots are free, then CASes STOPPING→IDLE.
// drain() provides a synchronous fallback for the final cleanup in run_queue_loop's exit path.
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

    // Private io_uring ring owned exclusively by queue thread 0 (SINGLE_ISSUER).
    // Lazily initialized by __ensure_ring() on first launch().
    io_uring _own_ring{};
    ublksrv_queue _own_queue{};
    bool _own_ring_initialized{false}; // true iff __ensure_ring() created _own_ring
    // Active queue for this resync task. Points to _own_queue.
    // null until first launch(); __ensure_ring() initializes it.
    // Only touched on queue thread 0 (SINGLE_ISSUER); no atomic needed.
    ublksrv_queue* _resync_queue{nullptr};
    void* _slot_buf_base{nullptr}; // aligned buffer pool: k_resync_slots × _max_size bytes
    std::vector< ResyncSlot > _slots;

    // Per-session state: valid when ACTIVE or STOPPING; reset to defaults on transition to IDLE.
    std::shared_ptr< MirrorDevice > _clean_mirror;
    std::shared_ptr< MirrorDevice > _dirty_mirror;
    std::function< void() > _complete_cb;
    std::string _session_uuid;
    uint64_t _resync_skip_from{0};
    uint32_t _consecutive_unavail{0};
    uint32_t _nr_pages{0};
    bool _cancel_submitted{false}; // true once IORING_ASYNC_CANCEL_ANY was submitted in STOPPING
    uint64_t _initial_resync_size{0};
    std::chrono::steady_clock::time_point _resync_start{};
    std::chrono::steady_clock::time_point _next_probe_time{};

    // State access helpers
    resync_state __load_state() const noexcept { return _state.load(std::memory_order_acquire); }

    bool __cas_state(resync_state& expected, resync_state desired) noexcept {
        return _state.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire);
    }

    // Per-slot merged coroutine: READ → Phase-2-check (sync) → WRITE → async-bitmap-flush.
    // Handles -EAGAIN (ring full) and -ECANCELED (cancel-all during STOPPING) by returning
    // immediately without marking mirrors unavailable. All error/unavail logic is here;
    // the drain loop only needs to poll done() and reset the slot.
    disk_task< int > __resync_slot_coro(ResyncSlot& slot, MirrorDevice& clean, MirrorDevice& dirty) noexcept;

    // Drives a CQE to the waiting cqe_state coroutine. Extracts the cqe_state* from
    // user_data (bit 63 tag + pointer), writes the result, marks it ready, and resumes
    // the suspended async_iov coroutine. After this call, slot.task->done() is true.
    void __process_cqe(io_uring_cqe* cqe) noexcept;

    // Returns true if any slot has an in-flight (not-yet-done) task.
    [[nodiscard]] bool has_in_flight() const noexcept;

    // Ensures _resync_queue is set. Creates _own_ring/_own_queue (COOP_TASKRUN | SINGLE_ISSUER).
    // Returns false if ring creation fails; launch() aborts the resync in that case.
    [[nodiscard]] bool __ensure_ring() noexcept;

    // Phase 2 conflict check: returns true if a write that overlaps [lba, lba+len) is either
    // still in-flight (overlaps) or completed after gen_before was snapshotted (completed_since).
    // Call snapshot_gen() BEFORE the Phase 1 check and the async read; pass the result here.
    [[nodiscard]] bool __phase2_conflict(uint64_t lba, uint32_t len, uint64_t gen_before) const noexcept;

    // Generic state transition helper - reduces duplication across launch/stop.
    // noinline: gcov attributes inlined template instructions to the call-site line numbers
    // rather than to the template body, making the entire retry loop appear uncovered.
    template < typename StateHandler >
    [[gnu::noinline]] bool __transition_to(resync_state initial, resync_state target, StateHandler&& handler) noexcept;

    // Drains all pending CQEs from the resync ring and delivers each to its waiting coroutine.
    // Must only be called on queue thread 0 (SINGLE_ISSUER constraint).
    void drain_cqes() noexcept;

    // Increments _yield_count (so tests can observe sweep completion) and returns the current
    // state. No sleep, no state transition — the queue thread's submit_and_wait_timeout call
    // provides natural I/O backpressure.
    // Incremented at the END of each tick() sweep, after all CQEs for this iteration
    // have been processed.
    resync_state __yield() noexcept;

    // Record resync-complete metrics and CAS state back to IDLE.
    // Called once when bitmap is clean (ACTIVE→IDLE) or after STOPPING drain completes.
    void __finish_session(resync_state final_state) noexcept;

    // Synchronous STOPPING drain: submit cancel-all, loop until all in-flight SQEs resolve,
    // then __finish_session(STOPPING). Safe to call when queue thread 0 is the sole accessor
    // of _own_ring (during drain() or the destructor's fallback path).
    void __drain_stopping() noexcept;

public:
    Raid1ResyncTask(std::shared_ptr< raid1::Bitmap >& bitmap, uint64_t offset, uint32_t io_size, uint32_t max_io,
                    uint32_t slot_count = k_default_slot_count, uint32_t chunk_size = k_min_chunk_size,
                    std::shared_ptr< ublkpp::UblkRaidMetrics > metrics = nullptr);
    ~Raid1ResyncTask() noexcept;

    // Probe a mirror device: reads at reserved_size, clears unavail on success,
    // sets unavail on failure. Returns true if device is available.
    static bool probe_mirror(MirrorDevice& mirror, uint64_t reserved_size) noexcept;

    void clean_region(uint64_t addr, uint32_t len, MirrorDevice& clean_device);

    // Initialises session state and CASes IDLE→ACTIVE. Returns immediately; the actual I/O
    // is driven by successive tick() calls on queue thread 0. No-op if already ACTIVE.
    // complete: called when resync finishes naturally (bitmap clean); not called on stop().
    void launch(std::string const& str_uuid, std::shared_ptr< MirrorDevice > clean_mirror,
                std::shared_ptr< MirrorDevice > dirty_mirror, std::function< void() >&& complete);

    // CASes ACTIVE→STOPPING (non-blocking). The next tick() call will submit
    // IORING_ASYNC_CANCEL_ANY and drain all in-flight SQEs before transitioning to IDLE.
    // Callers that need a synchronous stop must use drain() instead, or poll is_active().
    void stop() noexcept;

    // Performs one non-blocking resync sweep: submits pending SQEs to the resync ring, peeks
    // available CQEs, fills new copy slots. Must be called only on queue thread 0 (SINGLE_ISSUER).
    // No-op when IDLE. When STOPPING: cancels in-flight SQEs and, once drained, transitions to IDLE.
    void tick() noexcept;

    // Synchronous drain: if ACTIVE, CASes to STOPPING first; then loops until all in-flight
    // SQEs complete and transitions to IDLE. Called by run_queue_loop's exit path (queue thread 0)
    // and by the destructor fallback (after queue threads have joined).
    void drain() noexcept;

    // Returns true when a resync session is in progress (state == ACTIVE).
    bool is_active() const noexcept { return __load_state() == resync_state::ACTIVE; }

    // Returns true when no resync session is in progress (state == IDLE).
    bool is_idle() const noexcept { return __load_state() == resync_state::IDLE; }

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
