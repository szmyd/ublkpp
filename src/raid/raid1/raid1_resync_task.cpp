#include "raid1_resync_task.hpp"

#include <ublksrv.h>

#include "lib/logging.hpp"
#include "bitmap.hpp"
#include "raid1_impl.hpp"
#include "resync_constants.hpp"
#include "resync_cursor.hpp"

namespace ublkpp::raid1 {

ResyncCursor::ResyncCursor(Bitmap& bm, uint64_t hint) noexcept {
    auto [l, s] = hint > 0 ? bm.next_dirty_after(hint) : bm.next_dirty();
    if (s == 0 && hint > 0) std::tie(l, s) = bm.next_dirty();
    lba = l;
    sz = s;
}

bool ResyncCursor::skip(uint32_t len, Bitmap& bm) noexcept {
    sz -= len;
    lba += len;
    if (sz == 0) {
        if (!any_copy) {
            skip_from = lba;
            return true;
        }
        std::tie(lba, sz) = bm.next_dirty();
        any_copy = false;
    }
    return false;
}

void ResyncCursor::skip_inflight(uint32_t len, Bitmap& bm) noexcept {
    sz -= len;
    lba += len;
    if (sz == 0) {
        std::tie(lba, sz) = bm.next_dirty();
        any_copy = false;
    }
}

void ResyncCursor::advance(uint32_t len, Bitmap& bm) noexcept {
    any_copy = true;
    sz -= len;
    lba += len;
    if (sz == 0) {
        std::tie(lba, sz) = bm.next_dirty();
        any_copy = false;
    }
}

Raid1ResyncTask::Raid1ResyncTask(std::shared_ptr< raid1::Bitmap >& bitmap, uint64_t offset, uint32_t io_size,
                                 uint32_t max_io, uint32_t slot_count, uint32_t chunk_size,
                                 std::shared_ptr< ublkpp::UblkRaidMetrics > metrics) :
        _dirty_bitmap(bitmap),
        _metrics(metrics),
        _io_size(io_size),
        _max_size(max_io),
        _offset(offset),
        _region_tracker(slot_count, chunk_size) {
    if (!_dirty_bitmap) throw std::runtime_error("No Bitmap");

    // Allocate the aligned buffer pool: k_resync_slots buffers of _max_size each.
    if (::posix_memalign(&_slot_buf_base, static_cast< size_t >(_io_size),
                         static_cast< size_t >(_max_size) * k_resync_slots) != 0) {
        RLOGW("Resync slot buffer allocation failed ({}); resync unavailable", strerror(errno))
        return;
    }

    // Reserve before resize so that all slot addresses are stable after construction:
    // fake_data.private_data = &s.io stores a pointer into the vector element, which would
    // dangle if a later push_back triggered reallocation.
    _slots.reserve(k_resync_slots);
    _slots.resize(k_resync_slots);
    auto* base = static_cast< char* >(_slot_buf_base);
    for (uint32_t i = 0; i < k_resync_slots; ++i) {
        auto& s = _slots[i];
        s.io._pool.reserve(1);
        s.fake_data.tag = -1; // service-loop convention: tag -1 means no ublksrv slot
        s.fake_data.iod = &s.fake_iod;
        s.fake_data.private_data = &s.io;
        s.slot_iov.iov_base = base + static_cast< size_t >(i) * _max_size;
        s.slot_iov.iov_len = _max_size;
    }
    // Ring not initialized here — __ensure_ring() does it on first launch.
}

Raid1ResyncTask::~Raid1ResyncTask() noexcept {
    // Fallback drain: resync_drain() in run_queue_loop should have already transitioned to IDLE.
    // This path fires in tests or abnormal teardown where no queue thread called drain().
    drain();
    if (_own_ring_initialized) io_uring_queue_exit(&_own_ring);
    free(_slot_buf_base);
    _slot_buf_base = nullptr;
}

template < typename StateHandler >
[[gnu::noinline]] bool Raid1ResyncTask::__transition_to(resync_state initial, resync_state target,
                                                        StateHandler&& handler) noexcept {
    auto cur_state = initial;
    while (!__cas_state(cur_state, target)) {
        auto [next_state, action] = handler(cur_state);

        switch (action) {
        case transition_action::RETRY: // LCOV_EXCL_LINE
            cur_state = next_state;    // LCOV_EXCL_LINE
            break;                     // LCOV_EXCL_LINE
        case transition_action::RETRY_WITH_SLEEP:
            cur_state = next_state;
            std::this_thread::sleep_for(k_state_spin_time);
            break;
        case transition_action::SUCCESS:
            return true;
        case transition_action::EARLY_EXIT:
            return false;
        }
    }
    return true; // CAS succeeded
}

bool Raid1ResyncTask::__ensure_ring() noexcept {
    if (_resync_queue) return true;
    io_uring_params p{};
    p.flags = IORING_SETUP_COOP_TASKRUN | IORING_SETUP_SINGLE_ISSUER;
    if (io_uring_queue_init_params(k_resync_ring_depth, &_own_ring, &p) != 0) { // LCOV_EXCL_START
        RLOGE("Resync io_uring ring init failed ({}); RAID1 resync will not run until next launch", strerror(errno))
        return false;
    } // LCOV_EXCL_STOP
    _own_queue.ring_ptr = &_own_ring;
    _own_queue.q_depth = k_resync_ring_depth;
    _resync_queue = &_own_queue;
    _own_ring_initialized = true;
    return true;
}

bool Raid1ResyncTask::has_in_flight() const noexcept {
    for (auto const& s : _slots)
        if (s.task.has_value() && !s.task->done()) return true;
    return false;
}

void Raid1ResyncTask::launch(std::string const& str_uuid, std::shared_ptr< MirrorDevice > clean_mirror,
                             std::shared_ptr< MirrorDevice > dirty_mirror, std::function< void() >&& complete) {
    if (_slots.empty()) { // LCOV_EXCL_START
        RLOGE("Resync slots not initialized; resync unavailable for [uuid:{}]", str_uuid)
        return;
    } // LCOV_EXCL_STOP

    // If already running, don't re-launch.
    auto state = __load_state();
    if (state == resync_state::ACTIVE) {
        RLOGD("Resync Task aborted for [uuid:{}] - already running", str_uuid)
        return;
    }
    // STOPPING is transient — spin briefly until the previous session drains.
    while (state == resync_state::STOPPING) {
        std::this_thread::sleep_for(k_state_spin_time);
        state = __load_state();
    }

    if (!__ensure_ring()) {
        RLOGE("Resync ring unavailable; cannot perform resync for [uuid:{}]", str_uuid) // LCOV_EXCL_LINE
        return;                                                                         // LCOV_EXCL_LINE
    }

    // Initialise session fields BEFORE the CAS. The acq_rel CAS releases these writes so
    // that any tick() that loads ACTIVE with acquire also sees the completed field writes.
    // This prevents a TSAN data race that would occur if we CASed first and then wrote:
    // tick() could load ACTIVE between the CAS and the field writes.
    _clean_mirror = std::move(clean_mirror);
    _dirty_mirror = std::move(dirty_mirror);
    _complete_cb = std::move(complete);
    _session_uuid = str_uuid;
    _resync_skip_from = 0;
    _consecutive_unavail = 0;
    _cancel_submitted = false;
    _peak_in_flight.store(0, std::memory_order_relaxed);
    _nr_pages = _dirty_bitmap->dirty_pages();
    _initial_resync_size = _dirty_bitmap->dirty_data_est();
    _resync_start = std::chrono::steady_clock::now();
    _next_probe_time = std::chrono::steady_clock::time_point{};

    auto idle = resync_state::IDLE;
    if (!__cas_state(idle, resync_state::ACTIVE)) return; // LCOV_EXCL_LINE -- lost CAS race; harmless

    RLOGD("Resync Task created for [uuid:{}]", str_uuid)

    if (_metrics) { // GCOVR_EXCL_BR_LINE
        // LCOV_EXCL_START -- UblkRaidMetrics requires prometheus registry; not constructible in unit tests
        auto const active_count = s_active_resyncs.fetch_add(1, std::memory_order_relaxed) + 1;
        _metrics->record_resync_start();
        _metrics->record_active_resyncs(active_count);
    } // LCOV_EXCL_STOP
}

void Raid1ResyncTask::clean_region(uint64_t addr, uint32_t len, MirrorDevice& clean_mirror) {
    auto const pg_size = _dirty_bitmap->page_size();
    auto iov = iovec{.iov_base = nullptr, .iov_len = pg_size};

    auto const end = addr + len;
    auto cur_off = addr;
    while (end > cur_off) {
        auto [page, pg_offset, sz] = _dirty_bitmap->clean_region(cur_off, end - cur_off);
        cur_off += sz;
        if (!page) continue;
        iov.iov_base = page;

        auto const page_addr = (pg_size * pg_offset) + pg_size;

        // These don't actually need to succeed; this page will remain dirty and loaded the next time
        // we use this bitmap (extra copies for this page).
        if (auto res = clean_mirror.disk->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, page_addr); !res) {
            RLOGW("Failed to clear bitmap page to: {}", *clean_mirror.disk)
        }
    }
}

void Raid1ResyncTask::__process_cqe(io_uring_cqe* cqe) noexcept {
    auto const ud = cqe->user_data;
    auto const res = cqe->res;                       // ud and res must both be read before cqe_seen —
    io_uring_cqe_seen(_resync_queue->ring_ptr, cqe); // after this, the kernel may reuse the CQE buffer
    if (!(ud & k_target_bit)) return;
    auto* state = reinterpret_cast< cqe_state* >(ud & ~k_target_bit);
    if (!state) return;
    state->_result = res;
    state->_result_ready = true;
    if (auto h = std::exchange(state->_waiter, {})) h.resume(); // exchange zeroes handle before resume
}

bool Raid1ResyncTask::__phase2_conflict(uint64_t lba, uint32_t len, uint64_t gen_before) const noexcept {
    return _region_tracker.overlaps(lba, len) || _region_tracker.completed_since(lba, len, gen_before);
}

// Merged per-slot coroutine: READ → Phase-2-check (synchronous) → WRITE → async bitmap flush.
// Called once per slot per chunk; the drain loop polls done() and resets the slot when complete.
// -EAGAIN: ring was full when async_iov tried to submit — no SQE sent, no data moved; caller
//   should retry this chunk on the next sweep.
// -ECANCELED: stop() submitted IORING_ASYNC_CANCEL_ANY; return immediately without marking
//   mirrors unavailable (cancellation is not a device fault).
disk_task< int > Raid1ResyncTask::__resync_slot_coro(ResyncSlot& slot, MirrorDevice& clean,
                                                     MirrorDevice& dirty) noexcept {
    // --- READ from clean mirror ---
    slot.fake_iod.op_flags = UBLK_IO_OP_READ;
    slot.io._pool.clear();
    DLOGT("READ {} : [lba:{:#0x}|len:{:#0x}]", *clean.disk, slot.lba + _offset, slot.len)
    auto const rres =
        co_await clean.disk->async_iov(_resync_queue, &slot.fake_data, &slot.slot_iov, 1, slot.lba + _offset);
    if (rres == -ECANCELED) co_return rres;
    if (rres == -EAGAIN) co_return rres; // LCOV_EXCL_LINE -- ring-full is not triggered in tests
    if (rres != static_cast< int >(slot.len)) {
        RLOGE("Resync async read failed: {} [lba:{:#0x} len:{} got:{}]", rres < 0 ? strerror(-rres) : "short read",
              slot.lba, slot.len, rres)
        clean.unavail.test_and_set(std::memory_order_release);
        co_return -EIO;
    }

    // --- Phase 2 conflict check: runs synchronously between READ CQE and WRITE SQE submission ---
    if (__phase2_conflict(slot.lba, slot.len, slot.gen_before)) co_return 0;

    // --- WRITE to dirty mirror ---
    slot.fake_iod.op_flags = UBLK_IO_OP_WRITE;
    slot.io._pool.clear();
    DLOGT("WRITE {} : [lba:{:#0x}|len:{:#0x}]", *dirty.disk, slot.lba + _offset, slot.len)
    auto const wres =
        co_await dirty.disk->async_iov(_resync_queue, &slot.fake_data, &slot.slot_iov, 1, slot.lba + _offset);
    if (wres == -ECANCELED) co_return wres;
    if (wres == -EAGAIN) co_return wres; // LCOV_EXCL_LINE -- ring-full is not triggered in tests
    if (wres != static_cast< int >(slot.len)) {
        RLOGE("Resync async write failed: {} [lba:{:#0x} len:{} got:{}]", wres < 0 ? strerror(-wres) : "short write",
              slot.lba, slot.len, wres)
        dirty.unavail.test_and_set(std::memory_order_release);
        co_return -EIO;
    }

    // --- Async bitmap flush: update in-memory state then persist each newly-zeroed bitmap page ---
    // Non-fatal: if a flush fails the page stays dirty and the chunk will be re-copied on the
    // next launch. Uses the same fake_data/slot_iov plumbing as the data I/O above.
    auto const pg_size = _dirty_bitmap->page_size();
    for (auto cur_off = slot.lba; cur_off < slot.lba + slot.len;) {
        auto [page, pg_offset, sz] = _dirty_bitmap->clean_region(cur_off, slot.lba + slot.len - cur_off);
        cur_off += sz;
        if (!page) continue;
        iovec bm_iov{page, pg_size};
        slot.fake_iod.op_flags = UBLK_IO_OP_WRITE;
        slot.io._pool.clear();
        auto const bres =
            co_await clean.disk->async_iov(_resync_queue, &slot.fake_data, &bm_iov, 1, pg_size * (pg_offset + 1));
        if (bres == -ECANCELED) break; // stop() in progress; bitmap stays dirty, no error
        if (bres < 0) RLOGW("Bitmap page flush to {}: {}", *clean.disk, strerror(-bres))
    }

    if (_metrics) { _metrics->record_resync_progress(slot.len); } // GCOVR_EXCL_BR_LINE
    co_return wres;
}

void Raid1ResyncTask::__finish_session(resync_state final_state) noexcept {
    if (final_state == resync_state::STOPPING) {
        RLOGI("Resync Task Stopped for [uuid:{}] to: {}", _session_uuid, *_dirty_mirror->disk)
    } else {
        if (0 == _dirty_bitmap->dirty_pages()) {
            RLOGD("Resync Task Finished for [uuid:{}] to: {}", _session_uuid, *_dirty_mirror->disk)
            if (_complete_cb) _complete_cb();
        }
    }

    if (_metrics) { // GCOVR_EXCL_BR_LINE
        // LCOV_EXCL_START -- UblkRaidMetrics requires prometheus registry; not constructible in unit tests
        auto const final_count = s_active_resyncs.fetch_sub(1, std::memory_order_relaxed) - 1;
        auto const resync_end = std::chrono::steady_clock::now();
        auto const duration_seconds =
            std::chrono::duration_cast< std::chrono::seconds >(resync_end - _resync_start).count();
        if (duration_seconds > 0) { _metrics->record_resync_complete(duration_seconds); }
        _metrics->record_last_resync_size(_initial_resync_size);
        _metrics->record_active_resyncs(final_count);
    } // LCOV_EXCL_STOP

    _clean_mirror.reset();
    _dirty_mirror.reset();
    _complete_cb = nullptr;

    for (auto s = final_state; !__cas_state(s, resync_state::IDLE) && s == final_state;)
        std::this_thread::yield();
}

void Raid1ResyncTask::__drain_stopping() noexcept {
    if (!_own_ring_initialized) {
        __finish_session(resync_state::STOPPING);
        return;
    }
    auto* ring = _resync_queue->ring_ptr;

    // Submit cancel-all exactly once so the kernel delivers -ECANCELED CQEs promptly.
    if (!_cancel_submitted && has_in_flight()) {
        if (auto* sqe = io_uring_get_sqe(ring)) {
            io_uring_prep_cancel(sqe, static_cast< void* >(nullptr), IORING_ASYNC_CANCEL_ANY);
            sqe->user_data = 0; // prevent __process_cqe from dispatching the cancel CQE
            if (io_uring_submit(ring) >= 0) _cancel_submitted = true;
            // on submit failure, retry next sweep
        } else {
            RLOGW("Resync STOPPING: SQ full; cancel-all deferred")
        }
    }

    // Drain with watchdog deadline.
    auto const drain_deadline = std::chrono::steady_clock::now() + k_watchdog_timeout;
    while (has_in_flight()) {
        if (std::chrono::steady_clock::now() >= drain_deadline) {
            RLOGE("Resync STOPPING drain watchdog fired; forcibly clearing in-flight slots")
            // Clear cqe_state pools so __process_cqe cannot resume stale coroutine handles
            // if a CQE arrives after the slot frames are destroyed.
            for (auto& slot : _slots)
                slot.io._pool.clear();
            // Tear down the ring: guarantees no further CQEs can be delivered.
            io_uring_queue_exit(&_own_ring);
            _own_ring_initialized = false;
            _resync_queue = nullptr;
            // Safe to destroy frames now: ring is gone, no _waiter references live.
            for (auto& slot : _slots)
                slot.task.reset();
            break;
        }
        {
            io_uring_cqe* cqe{};
            auto ts = k_resync_tick;
            io_uring_submit_and_wait_timeout(ring, &cqe, 1, &ts, nullptr);
            drain_cqes();
        }
        for (auto& slot : _slots) {
            if (slot.task && slot.task->done()) slot.task.reset();
        }
    }
    __finish_session(resync_state::STOPPING);
}

// Non-blocking single-sweep drain for tick()'s STOPPING path. Submits cancel-all once,
// flushes COOP_TASKRUN task_work via submit_and_wait_timeout(zero), peeks CQEs, reaps
// done slots. Calls __finish_session only when all slots are free; otherwise returns so
// the queue thread can service other I/O before the next tick() call.
void Raid1ResyncTask::__drain_stopping_nonblocking() noexcept {
    if (!_own_ring_initialized) {
        __finish_session(resync_state::STOPPING);
        return;
    }
    auto* ring = _resync_queue->ring_ptr;

    if (!_cancel_submitted && has_in_flight()) {
        if (auto* sqe = io_uring_get_sqe(ring)) {
            io_uring_prep_cancel(sqe, static_cast< void* >(nullptr), IORING_ASYNC_CANCEL_ANY);
            sqe->user_data = 0;
            if (io_uring_submit(ring) >= 0) _cancel_submitted = true;
        } else {
            RLOGW("Resync STOPPING: SQ full; cancel-all deferred")
        }
    }

    // Single non-blocking sweep: flush task_work and process any available CQEs.
    {
        struct __kernel_timespec zero_ts{0, 0};
        io_uring_cqe* cqe{};
        io_uring_submit_and_wait_timeout(ring, &cqe, 0, &zero_ts, nullptr);
        drain_cqes();
    }
    for (auto& slot : _slots) {
        if (slot.task && slot.task->done()) slot.task.reset();
    }

    if (!has_in_flight()) __finish_session(resync_state::STOPPING);
    // else: tick() will be called again; another sweep will follow
}

// One non-blocking resync sweep. Called by queue thread 0 after every CQE batch.
// ACTIVE path: fill available slots, submit SQEs, peek CQEs, reap done slots. When the bitmap
// is clean, transitions ACTIVE→IDLE. STOPPING path: delegates to __drain_stopping().
void Raid1ResyncTask::tick() noexcept {
    auto cur_state = __load_state();
    if (cur_state == resync_state::IDLE) return;
    if (!_own_ring_initialized) return;
    auto* ring = _resync_queue->ring_ptr;

    if (cur_state == resync_state::STOPPING) {
        __drain_stopping_nonblocking();
        return;
    }

    // --- ACTIVE path ---

    // Handle dirty-mirror unavailability: probe at most once per avail_delay interval.
    if (_dirty_mirror->unavail.test(std::memory_order_acquire)) {
        auto const now = std::chrono::steady_clock::now();
        if (now >= _next_probe_time) {
            static auto const unavail_delay = std::chrono::seconds(SISL_OPTIONS["avail_delay"].as< uint32_t >());
            if (++_consecutive_unavail % 10 == 0) {
                RLOGW("Resync blocked: dirty mirror unreachable for ~{}s [{}]",
                      _consecutive_unavail * SISL_OPTIONS["avail_delay"].as< uint32_t >(), *_dirty_mirror->disk)
            }
            probe_mirror(*_dirty_mirror, _offset);
            _next_probe_time = now + unavail_delay;
            _nr_pages = _dirty_bitmap->dirty_pages();
            if (_metrics) _metrics->record_dirty_pages(_nr_pages); // GCOVR_EXCL_BR_LINE
        }
        _yield_count.fetch_add(1, std::memory_order_release);
        return;
    }
    _consecutive_unavail = 0;

    if (_nr_pages == 0) {
        __finish_session(resync_state::ACTIVE);
        return;
    }

    // Submit loop: fill available slots with merged async copy+bitmap coroutines.
    ResyncCursor cursor{*_dirty_bitmap, _resync_skip_from};
    for (auto& slot : _slots) {
        if (slot.task && !slot.task->done()) continue; // in-flight
        if (slot.task) slot.task.reset();              // eagerly reap completed slot
        if (cursor.sz == 0) break;

        auto const iov_len = cursor.chunk_len(_max_size);
        auto const gen_before = _region_tracker.snapshot_gen();

        // Phase 1: skip if an in-flight write overlaps this chunk.
        if (_region_tracker.overlaps(cursor.lba, iov_len)) {
            if (cursor.skip(iov_len, *_dirty_bitmap)) break;
            continue;
        }

        // Skip chunks already covered by another in-flight slot.
        {
            bool already_in_flight = false;
            auto const chunk_end = cursor.lba + iov_len;
            for (auto const& s : _slots) {
                if (!s.task || s.task->done()) continue;
                if (cursor.lba < s.lba + s.len && chunk_end > s.lba) {
                    already_in_flight = true;
                    break;
                }
            }
            if (already_in_flight) {
                cursor.skip_inflight(iov_len, *_dirty_bitmap);
                continue;
            }
        }

        slot.gen_before = gen_before;
        slot.lba = cursor.lba;
        slot.len = iov_len;
        slot.slot_iov.iov_len = iov_len;
        slot.task.emplace(std::move(__resync_slot_coro(slot, *_clean_mirror, *_dirty_mirror)).start());

        cursor.advance(iov_len, *_dirty_bitmap);
    }
    _resync_skip_from = cursor.skip_from;

    // Track peak concurrency after slot filling (atomic — readable from test threads).
    {
        uint32_t cur_in_flight = 0;
        for (auto const& slot : _slots)
            if (slot.task && !slot.task->done()) ++cur_in_flight;
        auto peak = _peak_in_flight.load(std::memory_order_relaxed);
        while (cur_in_flight > peak &&
               !_peak_in_flight.compare_exchange_weak(peak, cur_in_flight, std::memory_order_relaxed))
            ;
    }

    // Submit pending SQEs and flush COOP_TASKRUN task_work so CQEs are visible to
    // the subsequent drain_cqes() peek. io_uring_submit_and_wait_timeout with nr_wait=0
    // and timeout={0,0} returns immediately but triggers task_work delivery — unlike
    // io_uring_peek_cqe which does not flush task_work under IORING_SETUP_COOP_TASKRUN.
    {
        struct __kernel_timespec zero_ts{0, 0};
        io_uring_cqe* cqe{};
        int const submit_res = io_uring_submit_and_wait_timeout(ring, &cqe, 0, &zero_ts, nullptr);
        if (submit_res < 0) { // LCOV_EXCL_START
            RLOGE("Resync io_uring_submit_and_wait_timeout failed: {}; stopping", strerror(-submit_res))
            auto active = resync_state::ACTIVE;
            __cas_state(active, resync_state::STOPPING);
            __drain_stopping_nonblocking();
            return;
        } // LCOV_EXCL_STOP
        drain_cqes();
    }

    // Reap: reset slots whose full coroutine sequence has completed.
    for (auto& slot : _slots) {
        if (slot.task && slot.task->done()) slot.task.reset();
    }

    // Increment sweep counter (test observability) and update page count.
    __yield();
    _nr_pages = _dirty_bitmap->dirty_pages();
    if (_metrics) _metrics->record_dirty_pages(_nr_pages); // GCOVR_EXCL_BR_LINE
}

// Synchronous drain: if ACTIVE, transitions to STOPPING first; then runs __drain_stopping().
// Called from run_queue_loop's exit path (queue thread 0) and the destructor fallback.
void Raid1ResyncTask::drain() noexcept {
    auto state = __load_state();
    if (state == resync_state::ACTIVE) {
        __cas_state(state, resync_state::STOPPING);
        state = __load_state();
    }
    if (state != resync_state::STOPPING) return;
    __drain_stopping();
}

void Raid1ResyncTask::stop() noexcept {
    __transition_to(resync_state::ACTIVE, resync_state::STOPPING, [](resync_state state) -> transition_result {
        switch (state) {
        case resync_state::IDLE:
        case resync_state::STOPPING:
            return {state, transition_action::SUCCESS};
        case resync_state::ACTIVE:
            return {state, transition_action::RETRY_WITH_SLEEP};
        }
        std::unreachable();
    });
    // The drain and STOPPING→IDLE transition happen in tick() (called by queue thread 0) or in
    // drain() (called from run_queue_loop's exit path or the destructor fallback).
}

// Increments _yield_count so tests can observe sweep completion.
// The queue thread's submit_and_wait_timeout provides natural I/O backpressure between ticks.
resync_state Raid1ResyncTask::__yield() noexcept {
    _yield_count.fetch_add(1, std::memory_order_release);
    return __load_state();
}

// Probes mirror reachability with a single synchronous page read.
// Returns true  — device is reachable; unavail flag has been cleared.
// Returns false — device failed the probe; unavail flag has been set.
bool Raid1ResyncTask::probe_mirror(MirrorDevice& mirror, uint64_t reserved_size) noexcept {
    alignas(k_page_size) uint8_t probe_buf[k_page_size];
    auto iov = iovec{.iov_base = probe_buf, .iov_len = k_page_size};
    if (auto res = mirror.disk->sync_iov(UBLK_IO_OP_READ, &iov, 1, reserved_size); res) {
        mirror.unavail.clear(std::memory_order_release);
        return true;
    }
    mirror.unavail.test_and_set(std::memory_order_release);
    return false;
}

// Processes all pending CQEs from the resync ring, delivering each to its waiting coroutine.
void Raid1ResyncTask::drain_cqes() noexcept {
    if (!_resync_queue) return;
    io_uring_cqe* cqe = nullptr;
    while (io_uring_peek_cqe(_resync_queue->ring_ptr, &cqe) == 0 && cqe)
        __process_cqe(cqe);
}

} // namespace ublkpp::raid1
