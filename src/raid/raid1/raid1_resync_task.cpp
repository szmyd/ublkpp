#include "raid1_resync_task.hpp"

#include <ublksrv.h>
#include <sisl/utility/thread_factory.hpp>

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
        _region_tracker(slot_count, chunk_size),
        _resync_task() {
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
    if (_resync_task.joinable()) _resync_task.join();
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
        if (s.phase != ResyncSlot::Phase::FREE) return true;
    return false;
}

void Raid1ResyncTask::_start(std::string str_uuid, std::shared_ptr< MirrorDevice >& clean_mirror,
                             std::shared_ptr< MirrorDevice >& dirty_mirror, std::function< void() >&& complete) {
    RLOGD("Resync Task created for [uuid:{}]", str_uuid)

    if (!__ensure_ring()) {
        RLOGE("Resync ring unavailable; cannot perform resync for [uuid:{}]", str_uuid)
        for (auto active = resync_state::ACTIVE; !__cas_state(active, resync_state::IDLE);)
            std::this_thread::yield();
        return;
    }

    // Wait to become Available & IDLE
    auto cur_state = resync_state::IDLE;
    while (dirty_mirror->unavail.test(std::memory_order_acquire) || !__cas_state(cur_state, resync_state::ACTIVE)) {
        // If we're stopped or another we should exit
        if (resync_state::STOPPING == cur_state) break;

        cur_state = resync_state::IDLE;
        if (dirty_mirror->unavail.test(std::memory_order_acquire)) {
            cur_state = __load_state();
            if (resync_state::STOPPING == cur_state) break;
            std::this_thread::sleep_for(std::chrono::seconds(SISL_OPTIONS["avail_delay"].as< uint32_t >()));
            probe_mirror(*dirty_mirror, _offset);
        } else // LCOV_EXCL_START -- CAS IDLE→ACTIVE race inside _start(); not deterministically triggerable
            std::this_thread::sleep_for(std::chrono::microseconds(SISL_OPTIONS["resync_delay"].as< uint32_t >()));
        // LCOV_EXCL_STOP
    }
    cur_state = __load_state();

    // We are now guaranteed to be the only active thread performing I/O on the device
    if (resync_state::STOPPING != cur_state) {
        if (_slots.empty()) { // LCOV_EXCL_START -- posix_memalign failure; not injectable in tests
            RLOGE("No resync slots; aborting resync for [uuid:{}]", str_uuid)
            for (auto active = resync_state::ACTIVE; !__cas_state(active, resync_state::IDLE);)
                std::this_thread::yield();
            return;
        } // LCOV_EXCL_STOP
        auto const initial_resync_size = _dirty_bitmap->dirty_data_est();

        auto const resync_start = std::chrono::steady_clock::now();
        // Record resync start - increment global and per-device counters
        if (_metrics) { // GCOVR_EXCL_BR_LINE -- UblkRaidMetrics requires prometheus registry; not constructible in unit
                        // tests
            // LCOV_EXCL_START
            auto const active_count = s_active_resyncs.fetch_add(1, std::memory_order_relaxed) + 1;
            _metrics->record_resync_start();
            _metrics->record_active_resyncs(active_count);
        } // LCOV_EXCL_STOP

        cur_state = _run_resync_loop(clean_mirror, dirty_mirror);

        if (_metrics) { // GCOVR_EXCL_BR_LINE
            // LCOV_EXCL_START -- UblkRaidMetrics requires prometheus registry; not constructible in unit tests
            auto const final_count = s_active_resyncs.fetch_sub(1, std::memory_order_relaxed) - 1;
            auto const resync_end = std::chrono::steady_clock::now();
            auto const duration_seconds =
                std::chrono::duration_cast< std::chrono::seconds >(resync_end - resync_start).count();
            if (duration_seconds > 0) { _metrics->record_resync_complete(duration_seconds); }
            _metrics->record_last_resync_size(initial_resync_size);
            _metrics->record_active_resyncs(final_count);
        } // LCOV_EXCL_STOP
    }

    // If stopped, end now.
    if (resync_state::STOPPING == cur_state) {
        RLOGI("Resync Task Stopped for [uuid:{}] to: {}", str_uuid, *dirty_mirror->disk)
        for (auto stopping = resync_state::STOPPING;
             !__cas_state(stopping, resync_state::IDLE) && stopping == resync_state::STOPPING;)
            std::this_thread::yield();
        return;
    }

    // Otherwise we _should_ be active (not paused or idle), if the bitmap is clean call complete
    DEBUG_ASSERT_EQ(resync_state::ACTIVE, cur_state, "Resync stopped in unexpected state");
    // I/O may have been interrupted, if not check the bitmap and mark us as _clean_
    if (0 == _dirty_bitmap->dirty_pages()) complete();
    RLOGD("Resync Task Finished for [uuid:{}] to: {}", str_uuid, *dirty_mirror->disk)

    // Open up I/O Again
    for (auto active = resync_state::ACTIVE;
         !__cas_state(active, resync_state::IDLE) && active == resync_state::ACTIVE;)
        std::this_thread::yield();
}

void Raid1ResyncTask::launch(std::string const& str_uuid, std::shared_ptr< MirrorDevice > clean_mirror,
                             std::shared_ptr< MirrorDevice > dirty_mirror, std::function< void() >&& complete) {
    auto lg = std::scoped_lock< std::mutex >(_launch_lock);

    // First we must be IDLE. ACTIVE means already running; STOPPING is transient (stop() will
    // drive it back to IDLE), so we spin until it clears.
    while (true) {
        auto state = __load_state();
        if (state == resync_state::ACTIVE) {
            RLOGD("Resync Task aborted for [uuid:{}] - already running", str_uuid)
            return;
        }
        if (state == resync_state::IDLE) break;
        std::this_thread::sleep_for(k_state_spin_time);
    }

    // The previous resync thread may still be joinable even though state is IDLE — there is a
    // window between _start() setting state to IDLE and the thread actually returning.
    // Assigning to a joinable std::thread calls std::terminate(), so join here first.
    if (_resync_task.joinable()) _resync_task.join();

    _resync_task = sisl::named_thread(
        fmt::format("r_{}", str_uuid.substr(0, 13)),
        [this, uuid = str_uuid, clean = std::move(clean_mirror), dirty = std::move(dirty_mirror),
         compl_cb = std::move(complete)] mutable { _start(uuid, clean, dirty, std::move(compl_cb)); });
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

// Async copy loop: drives _resync_queue directly on the calling thread (the dedicated resync
// thread spawned by launch()). Each slot holds a hot_task<int> started from async_iov();
// io_uring_submit_and_wait_timeout submits pending SQEs and waits for a CQE (or 500 µs tick);
// drain_cqes() then delivers each CQE to the waiting disk_task coroutine via _waiter.resume(),
// making hot_task::done() true. The drain loop polls done() to advance READ→WRITE state machines.
//
// Per-region tracking (Phase 1 + Phase 2) is identical to the removed __run_coro path.
resync_state Raid1ResyncTask::_run_resync_loop(std::shared_ptr< MirrorDevice >& clean_mirror,
                                               std::shared_ptr< MirrorDevice >& dirty_mirror) noexcept {
    static auto const unavail_delay = std::chrono::seconds(SISL_OPTIONS["avail_delay"].as< uint32_t >());

    auto cur_state = resync_state::ACTIVE;
    uint32_t consecutive_unavail = 0;
    auto nr_pages = _dirty_bitmap->dirty_pages();
    if (_metrics) { _metrics->record_dirty_pages(nr_pages); } // GCOVR_EXCL_BR_LINE

    uint64_t resync_skip_from = 0;
    auto* ring = _resync_queue->ring_ptr;

    while (0 < nr_pages) {
        // Handle unavail: sleep in 500 µs ticks (non-blocking) with STOPPING check each tick.
        if (dirty_mirror->unavail.test(std::memory_order_acquire)) {
            if (++consecutive_unavail % 10 == 0) {
                RLOGW("Resync blocked: dirty mirror unreachable for ~{}s [{}]",
                      consecutive_unavail * SISL_OPTIONS["avail_delay"].as< uint32_t >(), *dirty_mirror->disk)
            }
            auto const unavail_end = std::chrono::steady_clock::now() + unavail_delay;
            while (std::chrono::steady_clock::now() < unavail_end) {
                cur_state = __load_state();
                if (cur_state == resync_state::STOPPING) break;
                io_uring_cqe* cqe{};
                auto ts = k_resync_tick;
                io_uring_submit_and_wait_timeout(ring, &cqe, 1, &ts, nullptr);
                drain_cqes();
            }
            _yield_count.fetch_add(1, std::memory_order_release);
            cur_state = __load_state();
            if (cur_state == resync_state::STOPPING) break;
            probe_mirror(*dirty_mirror, _offset);
            nr_pages = _dirty_bitmap->dirty_pages();
            if (_metrics) _metrics->record_dirty_pages(nr_pages); // GCOVR_EXCL_BR_LINE
            continue;
        }
        consecutive_unavail = 0;

        ResyncCursor cursor{*_dirty_bitmap, resync_skip_from};

        // Submit loop: fill free slots with async READ tasks. The natural throttle is slot
        // exhaustion (k_resync_slots = 8); no copies_left budget is needed here.
        if (cur_state != resync_state::STOPPING && !dirty_mirror->unavail.test(std::memory_order_acquire)) {
            for (auto& slot : _slots) {
                if (slot.phase != ResyncSlot::Phase::FREE) continue;
                if (cursor.sz == 0) break;

                auto const iov_len = cursor.chunk_len(_max_size);

                // Snapshot BEFORE Phase 1 so Phase 2 can detect writes completing during the READ.
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
                        if (s.phase == ResyncSlot::Phase::FREE) continue;
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
                slot.io._pool.clear();
                slot.fake_iod.op_flags = UBLK_IO_OP_READ;
                DLOGT("READ {} : [lba:{:#0x}|len:{:#0x}]", *clean_mirror->disk, cursor.lba + _offset, iov_len)

                auto t = clean_mirror->disk->async_iov(_resync_queue, &slot.fake_data, &slot.slot_iov, 1,
                                                       cursor.lba + _offset);
                slot.task.emplace(std::move(t).start());
                slot.phase = ResyncSlot::Phase::READ_PENDING;

                cursor.advance(iov_len, *_dirty_bitmap);
            }
        }

        resync_skip_from = cursor.skip_from;

        // Submit pending SQEs from async_iov().start() calls and wait for at least one CQE
        // (or 500 µs timeout). drain_cqes() delivers each completed CQE to the waiting
        // disk_task coroutine via cqe_state._waiter.resume(), making hot_task::done() true.
        {
            io_uring_cqe* cqe{};
            auto ts = k_resync_tick;
            io_uring_submit_and_wait_timeout(ring, &cqe, 1, &ts, nullptr);
            drain_cqes();
        }

        // Drain loop: advance slot state machines for tasks that have completed.
        for (auto& slot : _slots) {
            if (slot.phase == ResyncSlot::Phase::READ_PENDING && slot.task && slot.task->done()) {
                auto const read_res = slot.task->result();
                slot.task.reset();

                if (read_res == -EAGAIN) {
                    // Ring was full when async_iov tried to submit — no SQE was sent, no data
                    // read. Reset to FREE and let the submit loop retry this chunk next sweep.
                    slot.phase = ResyncSlot::Phase::FREE;
                    continue;
                }
                if (read_res != static_cast< int >(slot.len)) {
                    auto const msg = read_res < 0 ? strerror(-read_res) : "short read";
                    RLOGE("Resync async read failed: {} [lba:{:#0x} len:{} got:{}]", msg, slot.lba, slot.len, read_res)
                    clean_mirror->unavail.test_and_set(std::memory_order_release);
                    slot.phase = ResyncSlot::Phase::FREE;
                    continue;
                }

                if (__phase2_conflict(slot.lba, slot.len, slot.gen_before)) {
                    slot.phase = ResyncSlot::Phase::FREE;
                    continue;
                }

                slot.io._pool.clear();
                slot.fake_iod.op_flags = UBLK_IO_OP_WRITE;
                DLOGT("WRITE {} : [lba:{:#0x}|len:{:#0x}]", *dirty_mirror->disk, slot.lba + _offset, slot.len)

                auto write_t = dirty_mirror->disk->async_iov(_resync_queue, &slot.fake_data, &slot.slot_iov, 1,
                                                             slot.lba + _offset);
                slot.task.emplace(std::move(write_t).start());
                slot.phase = ResyncSlot::Phase::WRITE_PENDING;

            } else if (slot.phase == ResyncSlot::Phase::WRITE_PENDING && slot.task && slot.task->done()) {
                auto const write_res = slot.task->result();
                slot.task.reset();

                if (write_res == -EAGAIN) {
                    // Ring was full when async_iov tried to submit — transient, not a device fault.
                    // Reset to FREE; the submit loop will retry this chunk next sweep.
                    slot.phase = ResyncSlot::Phase::FREE;
                    continue;
                }
                if (write_res != static_cast< int >(slot.len)) {
                    auto const msg = write_res < 0 ? strerror(-write_res) : "short write";
                    RLOGE("Resync async write failed: {} [lba:{:#0x} len:{} got:{}]", msg, slot.lba, slot.len,
                          write_res)
                    dirty_mirror->unavail.test_and_set(std::memory_order_release);
                } else {
                    clean_region(slot.lba, slot.len, *clean_mirror);
                    if (_metrics) { _metrics->record_resync_progress(slot.len); } // GCOVR_EXCL_BR_LINE
                }
                slot.phase = ResyncSlot::Phase::FREE;
            }
        }

        // Check STOPPING. Drain in-flight slots by polling done() after each
        // io_uring_submit_and_wait_timeout + drain_cqes() tick.
        //
        // C2 invariant: no Phase 2 re-check here. Any host write that arrived after our
        // Phase 2 check also writes both mirrors, so the dirty mirror will eventually be
        // correct even if we don't re-copy. Skipping Phase 2 on STOPPING is intentional.
        //
        // E1 watchdog: arm a wall-clock deadline so a hung kernel cannot stall stop()
        // indefinitely.
        if (cur_state = __yield(); cur_state == resync_state::STOPPING) {
            auto const drain_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
            while (has_in_flight()) {
                if (std::chrono::steady_clock::now() >= drain_deadline) {
                    RLOGE("Resync STOPPING drain watchdog fired; forcibly freeing in-flight slots to unblock stop()")
                    for (auto& slot : _slots) {
                        if (slot.phase == ResyncSlot::Phase::FREE) continue;
                        slot.task.reset();
                        slot.phase = ResyncSlot::Phase::FREE;
                    }
                    break;
                }
                {
                    io_uring_cqe* cqe{};
                    auto ts = k_resync_tick;
                    io_uring_submit_and_wait_timeout(ring, &cqe, 1, &ts, nullptr);
                    drain_cqes();
                }
                for (auto& slot : _slots) {
                    if (slot.phase == ResyncSlot::Phase::FREE || !slot.task) continue;
                    if (!slot.task->done()) continue;
                    auto const res = slot.task->result();
                    slot.task.reset();
                    if (slot.phase == ResyncSlot::Phase::WRITE_PENDING) {
                        if (res == static_cast< int >(slot.len))
                            clean_region(slot.lba, slot.len, *clean_mirror);
                        else // E6: mirror write failed; mark unavail so next launch probes first
                            dirty_mirror->unavail.test_and_set(std::memory_order_release);
                    } else if (slot.phase == ResyncSlot::Phase::READ_PENDING && res != static_cast< int >(slot.len)) {
                        // E7: read from clean mirror failed; mark unavail
                        clean_mirror->unavail.test_and_set(std::memory_order_release);
                    }
                    slot.phase = ResyncSlot::Phase::FREE;
                }
            }
            break;
        }

        nr_pages = _dirty_bitmap->dirty_pages();
        if (_metrics) _metrics->record_dirty_pages(nr_pages); // GCOVR_EXCL_BR_LINE
    }

    return cur_state;
}

// Abort any on-going resync task by moving to STOPPING and waiting for completion.
void Raid1ResyncTask::stop() noexcept {
    auto lg = std::unique_lock< std::mutex >(_launch_lock);
    __transition_to(resync_state::ACTIVE, resync_state::STOPPING, [this](resync_state state) -> transition_result {
        switch (state) {
        case resync_state::IDLE: {
            // IDLE means the thread already finished naturally before stop() arrived.
            // If it's still joinable, spin until the thread exits and sets its state.
            if (_resync_task.joinable()) return {state, transition_action::RETRY_WITH_SLEEP};
            [[fallthrough]];
        }
        case resync_state::STOPPING:
            return {state, transition_action::SUCCESS};
        case resync_state::ACTIVE:
            return {state, transition_action::RETRY};
        }
        std::unreachable();
    });

    // Unlock before joining: _start() does not acquire _launch_lock, but holding it for the
    // full join duration (up to 30 s in the STOPPING drain) would block concurrent launch() callers.
    lg.unlock();
    if (_resync_task.joinable()) _resync_task.join();

    // If the resync completed naturally (ACTIVE→IDLE) before our CAS set STOPPING, the state
    // was never cleared from STOPPING. Reset it so launch() isn't stuck.
    for (auto stopping = resync_state::STOPPING;
         !__cas_state(stopping, resync_state::IDLE) && stopping == resync_state::STOPPING;)
        std::this_thread::yield();
}

// Increments _yield_count so tests can observe sweep completion and returns the current state.
// No sleep — the io_uring submit_and_wait_timeout in the async copy loop provides natural I/O
// backpressure.
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
