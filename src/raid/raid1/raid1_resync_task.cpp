#include "raid1_resync_task.hpp"

#include <ublksrv.h>
#include <sisl/utility/thread_factory.hpp>

#include "lib/logging.hpp"
#include "bitmap.hpp"
#include "raid1_impl.hpp"

namespace ublkpp::raid1 {

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
        s.fake_data.tag = static_cast< int >(i);
        s.fake_data.iod = &s.fake_iod;
        s.fake_data.private_data = &s.io;
        s.slot_iov.iov_base = base + static_cast< size_t >(i) * _max_size;
        s.slot_iov.iov_len = _max_size;
    }
    // Ring not initialized here — launch() (production) or __ensure_ring() (tests) does it.
}

Raid1ResyncTask::~Raid1ResyncTask() noexcept {
    if (_resync_dispatch) {
        // Coroutine path: stop() should have been called (and future waited) before we get here.
        // As a safety net, wait once more so we never destroy the resync ring while a coroutine
        // is still in flight.
        if (_done_future.valid()) _done_future.wait();
    } else {
        if (_resync_task.joinable()) _resync_task.join();
    }
    if (_resync_queue == &_own_queue) io_uring_queue_exit(&_own_ring);
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
    if (io_uring_queue_init_params(k_resync_ring_depth, &_own_ring, &p) != 0) {
        RLOGW("Resync io_uring ring init failed ({}); resync unavailable", strerror(errno))
        return false;
    }
    _own_queue.ring_ptr = &_own_ring;
    _own_queue.q_depth = k_resync_ring_depth;
    _resync_queue = &_own_queue;
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

        cur_state = __run(clean_mirror, dirty_mirror);

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
                             std::shared_ptr< MirrorDevice > dirty_mirror, std::function< void() >&& complete,
                             ublksrv_queue* resync_q, ResyncDispatcher* dispatch) {
    auto lg = std::scoped_lock< std::mutex >(_launch_lock);

    // First we must become IDLE
    auto transitioned =
        __transition_to(resync_state::IDLE, resync_state::IDLE, [](resync_state state) -> transition_result {
            switch (state) {
            case resync_state::IDLE: // LCOV_EXCL_LINE -- CAS(IDLE→IDLE) succeeds if state==IDLE; handler never called
                                     // with IDLE
                return {state, transition_action::SUCCESS}; // LCOV_EXCL_LINE
            case resync_state::ACTIVE:
                return {state, transition_action::EARLY_EXIT};
            case resync_state::STOPPING: // LCOV_EXCL_LINE -- transient; not deterministically catchable
                return {resync_state::IDLE, transition_action::RETRY_WITH_SLEEP}; // LCOV_EXCL_LINE
            }
            std::unreachable();
        });

    if (!transitioned) {
        RLOGD("Resync Task aborted for [uuid:{}] - already running", str_uuid)
        return;
    }

    // Update queue/dispatch only after confirming we will actually launch; avoids mutating
    // fields that an in-flight thread or coroutine may be reading on a concurrent early-exit.
    if (resync_q) _resync_queue = resync_q;
    _resync_dispatch = dispatch;

    if (dispatch) {
        // Coroutine dispatch path: transition to ACTIVE here (not inside __run_coro), reset the
        // done signal, then post the coroutine factory to run_resync_queue_loop.
        auto idle = resync_state::IDLE;
        const auto ok = __cas_state(idle, resync_state::ACTIVE);
        RELEASE_ASSERT(ok, "IDLE→ACTIVE CAS failed despite holding _launch_lock");
        _done_promise = std::promise< void >{};
        _done_future = _done_promise.get_future();
        dispatch->submit([this, uuid = str_uuid, clean = std::move(clean_mirror), dirty = std::move(dirty_mirror),
                          compl_cb = std::move(complete)]() mutable {
            return __run_coro(std::move(clean), std::move(dirty), std::move(compl_cb), std::move(uuid));
        });
    } else {
        // Thread fallback (standalone / test context): unchanged thread spawn path.
        // The previous resync thread may still be joinable even though state is IDLE — there is a
        // window between _start() setting state to IDLE and the thread actually returning.
        // Assigning to a joinable std::thread calls std::terminate(), so join here first.
        if (_resync_task.joinable()) _resync_task.join();

        _resync_task = sisl::named_thread(
            fmt::format("r_{}", str_uuid.substr(0, 13)),
            [this, uuid = str_uuid, clean = std::move(clean_mirror), dirty = std::move(dirty_mirror),
             compl_cb = std::move(complete)] mutable { _start(uuid, clean, dirty, std::move(compl_cb)); });
    }
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
    auto const res = cqe->res; // read before cqe_seen — the slot may be reused after
    io_uring_cqe_seen(_resync_queue->ring_ptr, cqe);
    if (!(ud & k_target_bit)) return;
    auto* state = reinterpret_cast< cqe_state* >(ud & ~k_target_bit);
    if (!state) return;
    state->_result = res;
    state->_result_ready = true;
    if (auto h = std::exchange(state->_waiter, {})) h.resume(); // exchange zeroes handle before resume
}

// Coroutine version of _start() + __run(). Spawned by run_resync_queue_loop into its
// exec::async_scope; the centralized loop drives the resync io_uring ring and resumes
// coroutines via cqe_state._waiter.resume(). This coroutine never calls
// io_uring_submit_and_wait_timeout directly — that is the loop's job.
//
// Unavail delay: instead of std::this_thread::sleep_for, we submit 500 µs io_uring timeout
// SQEs and co_await them so the resync loop thread can continue processing other coroutines.
// CQE dispatch for these timeouts uses the same cqe_state mechanism as regular I/O CQEs.
exec::task< void > Raid1ResyncTask::__run_coro(std::shared_ptr< MirrorDevice > clean_mirror,
                                               std::shared_ptr< MirrorDevice > dirty_mirror,
                                               std::function< void() > complete, std::string uuid) {
    RLOGD("Resync coroutine started for [uuid:{}]", uuid)

    static auto const unavail_delay = std::chrono::seconds(SISL_OPTIONS["avail_delay"].as< uint32_t >());

    // Helper: submit a 500 µs timeout SQE to the resync ring and co_await it.
    // Allows the resync loop to continue processing CQEs for other coroutines while we wait.
    // Falls back to std::this_thread::yield() if the SQ is temporarily full.
    // ts and tick live in sleep_tick's own coroutine frame, which is heap-pinned for the
    // duration of co_await sleep_tick(). Their addresses baked into the SQE user_data remain valid.
    auto sleep_tick = [this]() -> exec::task< void > {
        constexpr __kernel_timespec k_tick{.tv_sec = 0, .tv_nsec = 500'000};
        if (auto* sqe = next_sqe(_resync_queue)) {
            cqe_state tick{};
            auto ts = k_tick;
            io_uring_prep_timeout(sqe, &ts, 0, 0);
            io_uring_sqe_set_data64(sqe, reinterpret_cast< uint64_t >(&tick) | k_target_bit);
            co_await tick;
        } else {
            // SQ full: yield to let the caller drain CQEs and free SQ entries.
            std::this_thread::yield();
        }
    };

    // -- Metrics (same as _start()) --
    auto const initial_resync_size = _dirty_bitmap->dirty_data_est();
    auto const resync_start = std::chrono::steady_clock::now();
    if (_metrics) { // GCOVR_EXCL_BR_LINE
        // LCOV_EXCL_START
        auto const active_count = s_active_resyncs.fetch_add(1, std::memory_order_relaxed) + 1;
        _metrics->record_resync_start();
        _metrics->record_active_resyncs(active_count);
    } // LCOV_EXCL_STOP

    // -- Main loop --
    auto cur_state = resync_state::ACTIVE;
    uint32_t consecutive_unavail = 0;
    auto nr_pages = _dirty_bitmap->dirty_pages();
    if (_metrics) { _metrics->record_dirty_pages(nr_pages); } // GCOVR_EXCL_BR_LINE

    uint64_t resync_skip_from = 0;

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
                co_await sleep_tick();
            }
            _yield_count.fetch_add(1, std::memory_order_relaxed);
            cur_state = __load_state();
            if (cur_state == resync_state::STOPPING) break;
            probe_mirror(*dirty_mirror, _offset);
            nr_pages = _dirty_bitmap->dirty_pages();
            if (_metrics) _metrics->record_dirty_pages(nr_pages); // GCOVR_EXCL_BR_LINE
            continue;
        }
        consecutive_unavail = 0;

        auto copies_left = ((std::min(32U, SISL_OPTIONS["resync_level"].as< uint32_t >()) * 100U) / 32U) * 5U;

        auto [cursor_lba, cursor_sz] =
            resync_skip_from > 0 ? _dirty_bitmap->next_dirty_after(resync_skip_from) : _dirty_bitmap->next_dirty();
        if (0 == cursor_sz && resync_skip_from > 0) std::tie(cursor_lba, cursor_sz) = _dirty_bitmap->next_dirty();
        resync_skip_from = 0;

        bool any_copy = false;

        // Submit loop: push async READ tasks into free slots (identical logic to __run()).
        if (cur_state != resync_state::STOPPING && !dirty_mirror->unavail.test(std::memory_order_acquire)) {
            for (auto& slot : _slots) {
                if (copies_left == 0) break;
                if (slot.phase != ResyncSlot::Phase::FREE) continue;
                if (cursor_sz == 0) break;

                auto const iov_len = std::min(cursor_sz, _max_size);

                if (_region_tracker.overlaps(cursor_lba, iov_len)) {
                    cursor_sz -= iov_len;
                    cursor_lba += iov_len;
                    if (0 == cursor_sz) {
                        if (!any_copy) {
                            resync_skip_from = cursor_lba;
                            break;
                        }
                        std::tie(cursor_lba, cursor_sz) = _dirty_bitmap->next_dirty();
                        any_copy = false;
                    }
                    continue;
                }

                {
                    bool already_in_flight = false;
                    auto const chunk_end = cursor_lba + iov_len;
                    for (auto const& s : _slots) {
                        if (s.phase == ResyncSlot::Phase::FREE) continue;
                        if (cursor_lba < s.lba + s.len && chunk_end > s.lba) {
                            already_in_flight = true;
                            break;
                        }
                    }
                    if (already_in_flight) {
                        cursor_sz -= iov_len;
                        cursor_lba += iov_len;
                        if (0 == cursor_sz) {
                            std::tie(cursor_lba, cursor_sz) = _dirty_bitmap->next_dirty();
                            any_copy = false;
                        }
                        continue;
                    }
                }

                slot.gen_before = _region_tracker.snapshot_gen();
                slot.lba = cursor_lba;
                slot.len = iov_len;
                slot.slot_iov.iov_len = iov_len;
                slot.io._pool.clear();
                slot.fake_iod.op_flags = UBLK_IO_OP_READ;
                DLOGT("READ {} : [lba:{:#0x}|len:{:#0x}]", *clean_mirror->disk, cursor_lba + _offset, iov_len)

                auto t = clean_mirror->disk->async_iov(_resync_queue, &slot.fake_data, &slot.slot_iov, 1,
                                                       cursor_lba + _offset);
                slot.task.emplace(std::move(t).start());
                slot.phase = ResyncSlot::Phase::READ_PENDING;

                any_copy = true;
                --copies_left;
                cursor_sz -= iov_len;
                cursor_lba += iov_len;
                if (0 == cursor_sz) {
                    std::tie(cursor_lba, cursor_sz) = _dirty_bitmap->next_dirty();
                    any_copy = false;
                }
            }
        }

        // Instead of io_uring_submit_and_wait_timeout: co_await the first non-done in-flight slot.
        // The centralized run_resync_queue_loop handles CQE delivery and resumes slot.task's
        // disk_task coroutine via cqe_state._waiter.resume() → symmetric transfer back here.
        // Slots that complete "in background" (while we await another) have done()==true on the
        // next sweep; await_ready() short-circuits them without suspending.
        {
            bool needs_wait = false;
            for (auto const& s : _slots) {
                if (s.phase != ResyncSlot::Phase::FREE && s.task && !s.task->done()) {
                    needs_wait = true;
                    break;
                }
            }
            if (needs_wait) {
                for (auto& slot : _slots) {
                    if (slot.phase != ResyncSlot::Phase::FREE && slot.task && !slot.task->done()) {
                        co_await *slot.task; // suspends until this slot's CQE is dispatched
                        break;
                    }
                }
            }
        }

        // Drain loop: advance slot state machines for tasks that have completed (identical to __run()).
        for (auto& slot : _slots) {
            if (slot.phase == ResyncSlot::Phase::READ_PENDING && slot.task && slot.task->done()) {
                auto const read_res = slot.task->result();
                slot.task.reset();

                if (read_res < 0) {
                    RLOGE("Resync async read failed: {} [lba:{:#0x} len:{}]", strerror(-read_res), slot.lba, slot.len)
                    dirty_mirror->unavail.test_and_set(std::memory_order_release);
                    slot.phase = ResyncSlot::Phase::FREE;
                    continue;
                }

                if (_region_tracker.overlaps(slot.lba, slot.len) ||
                    _region_tracker.completed_since(slot.lba, slot.len, slot.gen_before)) {
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

                if (write_res < 0) {
                    RLOGE("Resync async write failed: {} [lba:{:#0x} len:{}]", strerror(-write_res), slot.lba, slot.len)
                    dirty_mirror->unavail.test_and_set(std::memory_order_release);
                } else {
                    clean_region(slot.lba, slot.len, *clean_mirror);
                    if (_metrics) { _metrics->record_resync_progress(slot.len); } // GCOVR_EXCL_BR_LINE
                }
                slot.phase = ResyncSlot::Phase::FREE;
            }
        }

        // Check STOPPING. Drain in-flight slots by co_await-ing each remaining task, so the
        // shared ring is clean for the next launch() (ring is owned by ublkpp_tgt_impl).
        if (cur_state = __yield(); cur_state == resync_state::STOPPING) {
            while (has_in_flight()) {
                for (auto& slot : _slots) {
                    if (slot.phase != ResyncSlot::Phase::FREE && slot.task) {
                        co_await *slot.task; // await_ready() fast-paths already-done tasks
                        auto const res = slot.task->result();
                        slot.task.reset();
                        // A WRITE that completed successfully must be marked clean so the bitmap
                        // reflects actual mirror state, even if the overall resync was stopped.
                        if (slot.phase == ResyncSlot::Phase::WRITE_PENDING && res >= 0)
                            clean_region(slot.lba, slot.len, *clean_mirror);
                        slot.phase = ResyncSlot::Phase::FREE;
                        break;
                    }
                }
            }
            break;
        }

        nr_pages = _dirty_bitmap->dirty_pages();
        if (_metrics) _metrics->record_dirty_pages(nr_pages); // GCOVR_EXCL_BR_LINE
    }

    // -- Metrics teardown (same as _start()) --
    if (_metrics) { // GCOVR_EXCL_BR_LINE
        // LCOV_EXCL_START
        auto const final_count = s_active_resyncs.fetch_sub(1, std::memory_order_relaxed) - 1;
        auto const resync_end = std::chrono::steady_clock::now();
        auto const duration_seconds =
            std::chrono::duration_cast< std::chrono::seconds >(resync_end - resync_start).count();
        if (duration_seconds > 0) { _metrics->record_resync_complete(duration_seconds); }
        _metrics->record_last_resync_size(initial_resync_size);
        _metrics->record_active_resyncs(final_count);
    } // LCOV_EXCL_STOP

    // -- State transition and completion callback --
    if (cur_state == resync_state::STOPPING) {
        RLOGI("Resync coroutine stopped for [uuid:{}] to: {}", uuid, *dirty_mirror->disk)
        for (auto s = resync_state::STOPPING; !__cas_state(s, resync_state::IDLE) && s == resync_state::STOPPING;) {}
    } else {
        DEBUG_ASSERT_EQ(resync_state::ACTIVE, cur_state, "Resync coroutine stopped in unexpected state");
        RLOGD("Resync coroutine finished for [uuid:{}] to: {}", uuid, *dirty_mirror->disk)
        for (auto s = resync_state::ACTIVE; !__cas_state(s, resync_state::IDLE) && s == resync_state::ACTIVE;) {}
    }

    // Signal stop() before invoking complete(): if complete() calls stop(), stop() calls
    // _done_future.wait() which must return immediately to avoid deadlock. State is already
    // IDLE above, so stop() will also find IDLE and skip the ACTIVE→STOPPING transition.
    _done_promise.set_value();
    if (cur_state != resync_state::STOPPING && 0 == _dirty_bitmap->dirty_pages()) complete();
}

// Thread-path copy loop: uses synchronous I/O (sync_iov) so tests that mock sync_iov
// work without a live io_uring ring. The coroutine path (__run_coro) uses async_iov
// driven by run_resync_queue_loop. Per-region tracking (Phase 1 + Phase 2) is identical.
resync_state Raid1ResyncTask::__run(auto& clean_mirror, auto& dirty_mirror) noexcept {
    static auto const unavail_delay = std::chrono::seconds(SISL_OPTIONS["avail_delay"].as< uint32_t >());
    static auto const avail_delay = std::chrono::microseconds(SISL_OPTIONS["resync_delay"].as< uint32_t >());

    auto cur_state = resync_state::ACTIVE;
    uint32_t consecutive_unavail = 0;

    auto nr_pages = _dirty_bitmap->dirty_pages();
    if (_metrics) { _metrics->record_dirty_pages(nr_pages); } // GCOVR_EXCL_BR_LINE

    uint64_t resync_skip_from = 0;
    auto& slot = _slots[0]; // one copy at a time; slot 0's buffer is the transfer buffer

    while (0 < nr_pages) {
        if (dirty_mirror->unavail.test(std::memory_order_acquire)) {
            if (++consecutive_unavail % 10 == 0) {
                RLOGW("Resync blocked: dirty mirror unreachable for ~{}s (probe reads failing) [{}]",
                      consecutive_unavail * SISL_OPTIONS["avail_delay"].as< uint32_t >(), *dirty_mirror->disk)
            }
            {
                auto const unavail_end = std::chrono::steady_clock::now() + unavail_delay;
                while (std::chrono::steady_clock::now() < unavail_end) {
                    if (resync_state::STOPPING == __load_state()) {
                        cur_state = resync_state::STOPPING;
                        break;
                    }
                    std::this_thread::sleep_for(avail_delay);
                }
                if (resync_state::STOPPING != cur_state) cur_state = __load_state();
            }
            _yield_count.fetch_add(1, std::memory_order_relaxed);
            if (resync_state::STOPPING == cur_state) break;
            probe_mirror(*dirty_mirror, _offset);
            nr_pages = _dirty_bitmap->dirty_pages();
            if (_metrics) _metrics->record_dirty_pages(nr_pages); // GCOVR_EXCL_BR_LINE
            continue;
        }
        consecutive_unavail = 0;

        auto copies_left = ((std::min(32U, SISL_OPTIONS["resync_level"].as< uint32_t >()) * 100U) / 32U) * 5U;

        auto [cursor_lba, cursor_sz] =
            resync_skip_from > 0 ? _dirty_bitmap->next_dirty_after(resync_skip_from) : _dirty_bitmap->next_dirty();
        if (0 == cursor_sz && resync_skip_from > 0) std::tie(cursor_lba, cursor_sz) = _dirty_bitmap->next_dirty();
        resync_skip_from = 0;

        bool any_copy = false;

        while (cursor_sz > 0 && copies_left > 0 && cur_state != resync_state::STOPPING &&
               !dirty_mirror->unavail.test(std::memory_order_acquire)) {
            auto const iov_len = std::min(cursor_sz, _max_size);

            // Phase 1: skip if an in-flight write overlaps this chunk.
            if (_region_tracker.overlaps(cursor_lba, iov_len)) {
                cursor_sz -= iov_len;
                cursor_lba += iov_len;
                if (0 == cursor_sz) {
                    if (!any_copy) {
                        resync_skip_from = cursor_lba;
                        break;
                    }
                    std::tie(cursor_lba, cursor_sz) = _dirty_bitmap->next_dirty();
                    any_copy = false;
                }
                continue;
            }

            auto const gen_before = _region_tracker.snapshot_gen();
            slot.slot_iov.iov_len = iov_len;

            DLOGT("READ {} : [lba:{:#0x}|len:{:#0x}]", *clean_mirror->disk, cursor_lba + _offset, iov_len)
            auto read_res = clean_mirror->disk->sync_iov(UBLK_IO_OP_READ, &slot.slot_iov, 1,
                                                         static_cast< off_t >(cursor_lba + _offset));
            if (!read_res) {
                RLOGE("Resync read failed: {} [lba:{:#0x} len:{}]", read_res.error().message(), cursor_lba, iov_len)
                dirty_mirror->unavail.test_and_set(std::memory_order_release);
                break;
            }

            // Phase 2: post-copy conflict check.
            if (_region_tracker.overlaps(cursor_lba, iov_len) ||
                _region_tracker.completed_since(cursor_lba, iov_len, gen_before)) {
                cursor_sz -= iov_len;
                cursor_lba += iov_len;
                any_copy = true;
                --copies_left;
                if (0 == cursor_sz) {
                    std::tie(cursor_lba, cursor_sz) = _dirty_bitmap->next_dirty();
                    any_copy = false;
                }
                continue;
            }

            DLOGT("WRITE {} : [lba:{:#0x}|len:{:#0x}]", *dirty_mirror->disk, cursor_lba + _offset, iov_len)
            auto write_res = dirty_mirror->disk->sync_iov(UBLK_IO_OP_WRITE, &slot.slot_iov, 1,
                                                          static_cast< off_t >(cursor_lba + _offset));
            if (!write_res) {
                RLOGE("Resync write failed: {} [lba:{:#0x} len:{}]", write_res.error().message(), cursor_lba, iov_len)
                dirty_mirror->unavail.test_and_set(std::memory_order_release);
            } else {
                clean_region(cursor_lba, iov_len, *clean_mirror);
                if (_metrics) { _metrics->record_resync_progress(iov_len); } // GCOVR_EXCL_BR_LINE
            }

            any_copy = true;
            --copies_left;
            cursor_sz -= iov_len;
            cursor_lba += iov_len;
            if (0 == cursor_sz) {
                std::tie(cursor_lba, cursor_sz) = _dirty_bitmap->next_dirty();
                any_copy = false;
            }
        }

        if (cur_state = __yield(); resync_state::STOPPING == cur_state) break;
        nr_pages = _dirty_bitmap->dirty_pages();
        if (_metrics) _metrics->record_dirty_pages(nr_pages); // GCOVR_EXCL_BR_LINE
    }
    return cur_state;
}

// Abort any on-going resync task by moving to STOPPING and waiting for completion.
// Thread path: join the resync thread. Coroutine path: wait on _done_future (the coroutine
// observes STOPPING on the next __yield() / tick, drains in-flight slots, and sets the promise).
void Raid1ResyncTask::stop() noexcept {
    auto lg = std::unique_lock< std::mutex >(_launch_lock);
    __transition_to(resync_state::ACTIVE, resync_state::STOPPING, [this](resync_state state) -> transition_result {
        switch (state) {
        case resync_state::IDLE: {
            // Dispatch path: IDLE means the coroutine already finished naturally
            // (ACTIVE→IDLE) before stop() arrived. Nothing to wait for.
            if (!_resync_dispatch && _resync_task.joinable()) return {state, transition_action::RETRY_WITH_SLEEP};
            [[fallthrough]];
        }
        case resync_state::STOPPING:
            return {state, transition_action::SUCCESS};
        case resync_state::ACTIVE:
            return {state, transition_action::RETRY};
        }
        std::unreachable();
    });

    if (_resync_dispatch) {
        // Coroutine path: release the lock before waiting so that if the complete() callback
        // calls stop() it can acquire _launch_lock without deadlocking. NOTE: complete() must
        // not itself call _done_future.wait() — concurrent waits on std::future are UB.
        lg.unlock();
        if (_done_future.valid()) _done_future.wait();
    } else {
        if (_resync_task.joinable()) _resync_task.join();
    }

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
    _yield_count.fetch_add(1, std::memory_order_relaxed);
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
// Called by the dispatch-path test's CQE drain thread instead of run_resync_queue_loop.
void Raid1ResyncTask::drain_cqes() noexcept {
    if (!_resync_queue) return;
    io_uring_cqe* cqe = nullptr;
    while (io_uring_peek_cqe(_resync_queue->ring_ptr, &cqe) == 0 && cqe)
        __process_cqe(cqe);
}

} // namespace ublkpp::raid1
