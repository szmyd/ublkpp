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
}

Raid1ResyncTask::~Raid1ResyncTask() noexcept {
    if (_resync_task.joinable()) _resync_task.join();
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

void Raid1ResyncTask::_start(std::string str_uuid, std::shared_ptr< MirrorDevice >& clean_mirror,
                             std::shared_ptr< MirrorDevice >& dirty_mirror, std::function< void() >&& complete) {
    RLOGD("Resync Task created for [uuid:{}]", str_uuid)
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
        auto const initial_resync_size = _dirty_bitmap->dirty_data_est();
        // Set ourselves up with a buffer to do all the read/write operations from
        // iov_len stays at _max_size for buffer registration; __run overwrites it per-chunk.
        auto iov = iovec{.iov_base = nullptr, .iov_len = _max_size};
        if (auto err = ::posix_memalign(&iov.iov_base, _io_size, _max_size); 0 != err || nullptr == iov.iov_base)
            [[unlikely]] { // LCOV_EXCL_START
            RLOGE("Could not allocate memory for I/O: {}", strerror(err))
            if (iov.iov_base) free(iov.iov_base);
            return;
        } // LCOV_EXCL_STOP

        // Dedicated io_uring ring for resync copies. Created per-launch so each resync
        // starts with a clean ring and resources are released immediately on completion.
        io_uring ring{};
        bool ring_valid = false;
        {
            io_uring_params params{};
            params.flags = IORING_SETUP_COOP_TASKRUN | IORING_SETUP_SINGLE_ISSUER;
            if (0 == io_uring_queue_init_params(k_resync_ring_depth, &ring, &params)) {
                // Register the full _max_size buffer once; io_uring pins pages at registration
                // time, eliminating per-I/O memory-pinning overhead (~2–10 µs/op). Each
                // individual copy uses only the first iov_len bytes of the registered region.
                if (0 == io_uring_register_buffers(&ring, &iov, 1)) {
                    ring_valid = true;
                } else {
                    RLOGW("Resync io_uring buffer registration failed ({}); falling back to sync I/O", strerror(errno))
                    io_uring_queue_exit(&ring);
                }
            } else {
                RLOGW("Resync io_uring ring init failed ({}); falling back to sync I/O", strerror(errno))
            }
        }

        // Extract backing fds for the io_uring path. Both must be ≥ 0; if either is -1
        // (e.g. composite disk or test mock), __run() falls back to sync_iov.
        auto const clean_fd = clean_mirror->disk->backend_fd();
        auto const dirty_fd = dirty_mirror->disk->backend_fd();

        auto const resync_start = std::chrono::steady_clock::now();
        // Record resync start - increment global and per-device counters
        // Capture the initial size of data to resync
        if (_metrics) { // GCOVR_EXCL_BR_LINE -- UblkRaidMetrics requires prometheus registry; not constructible in unit
                        // tests
            // LCOV_EXCL_START
            auto const active_count = s_active_resyncs.fetch_add(1, std::memory_order_relaxed) + 1;
            _metrics->record_resync_start();
            _metrics->record_active_resyncs(active_count);
        } // LCOV_EXCL_STOP

        cur_state = __run(clean_mirror, dirty_mirror, &iov, ring_valid ? &ring : nullptr, clean_fd, dirty_fd);

        if (ring_valid) {
            io_uring_unregister_buffers(&ring);
            io_uring_queue_exit(&ring);
        }
        free(iov.iov_base);

        if (_metrics) { // GCOVR_EXCL_BR_LINE
            // LCOV_EXCL_START -- UblkRaidMetrics requires prometheus registry; not constructible in unit tests
            auto const final_count = s_active_resyncs.fetch_sub(1, std::memory_order_relaxed) - 1;
            auto const resync_end = std::chrono::steady_clock::now();
            auto const duration_seconds =
                std::chrono::duration_cast< std::chrono::seconds >(resync_end - resync_start).count();
            if (duration_seconds > 0) { _metrics->record_resync_complete(duration_seconds); }
            // Record the size of data that was resynced (initial size before resync started)
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

    // The previous resync thread may still be joinable even though state is IDLE — there is a
    // window between _start() setting state to IDLE and the thread actually returning. Assigning
    // to a joinable std::thread calls std::terminate(), so join here first.
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

static inline io_result __copy_region(iovec* iovec, int nr_vecs, uint64_t addr, auto& src, auto& dest) {
    auto res = src.sync_iov(UBLK_IO_OP_READ, iovec, nr_vecs, addr);
    if (res) {
        if (res = dest.sync_iov(UBLK_IO_OP_WRITE, iovec, nr_vecs, addr); !res) {
            RLOGW("Could not write clean chunks of [sz:{}] [res:{}]", iovec_len(iovec, iovec + nr_vecs),
                  res.error().message())
        }
    } else {
        RLOGE("Could not read Data of [sz:{}] [res:{}]", iovec_len(iovec, iovec + nr_vecs), res.error().message())
    }
    return res;
}

io_result Raid1ResyncTask::__copy_region_async(io_uring& ring, int clean_fd, int dirty_fd, void* buf, uint32_t len,
                                               uint64_t addr) noexcept {
    // Submit a linked READ_FIXED → WRITE_FIXED pair. IOSQE_IO_LINK causes the kernel to
    // execute the write immediately after the read completes — one io_uring_enter() syscall,
    // no userspace round-trip between the two operations.
    auto* read_sqe = io_uring_get_sqe(&ring);
    auto* write_sqe = read_sqe ? io_uring_get_sqe(&ring) : nullptr;
    if (!read_sqe || !write_sqe) {
        RLOGE("Resync io_uring SQ ring full — ring depth {} too small", k_resync_ring_depth)
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }
    io_uring_prep_read_fixed(read_sqe, clean_fd, buf, len, static_cast< int64_t >(addr), 0 /*buf_index*/);
    read_sqe->flags |= IOSQE_IO_LINK;
    io_uring_prep_write_fixed(write_sqe, dirty_fd, buf, len, static_cast< int64_t >(addr), 0 /*buf_index*/);

    // Short timeout keeps stop() responsive: the resync loop wakes within k_stop_timeout_ns
    // after STOPPING is set, checks __yield(), and exits cleanly.
    constexpr __kernel_timespec k_stop_timeout{.tv_sec = 0, .tv_nsec = 500'000}; // 500 µs

    // Drain both CQEs. With linked SQEs the read CQE arrives before the write CQE.
    int read_res = 0, write_res = 0;
    for (int cqes_needed = 2; cqes_needed > 0;) {
        io_uring_cqe* cqe = nullptr;
        auto ts = k_stop_timeout;
        auto const r = io_uring_submit_and_wait_timeout(&ring, &cqe, 1, &ts, nullptr);
        if (r < 0 && r != -ETIME) {
            RLOGE("io_uring_submit_and_wait_timeout: {}", strerror(-r))
            // The SQEs may still be in-flight; peek_cqe misses CQEs that haven't landed yet.
            // Wait up to 1 s per remaining CQE so the ring is clean before we return.
            // If a CQE doesn't arrive within that window the ring is considered unrecoverable.
            constexpr __kernel_timespec k_drain_timeout{.tv_sec = 1, .tv_nsec = 0};
            for (int to_drain = cqes_needed; to_drain > 0;) {
                io_uring_cqe* s = nullptr;
                auto ts = k_drain_timeout;
                io_uring_submit_and_wait_timeout(&ring, &s, 1, &ts, nullptr);
                if (!s) break; // no CQE within 1 s: ring is in bad state
                io_uring_cqe_seen(&ring, s);
                --to_drain;
            }
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        if (cqe == nullptr) continue; // timeout — loop back and re-wait
        if (cqes_needed == 2)
            read_res = cqe->res;
        else
            write_res = cqe->res;
        io_uring_cqe_seen(&ring, cqe);
        --cqes_needed;
    }

    // -ECANCELED on the write means the kernel cancelled it because the linked read failed.
    if (read_res < 0) {
        RLOGE("Resync io_uring read failed: {}", strerror(-read_res))
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }
    if (read_res != static_cast< int >(len)) {
        RLOGE("Resync io_uring short read: {} of {} bytes", read_res, len)
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }
    if (write_res < 0) {
        RLOGE("Resync io_uring write failed: {}", strerror(-write_res))
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }
    if (write_res != static_cast< int >(len)) {
        RLOGE("Resync io_uring short write: {} of {} bytes", write_res, len)
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }
    return static_cast< size_t >(write_res);
}

resync_state Raid1ResyncTask::__run(auto& clean_mirror, auto& dirty_mirror, iovec* iov, io_uring* ring, int clean_fd,
                                    int dirty_fd) noexcept {
    static auto const unavail_delay = std::chrono::seconds(SISL_OPTIONS["avail_delay"].as< uint32_t >());
    static auto const avail_delay = std::chrono::microseconds(SISL_OPTIONS["resync_delay"].as< uint32_t >());

    auto cur_state = resync_state::ACTIVE;
    uint32_t consecutive_unavail = 0;

    auto nr_pages = _dirty_bitmap->dirty_pages();
    if (_metrics) { _metrics->record_dirty_pages(nr_pages); } // GCOVR_EXCL_BR_LINE

    // When a dirty run is entirely blocked by in-flight writes (!any_copy), skip past it on
    // the next sweep so higher-LBA dirty runs are not starved. Reset after use within each
    // sweep; set at end of inner loop to advance past a stuck run on the next sweep.
    uint64_t resync_skip_from = 0;

    while (0 < nr_pages) {
        // Skip copies entirely if the dirty mirror is known unavailable
        if (dirty_mirror->unavail.test(std::memory_order_acquire)) {
            if (++consecutive_unavail % 10 == 0) {
                RLOGW("Resync blocked: dirty mirror unreachable for ~{}s (probe reads failing) [{}]",
                      consecutive_unavail * SISL_OPTIONS["avail_delay"].as< uint32_t >(), *dirty_mirror->disk)
            }
            // Sleep unavail_delay, checking STOPPING each tick.
            // Always re-read state after the loop: if unavail_delay==0 (e.g. in tests)
            // the inner while never runs and the STOPPING check inside never fires.
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

        // TODO Change this so it's easier to control with a future QoS algorithm
        auto copies_left = ((std::min(32U, SISL_OPTIONS["resync_level"].as< uint32_t >()) * 100U) / 32U) * 5U;

        // Use the skip cursor if a fully-conflicting run was detected last sweep.
        auto [logical_off, sz] =
            resync_skip_from > 0 ? _dirty_bitmap->next_dirty_after(resync_skip_from) : _dirty_bitmap->next_dirty();
        if (0 == sz && resync_skip_from > 0) // nothing after cursor — wrap to beginning
            std::tie(logical_off, sz) = _dirty_bitmap->next_dirty();
        resync_skip_from = 0;

        // copies_left counts copy attempts (read+write to replica); only Phase 1 skips are free.
        // any_copy tracks whether this inner pass produced at least one copy; if a full
        // dirty run is exhausted via Phase-1 skips alone, we set resync_skip_from and break
        // to let __yield() fire, advancing past the stuck run on the next sweep.
        bool any_copy = false;
        while (0 < sz && 0U < copies_left) {
            auto const iov_len = std::min(sz, _max_size);

            // Capture generation before Phase 1 so Phase 2 can detect writes that complete
            // entirely between the two checks (slot freed before Phase 2 runs).
            auto const gen_before = _region_tracker.snapshot_gen();

            // Phase 1: pre-copy conflict check — skip this chunk if a write is in-flight
            // for this range. Advance within the dirty run so unrelated chunks still copy.
            if (_region_tracker.overlaps(logical_off, iov_len)) {
                sz -= iov_len;
                logical_off += iov_len;
                if (0 == sz) {
                    if (!any_copy) {
                        resync_skip_from = logical_off; // advance past conflicting run next sweep
                        break;
                    }
                    std::tie(logical_off, sz) = _dirty_bitmap->next_dirty();
                    any_copy = false;
                }
                continue;
            }

            iov->iov_len = iov_len;
            // Copy region from clean to dirty. Use the dedicated io_uring ring when backing
            // fds are available (FSDisk); fall back to sync_iov for composite disks and mocks.
            io_result res;
            if (ring && clean_fd >= 0 && dirty_fd >= 0) {
                res = __copy_region_async(*ring, clean_fd, dirty_fd, iov->iov_base, iov_len, logical_off + _offset);
            } else {
                res = __copy_region(iov, 1, logical_off + _offset, *clean_mirror->disk, *dirty_mirror->disk);
            }
            if (res) {
                // Phase 2: post-copy conflict check. Two cases require skipping clean_region:
                //   (a) overlaps() — write is still in-flight (single CAS slot still holds
                //       the packed value; k_free is not visible until untrack() completes).
                //   (b) completed_since() — write arrived AND fully completed during the READ
                //       window; slot freed before Phase 2 ran; shadow log catches this.
                if (!_region_tracker.overlaps(logical_off, iov_len) &&
                    !_region_tracker.completed_since(logical_off, iov_len, gen_before)) {
                    clean_region(logical_off, iov->iov_len, *clean_mirror);
                    if (_metrics) { _metrics->record_resync_progress(iov->iov_len); } // GCOVR_EXCL_BR_LINE
                }
                any_copy = true;
                --copies_left;
                sz -= iov_len;
                logical_off += iov_len;
                if (0 == sz) {
                    std::tie(logical_off, sz) = _dirty_bitmap->next_dirty();
                    any_copy = false;
                }
            } else {
                dirty_mirror->unavail.test_and_set(std::memory_order_acquire);
                break;
            }
        }

        // Check STOPPING inline. The io_uring submit_and_wait_timeout call in each
        // __copy_region_async already provides natural I/O backpressure; no sleep needed here.
        if (cur_state = __yield(); resync_state::STOPPING == cur_state) break;

        // Sweep and count dirty pages left
        nr_pages = _dirty_bitmap->dirty_pages();
        if (_metrics) _metrics->record_dirty_pages(nr_pages); // GCOVR_EXCL_BR_LINE
    }
    return cur_state;
}

// Abort any on-going resync task by moving to STOPPING and rejoin the thread.
// With SLEEPING removed, we CAS ACTIVE→STOPPING directly. The resync loop wakes
// from its io_uring submit_and_wait_timeout call within the configured timeout and
// observes STOPPING on the next __yield() call.
void Raid1ResyncTask::stop() noexcept {
    auto lg = std::scoped_lock< std::mutex >(_launch_lock);
    __transition_to(resync_state::ACTIVE, resync_state::STOPPING, [this](resync_state state) -> transition_result {
        switch (state) {
        case resync_state::IDLE: {
            if (_resync_task.joinable()) return {state, transition_action::RETRY_WITH_SLEEP};
            [[fallthrough]];
        }
        case resync_state::STOPPING:
            return {state, transition_action::SUCCESS};
        case resync_state::ACTIVE:
            // CAS(ACTIVE→STOPPING) succeeds on the very next attempt; no sleep needed.
            return {state, transition_action::RETRY};
        }
        std::unreachable();
    });
    if (_resync_task.joinable()) _resync_task.join();
    // If the thread finished naturally (ACTIVE→IDLE) before stop() CAS'd ACTIVE→STOPPING,
    // it returned without ever seeing STOPPING and never cleared it. _launch_lock is held
    // so no concurrent caller can observe this window; reset to IDLE so launch() isn't stuck.
    for (auto stopping = resync_state::STOPPING;
         !__cas_state(stopping, resync_state::IDLE) && stopping == resync_state::STOPPING;)
        std::this_thread::yield();
}

// Increments _yield_count so tests can observe sweep completion and returns the current state.
// No sleep — the io_uring submit_and_wait_timeout in the async copy loop provides natural I/O
// backpressure. On the sync fallback path (__copy_region) there is no inter-sweep delay; callers
// on that path may see higher CPU under fully-conflicted workloads, which is acceptable because
// the sync path is used only by composite disks and test mocks, not production FSDisk instances.
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
    mirror.unavail.test_and_set(std::memory_order_acquire);
    return false;
}

} // namespace ublkpp::raid1
