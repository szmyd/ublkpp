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

// ── Public API ────────────────────────────────────────────────────────────────────────────────

void Raid1ResyncTask::launch(std::string const& str_uuid, std::shared_ptr< MirrorDevice > clean_mirror,
                             std::shared_ptr< MirrorDevice > dirty_mirror, std::function< void() >&& complete) {
    auto lg = std::scoped_lock< std::mutex >(_launch_lock);

    if (_resync_task.joinable()) {
        // Thread exists: if it's winding down after stop(), join and re-launch; if running, skip.
        if (!_stop.load(std::memory_order_acquire)) {
            RLOGD("Resync Task aborted for [uuid:{}] - already running", str_uuid)
            return;
        }
        _resync_task.join();
    }

    RLOGD("Resync Task created for [uuid:{}]", str_uuid)
    _stop.store(false, std::memory_order_relaxed);
    _resync_task = sisl::named_thread(
        fmt::format("r_{}", str_uuid.substr(0, 13)),
        [this, uuid = str_uuid, clean = std::move(clean_mirror), dirty = std::move(dirty_mirror),
         compl_cb = std::move(complete)] mutable { _run(std::move(clean), std::move(dirty), std::move(compl_cb)); });
}

void Raid1ResyncTask::stop() noexcept {
    _stop.store(true, std::memory_order_release);
    auto lg = std::scoped_lock< std::mutex >(_launch_lock);
    if (_resync_task.joinable()) _resync_task.join();
}

// ── Private helpers ───────────────────────────────────────────────────────────────────────────

bool Raid1ResyncTask::_sleep_check_stop(std::chrono::microseconds dur) noexcept {
    static constexpr auto k_tick = std::chrono::microseconds{500};
    auto const end = std::chrono::steady_clock::now() + dur;
    while (std::chrono::steady_clock::now() < end) {
        if (_stop.load(std::memory_order_acquire)) return false;
        std::this_thread::sleep_for(k_tick);
    }
    return !_stop.load(std::memory_order_acquire);
}

bool Raid1ResyncTask::_init_ring(ublk_disk& clean_disk, ublk_disk& dirty_disk) noexcept {
    // Probe both disks with a temporary ring to check prep_iov_sqe() support.
    // Priming a zero-vector readv/writev SQE is sufficient — nr=0 means no actual I/O;
    // the SQEs are never submitted (io_uring_queue_exit() discards them).
    io_uring probe_ring{};
    if (io_uring_queue_init(k_resync_ring_depth, &probe_ring, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN) !=
        0) [[unlikely]]
        return false;

    iovec probe_iov{};
    bool const clean_ok = clean_disk.prep_iov_sqe(&probe_ring, UBLK_IO_OP_READ, &probe_iov, 0, 0, 0);
    bool const dirty_ok = dirty_disk.prep_iov_sqe(&probe_ring, UBLK_IO_OP_WRITE, &probe_iov, 0, 0, 0);
    io_uring_queue_exit(&probe_ring);

    if (!clean_ok || !dirty_ok) return false;

    if (io_uring_queue_init(k_resync_ring_depth, &_ring, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN) != 0)
        [[unlikely]]
        return false;

    for (auto& slot : _slots) {
        if (auto err = ::posix_memalign(&slot.buf, _io_size, _max_size); err != 0 || !slot.buf) [[unlikely]] {
            // LCOV_EXCL_START
            for (auto& s : _slots) {
                free(s.buf);
                s.buf = nullptr;
            }
            io_uring_queue_exit(&_ring);
            return false;
            // LCOV_EXCL_STOP
        }
    }

    _ring_initialized = true;
    return true;
}

// ── Entry point called by the resync thread ───────────────────────────────────────────────────

void Raid1ResyncTask::_run(std::shared_ptr< MirrorDevice > clean, std::shared_ptr< MirrorDevice > dirty,
                           std::function< void() > complete) {
    auto const initial_resync_size = _dirty_bitmap->dirty_data_est();
    auto const resync_start = std::chrono::steady_clock::now();

    if (_metrics) { // GCOVR_EXCL_BR_LINE
        // LCOV_EXCL_START
        auto const active_count = s_active_resyncs.fetch_add(1, std::memory_order_relaxed) + 1;
        _metrics->record_resync_start();
        _metrics->record_active_resyncs(active_count);
    } // LCOV_EXCL_STOP

    bool const use_uring = _init_ring(*clean->disk, *dirty->disk);
    RLOGD("Resync Task starting [use_uring:{}]", use_uring)

    if (use_uring)
        _run_uring(*clean, *dirty);
    else
        _run_sync(*clean, *dirty);

    if (use_uring) _drain_and_exit();

    if (_metrics) { // GCOVR_EXCL_BR_LINE
        // LCOV_EXCL_START
        auto const final_count = s_active_resyncs.fetch_sub(1, std::memory_order_relaxed) - 1;
        auto const resync_end = std::chrono::steady_clock::now();
        auto const duration_seconds =
            std::chrono::duration_cast< std::chrono::seconds >(resync_end - resync_start).count();
        if (duration_seconds > 0) _metrics->record_resync_complete(duration_seconds);
        _metrics->record_last_resync_size(initial_resync_size);
        _metrics->record_active_resyncs(final_count);
    } // LCOV_EXCL_STOP

    if (_stop.load(std::memory_order_acquire)) {
        RLOGI("Resync Task Stopped to: {}", *dirty->disk)
        return;
    }

    if (0 == _dirty_bitmap->dirty_pages()) complete();
    RLOGD("Resync Task Finished to: {}", *dirty->disk)
}

// ── Async (io_uring) path ─────────────────────────────────────────────────────────────────────

// Fills free slots with READ SQEs starting from (cursor_lba, cursor_sz).
// cursor_lba is a monotonically advancing high-water mark within a single scan pass;
// it never wraps within _fill_slots — the outer loop resets it for the next pass.
// This prevents re-submitting READs for ranges already covered by WRITE_PENDING slots.
void Raid1ResyncTask::_fill_slots(MirrorDevice& clean, uint64_t& cursor_lba, uint32_t& cursor_sz,
                                  uint64_t& cursor_skip_from) noexcept {
    for (auto idx = 0u; idx < k_resync_slots && !_stop.load(std::memory_order_relaxed); ++idx) {
        auto& slot = _slots[idx];
        if (slot.phase != ResyncSlot::Phase::FREE) continue;

        // Advance cursor to the next dirty chunk without wrapping past the start.
        if (cursor_sz == 0) {
            uint64_t const from = cursor_skip_from > 0 ? cursor_skip_from : cursor_lba;
            cursor_skip_from = 0;
            auto [lba, sz] = _dirty_bitmap->next_dirty_after(from);
            if (sz == 0) return; // no dirty data past current position — scan pass complete
            cursor_lba = lba;
            cursor_sz = sz;
        }

        auto const iov_len = std::min(cursor_sz, _max_size);
        auto const gen_before = _region_tracker.snapshot_gen();

        // Phase 1: skip if a write is in-flight for this range.
        if (_region_tracker.overlaps(cursor_lba, iov_len)) {
            cursor_lba += iov_len;
            cursor_sz -= iov_len;
            if (cursor_sz == 0) cursor_skip_from = cursor_lba;
            continue;
        }

        slot.iov = {.iov_base = slot.buf, .iov_len = iov_len};
        if (!clean.disk->prep_iov_sqe(&_ring, UBLK_IO_OP_READ, &slot.iov, 1, cursor_lba + _offset, idx)) [[unlikely]]
            return; // ring full; process existing CQEs first

        slot.phase = ResyncSlot::Phase::READ_PENDING;
        slot.lba = cursor_lba;
        slot.len = iov_len;
        slot.gen_before = gen_before;
        ++_in_flight;

        cursor_lba += iov_len;
        cursor_sz -= iov_len;
    }
}

void Raid1ResyncTask::_process_cqes(MirrorDevice& clean, MirrorDevice& dirty) noexcept {
    io_uring_cqe* cqe{};
    unsigned head{};
    unsigned seen{0};

    io_uring_for_each_cqe(&_ring, head, cqe) {
        ++seen;
        auto const ud = io_uring_cqe_get_data64(cqe);
        if (ud == k_resync_cancel_tag) continue;

        auto const slot_idx = static_cast< uint32_t >(ud & 0xFFu);
        bool const is_write = (ud & k_resync_write_tag) != 0;
        auto& slot = _slots[slot_idx];

        if (!is_write) {
            // READ CQE
            if (cqe->res < 0) [[unlikely]] {
                RLOGE("Resync async READ failed at lba={:#x}: {}", slot.lba, strerror(-cqe->res))
                dirty.unavail.test_and_set(std::memory_order_acquire);
                slot.phase = ResyncSlot::Phase::FREE;
                --_in_flight;
                continue;
            }

            // Phase 2: skip the write if a concurrent user write overlaps this range.
            // Two cases: still in-flight (overlaps()) or completed during our READ (completed_since()).
            if (_region_tracker.overlaps(slot.lba, slot.len) ||
                _region_tracker.completed_since(slot.lba, slot.len, slot.gen_before)) {
                slot.phase = ResyncSlot::Phase::FREE;
                --_in_flight;
                continue;
            }

            if (!dirty.disk->prep_iov_sqe(&_ring, UBLK_IO_OP_WRITE, &slot.iov, 1, slot.lba + _offset,
                                          slot_idx | k_resync_write_tag)) [[unlikely]] {
                // LCOV_EXCL_START — ring should never be full here (depth ≥ 2×slots)
                RLOGW("Resync: WRITE SQE submit failed for lba={:#x} — keeping dirty", slot.lba)
                slot.phase = ResyncSlot::Phase::FREE;
                --_in_flight;
                continue;
                // LCOV_EXCL_STOP
            }
            slot.phase = ResyncSlot::Phase::WRITE_PENDING;
        } else {
            // WRITE CQE
            if (cqe->res < 0) [[unlikely]] {
                RLOGW("Resync async WRITE failed at lba={:#x}: {}", slot.lba, strerror(-cqe->res))
                dirty.unavail.test_and_set(std::memory_order_acquire);
            } else {
                clean_region(slot.lba, slot.len, clean);
                if (_metrics) { _metrics->record_resync_progress(slot.len); } // GCOVR_EXCL_BR_LINE
            }
            slot.phase = ResyncSlot::Phase::FREE;
            --_in_flight;
        }
    }

    if (seen) io_uring_cq_advance(&_ring, seen);
}

void Raid1ResyncTask::_drain_and_exit() noexcept {
    if (_in_flight > 0) {
        if (auto* sqe = io_uring_get_sqe(&_ring)) {
            io_uring_prep_cancel(sqe, nullptr, IORING_ASYNC_CANCEL_ANY);
            io_uring_sqe_set_data64(sqe, k_resync_cancel_tag);
            io_uring_submit(&_ring);
        }
        // Drain all remaining CQEs. Each slot contributes exactly 1 to _in_flight;
        // the first CQE for it (READ -ECANCELED or WRITE completion/cancel) frees it.
        while (_in_flight > 0) {
            io_uring_cqe* cqe{};
            io_uring_wait_cqe(&_ring, &cqe);
            auto const ud = io_uring_cqe_get_data64(cqe);
            if (ud != k_resync_cancel_tag) {
                auto const slot_idx = static_cast< uint32_t >(ud & 0xFFu);
                auto& slot = _slots[slot_idx];
                if (slot.phase != ResyncSlot::Phase::FREE) {
                    slot.phase = ResyncSlot::Phase::FREE;
                    --_in_flight;
                }
            }
            io_uring_cqe_seen(&_ring, cqe);
        }
    }

    if (_ring_initialized) {
        io_uring_queue_exit(&_ring);
        _ring_initialized = false;
    }
    for (auto& slot : _slots) {
        free(slot.buf);
        slot.buf = nullptr;
    }
}

void Raid1ResyncTask::_run_uring(MirrorDevice& clean, MirrorDevice& dirty) {
    static auto const unavail_delay = std::chrono::seconds(SISL_OPTIONS["avail_delay"].as< uint32_t >());
    static auto const avail_delay = std::chrono::microseconds(SISL_OPTIONS["resync_delay"].as< uint32_t >());

    uint64_t cursor_lba{0}, cursor_skip_from{0};
    uint32_t cursor_sz{0};
    uint32_t consecutive_unavail{0};

    while (!_stop.load(std::memory_order_acquire)) {
        if (dirty.unavail.test(std::memory_order_acquire)) {
            if (++consecutive_unavail % 10 == 0)
                RLOGW("Resync blocked: dirty mirror unreachable for ~{}s [{}]",
                      consecutive_unavail * SISL_OPTIONS["avail_delay"].as< uint32_t >(), *dirty.disk)
            if (!_sleep_check_stop(unavail_delay)) break;
            probe_mirror(dirty, _offset);
            continue;
        }
        consecutive_unavail = 0;

        _fill_slots(clean, cursor_lba, cursor_sz, cursor_skip_from);

        if (_in_flight > 0) {
            __kernel_timespec ts{0, 500'000}; // 500µs
            io_uring_cqe* dummy{};
            io_uring_submit_and_wait_timeout(&_ring, &dummy, 1, &ts, nullptr);
            _process_cqes(clean, dirty);
        } else {
            // No slots in flight: either the scan pass is complete or all dirty ranges are
            // conflicting with in-flight writes. Reset the cursor for the next pass and yield.
            auto const nr_pages = _dirty_bitmap->dirty_pages();
            if (_metrics) { _metrics->record_dirty_pages(nr_pages); } // GCOVR_EXCL_BR_LINE
            if (0 == nr_pages) break;                                 // bitmap clean — session complete

            cursor_lba = 0;
            cursor_sz = 0;
            cursor_skip_from = 0;
            _yield_count.fetch_add(1, std::memory_order_relaxed);
            if (!_sleep_check_stop(avail_delay)) break;
        }
    }
}

// ── Synchronous (sync_iov) fallback path ─────────────────────────────────────────────────────
//
// Used when prep_iov_sqe() is unavailable (TestDisk, composite disks, overlayfs).
// Semantics are identical to the old __run() but without the SLEEPING/STOPPING state machine.
// stop() is detected via _stop.load() checks instead of CAS transitions.

void Raid1ResyncTask::_run_sync(MirrorDevice& clean, MirrorDevice& dirty) {
    static auto const unavail_delay = std::chrono::seconds(SISL_OPTIONS["avail_delay"].as< uint32_t >());
    static auto const avail_delay = std::chrono::microseconds(SISL_OPTIONS["resync_delay"].as< uint32_t >());

    auto nr_pages = _dirty_bitmap->dirty_pages();
    if (_metrics) { _metrics->record_dirty_pages(nr_pages); } // GCOVR_EXCL_BR_LINE

    auto iov = iovec{.iov_base = nullptr, .iov_len = 0};
    if (auto err = ::posix_memalign(&iov.iov_base, _io_size, _max_size); 0 != err || !iov.iov_base) [[unlikely]] {
        // LCOV_EXCL_START
        RLOGE("Could not allocate I/O buffer: {}", strerror(err))
        if (iov.iov_base) free(iov.iov_base);
        return;
        // LCOV_EXCL_STOP
    }

    uint32_t consecutive_unavail = 0;
    uint64_t resync_skip_from = 0;

    while (0 < nr_pages && !_stop.load(std::memory_order_acquire)) {
        if (dirty.unavail.test(std::memory_order_acquire)) {
            if (++consecutive_unavail % 10 == 0)
                RLOGW("Resync blocked: dirty mirror unreachable for ~{}s (probe reads failing) [{}]",
                      consecutive_unavail * SISL_OPTIONS["avail_delay"].as< uint32_t >(), *dirty.disk)
            if (!_sleep_check_stop(unavail_delay)) break;
            probe_mirror(dirty, _offset);
            nr_pages = _dirty_bitmap->dirty_pages();
            if (_metrics) _metrics->record_dirty_pages(nr_pages); // GCOVR_EXCL_BR_LINE
            continue;
        }
        consecutive_unavail = 0;

        // TODO Change this so it's easier to control with a future QoS algorithm
        auto copies_left = ((std::min(32U, SISL_OPTIONS["resync_level"].as< uint32_t >()) * 100U) / 32U) * 5U;

        auto [logical_off, sz] =
            resync_skip_from > 0 ? _dirty_bitmap->next_dirty_after(resync_skip_from) : _dirty_bitmap->next_dirty();
        if (0 == sz && resync_skip_from > 0) std::tie(logical_off, sz) = _dirty_bitmap->next_dirty();
        resync_skip_from = 0;

        bool any_copy = false;
        while (0 < sz && 0U < copies_left && !_stop.load(std::memory_order_acquire)) {
            auto const iov_len = std::min(sz, _max_size);
            auto const gen_before = _region_tracker.snapshot_gen();

            // Phase 1: pre-copy conflict check.
            if (_region_tracker.overlaps(logical_off, iov_len)) {
                sz -= iov_len;
                logical_off += iov_len;
                if (0 == sz) {
                    if (!any_copy) {
                        resync_skip_from = logical_off;
                        break;
                    }
                    std::tie(logical_off, sz) = _dirty_bitmap->next_dirty();
                    any_copy = false;
                }
                continue;
            }

            iov.iov_len = iov_len;
            auto res = clean.disk->sync_iov(UBLK_IO_OP_READ, &iov, 1, logical_off + _offset);
            if (res) {
                res = dirty.disk->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, logical_off + _offset);
                if (res) {
                    // Phase 2: post-copy conflict check.
                    if (!_region_tracker.overlaps(logical_off, iov_len) &&
                        !_region_tracker.completed_since(logical_off, iov_len, gen_before)) {
                        clean_region(logical_off, iov_len, clean);
                        if (_metrics) { _metrics->record_resync_progress(iov.iov_len); } // GCOVR_EXCL_BR_LINE
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
                    dirty.unavail.test_and_set(std::memory_order_acquire);
                    break;
                }
            } else {
                RLOGE("Resync: read from clean disk failed at lba={:#x}: {}", logical_off, res.error().message())
                break;
            }
        }

        _yield_count.fetch_add(1, std::memory_order_relaxed);
        if (!_sleep_check_stop(dirty.unavail.test(std::memory_order_acquire) ? unavail_delay : avail_delay)) break;

        nr_pages = _dirty_bitmap->dirty_pages();
        if (_metrics) _metrics->record_dirty_pages(nr_pages); // GCOVR_EXCL_BR_LINE
    }

    free(iov.iov_base);
}

// ── Static helpers ────────────────────────────────────────────────────────────────────────────

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
