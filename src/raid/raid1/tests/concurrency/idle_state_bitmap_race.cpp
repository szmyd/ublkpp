#include "asyncio/async_raid1_common.hpp"

#include <atomic>
#include <chrono>
#include <thread>

using namespace std::chrono_literals;

// Local helper mirroring the one in tests/asyncio/bitmap.cpp -- degrades the array by
// failing the backup write of an initial 32 KiB chunk so the dirty bitmap reflects
// exactly that range. Fixture's toggle_resync(false) means __become_degraded does not
// spawn the resync task here; the test enables it explicitly below.
static void degrade_via_backup_fail(ublkpp::MockUblksrv* mock, int tag, uint64_t start_sector, uint32_t nr_sectors) {
    auto res = mock->submit_io(tag, UBLK_IO_OP_WRITE, start_sector, nr_sectors, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);
    EXPECT_TRUE(mock->inject_cqe(tag, static_cast< int >(nr_sectors * 512)).empty()); // active ok
    auto comp = mock->inject_cqe(tag, -EIO);                                          // backup fails -> degraded
    ASSERT_EQ(comp.size(), 1u);
    EXPECT_GT(comp[0].result, 0);
}

// Regression test: ResyncWriteGuard fails to pause the resync when the resync task
// is in IDLE state at the moment of enqueue_write(). __pause() handles IDLE with
// EARLY_EXIT, so the cnt is incremented but no state transition is established.
// The resync subsequently CAS-es IDLE->ACTIVE in _start() and proceeds to copy
// dirty regions, clearing the bitmap while a write is still in flight. The in-flight
// write may have updated disk_a but disk_b receives stale data from the resync's
// read of disk_a (taken before the write committed); the bitmap is then cleared by
// resync's clean_region. A subsequent read may round-robin to disk_b and return the
// stale value.
//
// The fix (#253) replaces the global PAUSE state with a per-region tracker. Resync's
// __copy_region checks the tracker before reading and after writing; if any in-flight
// write overlaps the chunk, clean_region is skipped and the bitmap is preserved.
//
// Test strategy:
//   1. Degrade the array so the dirty bitmap has exactly one chunk dirty and
//      disk_b->unavail = true. With the fixture's _resync_enabled=false default,
//      __become_degraded does NOT spawn the resync.
//   2. Submit a write to that dirty chunk before launching the resync. The write's
//      ResyncWriteGuard ctor runs while state == IDLE, so __pause() EARLY_EXITs.
//      The active write registers a cqe_state but no CQE is injected yet, so the
//      write coroutine remains suspended and the guard's dequeue is NOT yet called.
//   3. Enable resync. The resync thread probes disk_b (succeeds via fixture default),
//      CAS IDLE->ACTIVE, and iterates dirty regions.
//   4. Sleep long enough for resync to complete one __run pass (avail_delay=0 in
//      tests, so probe is instant; mocks return immediately).
//   5. Assert: while the write is in flight, the bitmap must NOT have been cleared.
//
// With the BUG (this commit): resync copies the region, clean_region clears the
// bitmap, __become_clean transitions the route to EITHER, and bytes_to_sync returns
// 0 (the EITHER branch in replica_states() forces it to 0). EXPECT_GE fails.
//
// With the FIX (region tracker): Phase 1 of __copy_region sees the tracked write's
// LBA overlap and skips both the copy and clean_region. The bitmap stays dirty, the
// route stays DEVA, and bytes_to_sync remains at the original chunk size.
TEST_F(AsyncRaid1Fixture, IdleStateEnqueueAllowsResyncToClearBitmap) {
    // Step 1: degrade the array. Bitmap dirty for [0, 32 KiB); disk_b->unavail = true.
    degrade_via_backup_fail(mock.get(), 0, 0, 32 * Ki / 512);
    auto const initial_bytes = raid->replica_states().bytes_to_sync;
    ASSERT_GT(initial_bytes, 0u);
    ASSERT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::ERROR);

    // Step 2: submit a write to the dirty chunk WHILE the resync task is still in
    // its initial IDLE state. The ResyncWriteGuard ctor's __pause() will hit the
    // IDLE -> EARLY_EXIT arm, leaving cnt=1 but no state transition. The active task
    // registers a cqe_state on disk_a; the coroutine suspends on co_await active_task
    // because we do not inject the CQE here.
    auto write_res = mock->submit_io(10, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(write_res);
    EXPECT_EQ(write_res.value(), 1u); // SKIP path (B.unavail=true) -> 1 cqe_state

    // Step 3: launch the resync task. State transitions IDLE -> ACTIVE inside _start()
    // (probe instant because avail_delay=0). __run then iterates dirty regions.
    raid->toggle_resync(true);

    // Step 4: wait long enough for the resync to do at least one __copy_region +
    // clean_region cycle. In mocks, sync_iov returns immediately, so this happens in
    // microseconds; 200ms is well above the upper bound for any retry/yield delay.
    std::this_thread::sleep_for(200ms);

    // Step 5: the invariant -- while a write is in flight, the bitmap for its region
    // MUST NOT have been cleared. With the BUG, bytes_to_sync drops to 0 (resync
    // cleaned the chunk AND __become_clean flipped the route to EITHER). With the
    // FIX, the region tracker prevents clean_region from running and the bitmap
    // stays dirty (bytes_to_sync >= initial_bytes).
    EXPECT_GE(raid->replica_states().bytes_to_sync, initial_bytes)
        << "Resync cleared the bitmap while a write was in flight -- "
           "stale read possible (IDLE-state __pause early-exit race)";

    // Cleanup: complete the write so the coroutine can resume and the guard's
    // destructor fires (dequeue_write). Then stop resync.
    auto comp = mock->inject_cqe(10, 4 * Ki);
    ASSERT_EQ(comp.size(), 1u);

    raid->toggle_resync(false);
}
