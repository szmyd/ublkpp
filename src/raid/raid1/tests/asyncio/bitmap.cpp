#include "async_raid1_common.hpp"

#include <chrono>
#include <thread>

using namespace std::chrono_literals;

// Poll until bytes_to_sync reaches 0 AND no device is SYNCING, or the timeout elapses.
// bytes_to_sync drains before the route is restored to EITHER, so checking both avoids
// a race where we observe bytes_to_sync==0 but device_b still shows SYNCING.
static void wait_for_resync(ublkpp::raid1::Raid1Disk* raid, int timeout_ms = 5000) {
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        auto const s = raid->replica_states();
        if (s.bytes_to_sync == 0 && s.device_a != ublkpp::raid1::replica_state::SYNCING &&
            s.device_b != ublkpp::raid1::replica_state::SYNCING)
            break;
        std::this_thread::sleep_for(10ms);
    }
}

// Degrade disk_b at a given write address and return the active result.
// Precondition: array is healthy; uses tags 0 (healthy write) or can be reused after prior complete.
static void degrade_via_backup_fail(ublkpp::MockUblksrv* mock, int tag, uint64_t start_sector, uint32_t nr_sectors) {
    auto res = mock->submit_io(tag, UBLK_IO_OP_WRITE, start_sector, nr_sectors, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);
    EXPECT_TRUE(mock->inject_cqe(tag, static_cast< int >(nr_sectors * 512)).empty()); // active ok
    auto comp = mock->inject_cqe(tag, -EIO);                                          // backup fails
    ASSERT_EQ(comp.size(), 1u);
    EXPECT_GT(comp[0].result, 0);
}

// When the active device fails a write, the dirty bitmap is updated and bytes_to_sync > 0.
// The failed device shows ERROR in replica_states().
TEST_F(AsyncRaid1Fixture, BitmapDirtyOnActiveWriteFailure) {
    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty()); // active fails
    auto comp = mock->inject_cqe(0, 4 * Ki);        // backup succeeds → degraded
    ASSERT_EQ(comp.size(), 1u);
    EXPECT_EQ(comp[0].result, 4 * Ki);

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::ERROR);
    EXPECT_GT(states.bytes_to_sync, 0u);
}

// When the backup device fails a write, the dirty bitmap is also updated.
// The failed backup device shows ERROR; bytes_to_sync > 0.
TEST_F(AsyncRaid1Fixture, BitmapDirtyOnBackupWriteFailure) {
    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    EXPECT_TRUE(mock->inject_cqe(0, 4 * Ki).empty()); // active succeeds
    auto comp = mock->inject_cqe(0, -EIO);            // backup fails → degraded
    ASSERT_EQ(comp.size(), 1u);
    EXPECT_EQ(comp[0].result, 4 * Ki);

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::ERROR);
    EXPECT_GT(states.bytes_to_sync, 0u);
}

// A write in degraded mode (backup unavail) still dirties the bitmap even though
// the backup was skipped. bytes_to_sync reflects the pending sync obligation.
TEST_F(AsyncRaid1Fixture, BitmapDirtyOnDegradedWrite) {
    // First write: fail active → backup succeeds → degraded (disk_a ERROR).
    {
        auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
        auto comp = mock->inject_cqe(0, 4 * Ki);
        ASSERT_EQ(comp.size(), 1u);
    }
    auto const bytes_after_first = raid->replica_states().bytes_to_sync;
    ASSERT_GT(bytes_after_first, 0u);

    // Second write in degraded mode: only backup (disk_b) active, disk_a skipped.
    // The SKIP path also calls dirty_region → bytes_to_sync increases.
    auto res = mock->submit_io(1, UBLK_IO_OP_WRITE, 32 * Ki / 512, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 1u); // only one cqe_state (no backup)

    auto comp = mock->inject_cqe(1, 4 * Ki);
    ASSERT_EQ(comp.size(), 1u);
    EXPECT_EQ(comp[0].result, 4 * Ki);

    EXPECT_GE(raid->replica_states().bytes_to_sync, bytes_after_first);
}

// A freshly-constructed array has no dirty regions.
TEST_F(AsyncRaid1Fixture, FreshArrayHasZeroBytesToSync) {
    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u);
}

// After a degrading write, enabling resync allows the background task to copy the dirty region
// from the clean mirror (disk_a) to the dirty mirror (disk_b) and return to a clean state.
// probe_mirror succeeds via the default ON_CALL (truthy) which clears disk_b's unavail flag.
TEST_F(AsyncRaid1Fixture, ResyncBecomesClean) {
    degrade_via_backup_fail(mock.get(), 0, 0, 32 * Ki / 512);
    ASSERT_GT(raid->replica_states().bytes_to_sync, 0u);

    raid->toggle_resync(true);
    wait_for_resync(raid.get());

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u);
}

// A single 32 KiB dirty region (one chunk) is cleared by the resync task.
TEST_F(AsyncRaid1Fixture, ResyncCleansSingleRegion) {
    constexpr uint64_t k_offset = 64 * Ki;
    degrade_via_backup_fail(mock.get(), 0, k_offset / 512, 32 * Ki / 512);
    EXPECT_EQ(raid->replica_states().bytes_to_sync, 32 * Ki);

    raid->toggle_resync(true);
    wait_for_resync(raid.get());

    EXPECT_EQ(raid->replica_states().bytes_to_sync, 0u);
    EXPECT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::CLEAN);
}

// Multiple dirty regions at non-adjacent offsets are all cleared by resync.
TEST_F(AsyncRaid1Fixture, ResyncCleansMultipleRegions) {
    // First write degrades the array.
    degrade_via_backup_fail(mock.get(), 0, 64 * Ki / 512, 32 * Ki / 512);
    // Two more degraded writes at different offsets dirty additional chunks.
    for (int tag = 1; tag <= 2; ++tag) {
        auto res = mock->submit_io(tag, UBLK_IO_OP_WRITE, (tag + 2) * 64 * Ki / 512, 32 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);
        auto comp = mock->inject_cqe(tag, 32 * Ki);
        ASSERT_EQ(comp.size(), 1u);
    }
    EXPECT_EQ(raid->replica_states().bytes_to_sync, 3 * 32 * Ki);

    raid->toggle_resync(true);
    wait_for_resync(raid.get());

    EXPECT_EQ(raid->replica_states().bytes_to_sync, 0u);
    EXPECT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::CLEAN);
}

// If the clean-mirror READ fails during resync, the dirty region cannot be copied and must stay
// marked dirty. The read failure covers data addresses only (>= reserved_size); probe_mirror on
// disk_b still succeeds via the default ON_CALL, clearing unavail.
TEST_F(AsyncRaid1Fixture, ResyncReadFailurePreservesDirty) {
    degrade_via_backup_fail(mock.get(), 0, 0, 32 * Ki / 512);

    // Resync reads in the thread path use sync_iov directly; fail all reads at data addresses.
    ON_CALL(*disk_a, sync_iov(UBLK_IO_OP_READ, _, _, testing::Ge((off_t)raid->reserved_size())))
        .WillByDefault([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    raid->toggle_resync(true);
    std::this_thread::sleep_for(100ms);

    // bytes_to_sync must be > 0 (region not cleared); device_b state is omitted because
    // avail_delay=0 lets probe_mirror clear unavail immediately after __copy_region re-sets it.
    EXPECT_GT(raid->replica_states().bytes_to_sync, 0u);
}

// If the dirty-mirror WRITE fails during resync, the dirty region must remain dirty.
// Writes to data addresses (>= reserved_size) fail; SB and bitmap-page writes are unaffected.
TEST_F(AsyncRaid1Fixture, ResyncWriteFailurePreservesDirty) {
    degrade_via_backup_fail(mock.get(), 0, 0, 32 * Ki / 512);

    // Resync writes in the thread path use sync_iov directly; fail all writes at data addresses.
    ON_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Ge((off_t)raid->reserved_size())))
        .WillByDefault([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    raid->toggle_resync(true);
    std::this_thread::sleep_for(100ms);

    // bytes_to_sync must be > 0 (region not cleared); device_b state is omitted because
    // avail_delay=0 lets probe_mirror clear unavail immediately after __copy_region re-sets it.
    EXPECT_GT(raid->replica_states().bytes_to_sync, 0u);
}

// With resync disabled (never enabled), dirty regions must persist indefinitely.
TEST_F(AsyncRaid1Fixture, ResyncStoppedPreservesDirty) {
    degrade_via_backup_fail(mock.get(), 0, 0, 32 * Ki / 512);
    EXPECT_GT(raid->replica_states().bytes_to_sync, 0u);
    EXPECT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::ERROR);
    // toggle_resync(true) is deliberately NOT called.
}

// A single large write (256 KiB = 8 × 32 KiB chunks) that degrades the array leaves
// all 8 chunks dirty. Resync copies each in sequence until bytes_to_sync returns to 0.
TEST_F(AsyncRaid1Fixture, ResyncCleansLargeRegion) {
    degrade_via_backup_fail(mock.get(), 0, 64 * Ki / 512, 256 * Ki / 512);
    EXPECT_EQ(raid->replica_states().bytes_to_sync, 256 * Ki);

    raid->toggle_resync(true);
    wait_for_resync(raid.get());

    EXPECT_EQ(raid->replica_states().bytes_to_sync, 0u);
    EXPECT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::CLEAN);
}

// After degradation disk_b has unavail=true. The resync task calls probe_mirror (sync_iov READ
// on disk_b at reserved_size) to test whether disk_b is reachable before attempting a full resync.
// The default ON_CALL returns truthy, which clears unavail and lets resync proceed.
TEST_F(AsyncRaid1Fixture, ResyncUnblocksAfterProbe) {
    degrade_via_backup_fail(mock.get(), 0, 0, 32 * Ki / 512);

    // Catch-all for SB/bitmap writes on disk_b during resync and shutdown; registered first so
    // it is tried last (LIFO) and does not shadow the probe-READ expectation below.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // Explicitly verify probe_mirror is called at least once.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_READ, _, _, (off_t)raid->reserved_size()))
        .Times(testing::AtLeast(1))
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov && iov->iov_base) memcpy(iov->iov_base, &async_raid1_superblock, ublkpp::raid1::k_page_size);
            return static_cast< int >(iov->iov_len);
        });

    raid->toggle_resync(true);
    wait_for_resync(raid.get());

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u);
}

// Calling toggle_resync(true) while a resync is already running is a no-op; the second launch()
// hits the EARLY_EXIT arm inside __transition_to and returns immediately.
// Exercises __transition_to handler lines (ACTIVE/SLEEPING/PAUSE → EARLY_EXIT) and the
// early-return log+return in launch().
TEST_F(AsyncRaid1Fixture, ResyncLaunchWhileRunningIsNoop) {
    degrade_via_backup_fail(mock.get(), 0, 0, 32 * Ki / 512);
    ASSERT_GT(raid->replica_states().bytes_to_sync, 0u);

    std::atomic_bool resync_started{false};
    // Resync reads in the thread path use sync_iov; slow reads to keep state ACTIVE long enough
    // for the extra toggle_resync(true) calls to hit the EARLY_EXIT arm.
    ON_CALL(*disk_a, sync_iov(UBLK_IO_OP_READ, _, _, testing::Ge((off_t)raid->reserved_size())))
        .WillByDefault([&resync_started](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            resync_started.store(true);
            std::this_thread::sleep_for(50ms);
            return static_cast< int >(iov->iov_len);
        });

    raid->toggle_resync(true); // launches resync: IDLE → ACTIVE

    // Wait until the resync task is definitely in ACTIVE state (reading disk_a).
    // Without this wait, extra toggle_resync(true) calls may see state=IDLE and join+relaunch
    // instead of hitting the EARLY_EXIT arm.
    auto const dl = std::chrono::steady_clock::now() + 5s;
    while (!resync_started.load() && std::chrono::steady_clock::now() < dl)
        std::this_thread::sleep_for(1ms);
    ASSERT_TRUE(resync_started.load());

    // Each call hits the ACTIVE → EARLY_EXIT arm inside __transition_to.
    for (int i = 0; i < 5; ++i)
        raid->toggle_resync(true);

    wait_for_resync(raid.get());
    EXPECT_EQ(raid->replica_states().bytes_to_sync, 0u);
    EXPECT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::CLEAN);
}

// Calling toggle_resync(false) while a resync is mid-I/O must terminate without deadlock.
// stop() sees state=ACTIVE and returns RETRY; once I/O finishes and resync completes naturally,
// stop() detects IDLE (not joinable) and returns.
TEST_F(AsyncRaid1Fixture, ResyncStopTerminatesWithoutDeadlock) {
    degrade_via_backup_fail(mock.get(), 0, 0, 32 * Ki / 512);
    for (int tag = 1; tag <= 2; ++tag) {
        auto res = mock->submit_io(tag, UBLK_IO_OP_WRITE, tag * 64 * Ki / 512, 32 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);
        auto comp = mock->inject_cqe(tag, 32 * Ki);
        ASSERT_EQ(comp.size(), 1u);
    }
    ASSERT_EQ(raid->replica_states().bytes_to_sync, 3 * 32 * Ki);

    std::atomic_bool resync_started{false};
    std::atomic_bool unblock_reads{false};
    // Resync reads in the thread path use sync_iov; block reads until unblock_reads is set.
    ON_CALL(*disk_a, sync_iov(UBLK_IO_OP_READ, _, _, testing::Ge((off_t)raid->reserved_size())))
        .WillByDefault([&](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            resync_started.store(true);
            while (!unblock_reads.load(std::memory_order_acquire))
                std::this_thread::sleep_for(1ms);
            return static_cast< int >(iov->iov_len);
        });

    raid->toggle_resync(true);

    auto const dl = std::chrono::steady_clock::now() + 5s;
    while (!resync_started.load() && std::chrono::steady_clock::now() < dl)
        std::this_thread::sleep_for(1ms);
    ASSERT_TRUE(resync_started.load());

    // stop() calls join() which blocks until the resync thread exits; run it on a background
    // thread and unblock reads from here to avoid deadlock.
    std::thread stopper([this]() { raid->toggle_resync(false); });

    // Give stop() time to enter its ACTIVE→RETRY_WITH_SLEEP spin, then release reads.
    std::this_thread::sleep_for(5ms);
    unblock_reads.store(true, std::memory_order_release);

    stopper.join(); // stop() must return; if it deadlocks the test hangs and fails
}

// An async write submitted while resync is running registers its LBA range in the region
// tracker. Resync skips the conflicting chunk until the write completes (dequeue_write).
// After the write CQE arrives, resync retries and clears all remaining dirty regions.
TEST_F(AsyncRaid1Fixture, ResyncBlockedByOutstandingWrites) {
    // Degrade and dirty 3 regions.
    degrade_via_backup_fail(mock.get(), 0, 0, 32 * Ki / 512);
    for (int tag = 1; tag <= 2; ++tag) {
        auto res = mock->submit_io(tag, UBLK_IO_OP_WRITE, tag * 64 * Ki / 512, 32 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);
        auto comp = mock->inject_cqe(tag, 32 * Ki);
        ASSERT_EQ(comp.size(), 1u);
    }
    ASSERT_EQ(raid->replica_states().bytes_to_sync, 3 * 32 * Ki);

    // Slow sync_iov reads on disk_a to make resync measurable (gives time to submit write).
    // Normal IO slots use inject_cqe and are not affected by this mock.
    std::atomic_bool resync_started{false};
    ON_CALL(*disk_a, sync_iov(UBLK_IO_OP_READ, _, _, testing::Ge((off_t)raid->reserved_size())))
        .WillByDefault([&resync_started](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            resync_started = true;
            std::this_thread::sleep_for(20ms);
            return static_cast< int >(iov->iov_len);
        });

    raid->toggle_resync(true);

    // Wait for resync to start its first READ.
    auto const dl = std::chrono::steady_clock::now() + 5s;
    while (!resync_started && std::chrono::steady_clock::now() < dl)
        std::this_thread::sleep_for(1ms);
    ASSERT_TRUE(resync_started);

    // Submit a write while resync is mid-READ. In degraded mode this yields 1 cqe_state;
    // if probe_mirror cleared unavail first it may be 2. Drain all but the last cqe_state.
    auto pending = mock->submit_io(10, UBLK_IO_OP_WRITE, 10 * 64 * Ki / 512, 32 * Ki / 512, nullptr);
    ASSERT_TRUE(pending);
    for (uint32_t i = 0; i + 1 < pending.value(); ++i)
        EXPECT_TRUE(mock->inject_cqe(10, 32 * Ki).empty());

    // Complete the final cqe_state → dequeue_write() → resync retries the conflicting chunk.
    auto comp = mock->inject_cqe(10, 32 * Ki);
    ASSERT_EQ(comp.size(), 1u);

    wait_for_resync(raid.get(), 10000);
    EXPECT_EQ(raid->replica_states().bytes_to_sync, 0u);
    EXPECT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::CLEAN);
}
