#include "async_raid1_common.hpp"

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
    EXPECT_EQ(res.value(), 1u); // only one CqeState (no backup)

    auto comp = mock->inject_cqe(1, 4 * Ki);
    ASSERT_EQ(comp.size(), 1u);
    EXPECT_EQ(comp[0].result, 4 * Ki);

    EXPECT_GE(raid->replica_states().bytes_to_sync, bytes_after_first);
}
