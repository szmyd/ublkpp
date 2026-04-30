#include "async_raid1_common.hpp"

// Read failover: primary device fails → failover to backup.
// Initial submit registers 1 CqeState (primary only). After injecting -EIO,
// RAID1 starts the failover task (registers a second CqeState) and suspends.
// Second inject delivers success to backup → completion returned.
TEST_F(AsyncRaid1Fixture, ReadFailoverToBackup) {
    std::thread([this] {
        // Expect both disks to be called: primary attempt + failover attempt.
        EXPECT_CALL(*disk_a, async_iov(_, _, _, _, _, _)).Times(1);
        EXPECT_CALL(*disk_b, async_iov(_, _, _, _, _, _)).Times(1);

        auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u); // only primary task started

        // Primary fails → RAID1 starts failover task (not done yet)
        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
        // Backup succeeds → RAID1 completes
        auto completions = mock->inject_cqe(0, 4 * Ki);
        ASSERT_EQ(completions.size(), 1u);
        EXPECT_EQ(completions[0].tag, 0);
        EXPECT_EQ(completions[0].result, 4 * Ki);
    }).join();
}

// Read failover: both devices fail → -EIO returned.
TEST_F(AsyncRaid1Fixture, ReadBothDevicesFail) {
    std::thread([this] {
        auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        // Primary fails → failover task started
        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
        // Backup also fails → -EIO
        auto completions = mock->inject_cqe(0, -EIO);
        ASSERT_EQ(completions.size(), 1u);
        EXPECT_EQ(completions[0].result, -EIO);
    }).join();
}

// Write: active device fails; backup was started eagerly and succeeds.
// co_return backup_res (backup bytes returned).
TEST_F(AsyncRaid1Fixture, WriteActiveFailsBecomeDegraded) {
    EXPECT_CALL(*disk_a, async_iov(_, _, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, async_iov(_, _, _, _, _, _)).Times(1);

    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    // Active -EIO → RAID1 awaits already-started backup
    EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
    // Backup succeeds → co_return backup_res = 4Ki
    auto completions = mock->inject_cqe(0, 4 * Ki);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, 4 * Ki);
}

// Write: active succeeds but backup device fails → become degraded, co_return active_res.
TEST_F(AsyncRaid1Fixture, WriteReplicaFailsBecomeDegraded) {
    EXPECT_CALL(*disk_a, async_iov(_, _, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, async_iov(_, _, _, _, _, _)).Times(1);

    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    // Active succeeds → RAID1 awaits backup
    EXPECT_TRUE(mock->inject_cqe(0, 4 * Ki).empty());
    // Backup -EIO → become_degraded, co_return active_res = 4Ki
    auto completions = mock->inject_cqe(0, -EIO);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, 4 * Ki);
}

// Write: both devices fail → -EIO.
TEST_F(AsyncRaid1Fixture, WriteBothDevicesFail) {
    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
    auto completions = mock->inject_cqe(0, -EIO);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EIO);
}
