#include "async_raid1_common.hpp"

// Read failover: primary device fails → failover to backup.
// Initial submit registers 1 cqe_state (primary only). After injecting -EIO,
// RAID1 starts the failover task (registers a second cqe_state) and suspends.
// Second inject delivers success to backup → completion returned.
TEST_F(AsyncRaid1Fixture, ReadFailoverToBackup) {
    std::thread([this] {
        // Expect both disks to be called: primary attempt + failover attempt.
        EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
        EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);

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
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);

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
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);

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

// Write in degraded mode: sole surviving device also fails → -EAGAIN (transient; kernel can retry).
// Step 1: degrade the array by letting the backup (disk_b) write fail.
// Step 2: second write goes only to disk_a (now sole active); disk_a also fails → -EAGAIN.
TEST_F(AsyncRaid1Fixture, WriteFailsInDegradedMode) {
    // Step 1: degrade — backup fails, active result returned.
    {
        auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 2u);

        EXPECT_TRUE(mock->inject_cqe(0, 4 * Ki).empty()); // active succeeds
        auto comp = mock->inject_cqe(0, -EIO);            // backup fails → degrade
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_GT(comp[0].result, 0);
    }
    ASSERT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::ERROR);

    // Step 2: write in degraded mode; sole active (disk_a) fails → -EAGAIN.
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(0);
    auto res = mock->submit_io(1, UBLK_IO_OP_WRITE, 8 * Ki / 512, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 1u); // degraded: only one device active

    auto comp = mock->inject_cqe(1, -EIO);
    ASSERT_EQ(comp.size(), 1u);
    EXPECT_EQ(comp[0].result, -EAGAIN);
}

// Write: both devices fail → -EAGAIN (transient; active was reached but backup couldn't be established).
TEST_F(AsyncRaid1Fixture, WriteBothDevicesFail) {
    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
    auto completions = mock->inject_cqe(0, -EIO);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EAGAIN);
}
