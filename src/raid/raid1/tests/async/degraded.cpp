#include "async_raid1_common.hpp"

// When active (disk_a) fails and __become_degraded cannot write the superblock to the surviving
// device (disk_b), the degradation is rolled back: route stays EITHER, disk_a is not marked
// unavail. The backup write that was already in flight succeeds and is returned to the caller.
TEST_F(AsyncRaid1Fixture, BecomeDegradedSbFail) {
    // Intercept the superblock WRITE at addr=0 that __become_degraded issues to disk_b.
    // RetiresOnSaturation so the destructor's clean-shutdown SB write falls through to ON_CALL.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, 0))
        .Times(2)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    // Active (disk_a) fails → __become_degraded fires, SB write to disk_b fails → rollback.
    EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
    // Backup (disk_b) write succeeds → returned (backup_res) despite active failure.
    auto completions = mock->inject_cqe(0, 4 * Ki);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, 4 * Ki);

    // Route was rolled back to EITHER — replica_states() EITHER branch hard-codes sync_bytes=0
    // and treats both devices as active, so both show CLEAN even though dirty_region was called.
    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u);
}

// When the backup (disk_b) write fails and __become_degraded succeeds in writing the superblock
// to the active device (disk_a), disk_b is marked ERROR and the caller gets the active result.
// This is the normal degradation path from `WriteReplicaFailsBecomeDegraded` — this test verifies
// the post-degradation read routing: reads must go to disk_a (now the sole active device).
TEST_F(AsyncRaid1Fixture, ReadAfterReplicaFail) {
    // Degrade the array: disk_b (backup) fails the write.
    {
        auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 2u);

        EXPECT_TRUE(mock->inject_cqe(0, 4 * Ki).empty()); // active succeeds
        auto comp = mock->inject_cqe(0, -EIO);            // backup fails → degraded
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, 4 * Ki);
    }
    ASSERT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::ERROR);

    // In degraded mode with disk_b unavail, reads must route to disk_a only.
    std::thread([this] {
        EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
        EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(0);

        auto res = mock->submit_io(1, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        auto comp = mock->inject_cqe(1, 4 * Ki);
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, 4 * Ki);
    }).join();
}
