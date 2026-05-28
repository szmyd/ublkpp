#include "async_raid1_common.hpp"

// When active (disk_a) fails and __become_degraded cannot write the superblock to the surviving
// device (disk_b), the degradation is NOT rolled back. Rolling back to EITHER would allow
// round-robin reads to disk_a (which missed the write), serving inconsistent data. Instead,
// disk_a is marked ERROR in-memory and the backup result is returned to the caller. The dirty
// bitmap covers the region; shutdown or full recovery will reconcile.
TEST_F(AsyncRaid1Fixture, BecomeDegradedSbFail) {
    // Catch-all for bitmap-page writes (addr > 0) that occur during shutdown; registered first so
    // the addr=0 EXPECT_CALL (registered second) takes LIFO priority for superblock writes.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // Intercept the superblock WRITE at addr=0 that __become_degraded issues to disk_b.
    // Second WillOnce covers the clean-shutdown SB write after the test.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, 0))
        .Times(2)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    // Active (disk_a) fails → __become_degraded fires, SB write to disk_b fails.
    EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
    // Backup (disk_b) succeeded; its result is returned to the caller.
    auto completions = mock->inject_cqe(0, 4 * Ki);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, 4 * Ki);

    // disk_a is ERROR in-memory; disk_b is the sole active device.
    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::ERROR);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_GT(states.bytes_to_sync, 0u);
}

// An I/O request with an unrecognised opcode must be rejected immediately without touching either
// device. The completion result must be negative.
TEST_F(AsyncRaid1Fixture, UnknownOpcodeIsRejected) {
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(0);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, 0xFF, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 0u); // sync completion — no CqeStates registered

    auto completions = mock->inject_cqe(0, 0);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_LT(completions[0].result, 0);
}

// Phase 1: active (disk_a) fails AND the superblock write to disk_b fails. disk_a is marked ERROR
// in-memory (no rollback); the backup result is returned to the caller.
// Phase 2: array is now degraded; a subsequent write routes to disk_b only (1 cqe_state).
// Together these verify that a failed SB write still produces correct in-memory degradation and
// that writes in the resulting degraded state behave correctly.
TEST_F(AsyncRaid1Fixture, WriteAndSbUpdateBothFail) {
    // Catch-all for bitmap-page writes (addr > 0) that occur during shutdown; registered first so
    // the addr=0 EXPECT_CALL (registered second) takes LIFO priority for superblock writes.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // SB writes to disk_b at addr=0: Phase-1 fails, shutdown succeeds.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
        .Times(2)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // Phase 1: active -EIO → SB write fails → disk_a ERROR in-memory; backup result returned.
    {
        auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 2u);

        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty()); // active fails → SB fails → disk_a ERROR
        auto comp = mock->inject_cqe(0, 4 * Ki);        // backup succeeded → returned to caller
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, 4 * Ki);

        auto const states = raid->replica_states();
        EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::ERROR);
        EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
        EXPECT_GT(states.bytes_to_sync, 0u);
    }

    // Phase 2: array is degraded; write routes to disk_b only.
    {
        auto res = mock->submit_io(1, UBLK_IO_OP_WRITE, 8 * Ki / 512, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u); // degraded → single cqe_state

        auto comp = mock->inject_cqe(1, 4 * Ki);
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_GT(comp[0].result, 0);

        EXPECT_EQ(raid->replica_states().device_a, ublkpp::raid1::replica_state::ERROR);
        EXPECT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::CLEAN);
    }
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
        EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1).WillRepeatedly(make_async_iov_action());
        EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(0);

        auto res = mock->submit_io(1, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        auto comp = mock->inject_cqe(1, 4 * Ki);
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, 4 * Ki);
    }).join();
}
