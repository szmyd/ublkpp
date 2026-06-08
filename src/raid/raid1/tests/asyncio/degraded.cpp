#include "async_raid1_common.hpp"

// When active (disk_a) fails and __become_degraded cannot write the superblock to the surviving
// device (disk_b), the degradation is NOT persisted on disk. The in-memory route is kept degraded
// (rolling back to EITHER would allow reads to the failed device) and disk_a is marked ERROR.
// Because a crash at this point would let restart self-heal from disk_a (stale), overwriting
// whatever disk_b wrote, the caller receives -EAGAIN rather than the backup result.
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
    // Backup (disk_b) succeeded, but degradation is not on disk → caller gets -EAGAIN.
    auto completions = mock->inject_cqe(0, 4 * Ki);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EAGAIN);

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
// in-memory (no rollback). Because degradation is not on disk the caller receives -EAGAIN, not the
// backup result — a crash would self-heal from the stale disk_a, destroying disk_b's write.
// Phase 2: verifies that the failed SB write still leaves correct in-memory degradation state —
// a subsequent write routes to disk_b only (1 cqe_state) and succeeds.
TEST_F(AsyncRaid1Fixture, WriteAndSbUpdateBothFail) {
    // Catch-all for bitmap-page writes (addr > 0) that occur during shutdown; registered first so
    // the addr=0 EXPECT_CALL (registered second) takes LIFO priority for superblock writes.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // SB writes to disk_b at addr=0: Phase-1 fails, Phase-2 retry succeeds, destructor succeeds.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
        .Times(3)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // Phase 1: active -EIO → SB write fails → disk_a ERROR in-memory; backup result returned.
    {
        auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 2u);

        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty()); // active fails → SB fails → disk_a ERROR
        auto comp = mock->inject_cqe(0, 4 * Ki);        // backup succeeded but SB failed → -EAGAIN
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, -EAGAIN);

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

// Phase 1: active (disk_a) fails; __become_degraded SB write to disk_b fails → _degraded_sb_pending set.
// Phase 2: degraded write to disk_b succeeds; __try_persist_degraded_sb retries and succeeds → write
//          is acked with the normal data result (no EIO). _degraded_sb_pending is cleared.
TEST_F(AsyncRaid1Fixture, DegradedSbPersistRetrySucceeds) {
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // SB writes to disk_b at addr=0: Phase-1 fails; Phase-2 retry and destructor succeed.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
        .Times(3)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // Phase 1: disk_a fails → SB write fails → _degraded_sb_pending set. Site 1 returns -EAGAIN
    //          because the degradation is not yet on disk (client must retry).
    {
        auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 2u);

        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
        auto comp = mock->inject_cqe(0, 4 * Ki);
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, -EAGAIN); // SB write failed → not acked to client
    }
    EXPECT_EQ(raid->replica_states().device_a, ublkpp::raid1::replica_state::ERROR);

    // Phase 2: degraded write → SB retry succeeds → write acked with normal result (not EAGAIN).
    {
        auto res = mock->submit_io(1, UBLK_IO_OP_WRITE, 8 * Ki / 512, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u); // degraded → single cqe_state

        auto comp = mock->inject_cqe(1, 4 * Ki);
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, 4 * Ki); // SB retry cleared the pending flag; write acked normally
    }

    EXPECT_EQ(raid->replica_states().device_a, ublkpp::raid1::replica_state::ERROR);
    EXPECT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::CLEAN);
}

// Phase 1: active (disk_a) fails; __become_degraded SB write to disk_b fails → _degraded_sb_pending set.
// Phase 2: degraded write to disk_b succeeds; __try_persist_degraded_sb retries but fails again →
//          write is NOT acked (EAGAIN returned). _degraded_sb_pending remains set until destructor.
TEST_F(AsyncRaid1Fixture, DegradedSbPersistRetryFails) {
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // SB writes to disk_b at addr=0: Phase-1 fails, Phase-2 retry fails, destructor succeeds.
    EXPECT_CALL(*disk_b, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
        .Times(3)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // Phase 1: disk_a fails → SB write fails → _degraded_sb_pending set. Site 1 returns -EAGAIN
    //          because the degradation is not yet on disk (client must retry).
    {
        auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 2u);

        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
        auto comp = mock->inject_cqe(0, 4 * Ki);
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, -EAGAIN); // SB write failed → not acked to client
    }
    EXPECT_EQ(raid->replica_states().device_a, ublkpp::raid1::replica_state::ERROR);

    // Phase 2: degraded write data succeeds but SB retry fails → EAGAIN returned to caller.
    // The client will retry; _degraded_sb_pending stays set for the destructor to handle.
    {
        auto res = mock->submit_io(1, UBLK_IO_OP_WRITE, 8 * Ki / 512, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        auto comp = mock->inject_cqe(1, 4 * Ki); // disk_b data write succeeded
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, -EAGAIN); // but SB retry failed → not acked
    }
}
