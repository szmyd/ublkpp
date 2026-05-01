#include "async_raid1_common.hpp"

// Healthy discard: fans out to both replicas via async_iov.
// inject active first (RAID1 suspends on backup), then inject backup → completion.
TEST_F(AsyncRaid1Fixture, DiscardBothDevices) {
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);

    // 32KiB discard — chunk-aligned so totally_aligned=true (chunk_size=32Ki)
    auto res = mock->submit_io(0, UBLK_IO_OP_DISCARD, 0, 32 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u); // active + backup CqeStates

    // Active done, suspend on backup
    EXPECT_TRUE(mock->inject_cqe(0, 0).empty());
    // Backup done → completion
    auto completions = mock->inject_cqe(0, 0);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].tag, 0);
    EXPECT_EQ(completions[0].result, 0);
}

// Discard where async_iov returns 0 (synchronous inline completion) → task finishes
// without suspending on co_await *state. RAID1 still awaits both tasks.
TEST_F(AsyncRaid1Fixture, DiscardSyncCompletion) {
    ON_CALL(*disk_a, submit_iov(_, _, _, _, _)).WillByDefault(Return(io_result{0}));
    ON_CALL(*disk_b, submit_iov(_, _, _, _, _)).WillByDefault(Return(io_result{0}));

    auto res = mock->submit_io(0, UBLK_IO_OP_DISCARD, 0, 32 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    // Both tasks complete synchronously; no CqeStates registered
    EXPECT_EQ(res.value(), 0u);

    // No waiter — task already done; inject_cqe returns stored result immediately
    auto completions = mock->inject_cqe(0, 0);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, 0);
}
