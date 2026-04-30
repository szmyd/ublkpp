#include "async_raid0_common.hpp"

TEST_F(AsyncRaid0Fixture, FlushDelegatesAndCompletesSynchronously) {
    // Raid0Disk::handle_flush fans out to all children; each child returns 0.
    // handle_io_async co_returns synchronously — no SQEs, no inject_cqe needed.
    EXPECT_CALL(*disk_a, handle_flush(_, _, _)).Times(1);
    EXPECT_CALL(*disk_b, handle_flush(_, _, _)).Times(1);
    EXPECT_CALL(*disk_c, handle_flush(_, _, _)).Times(1);
    EXPECT_CALL(*disk_a, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*disk_b, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*disk_c, async_iov(_, _, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_FLUSH, 0, 0, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 0u); // no CqeStates registered

    // Task completed synchronously; inject_cqe with any value returns the stored result.
    auto completions = mock->inject_cqe(0, 0);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].tag, 0);
    EXPECT_EQ(completions[0].result, 0);
}
