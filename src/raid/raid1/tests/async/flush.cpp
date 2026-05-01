#include "async_raid1_common.hpp"

// RAID1's handle_flush returns 0 synchronously — no children are called, no SQEs submitted.
// handle_io_async co_returns 0 immediately — pool size is 0.
// inject_cqe with any result on a finished task returns the stored value.
TEST_F(AsyncRaid1Fixture, FlushCompletesSync) {
    EXPECT_CALL(*disk_a, async_iov(_, _, _, _, _)).Times(0);
    EXPECT_CALL(*disk_b, async_iov(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_FLUSH, 0, 0, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 0u); // no CqeStates registered

    auto completions = mock->inject_cqe(0, 0);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].tag, 0);
    EXPECT_EQ(completions[0].result, 0);
}
