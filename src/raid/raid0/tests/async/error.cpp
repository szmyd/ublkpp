#include "async_raid0_common.hpp"

TEST_F(AsyncRaid0Fixture, PreSqeErrorPropagatesEio) {
    // async_iov fails before any SQE is submitted → __distribute returns error → task co_returns -EIO.
    EXPECT_CALL(*disk_a, async_iov(_, _, _, _, _, _))
        .WillOnce(Return(std::unexpected(std::make_error_condition(std::errc::io_error))));

    auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 0u); // no states registered — task already done with -EIO

    auto completions = mock->inject_cqe(0, 0);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EIO);
}

TEST_F(AsyncRaid0Fixture, PostCqeNegativeResultPropagatesAndSkipsRemaining) {
    // First stripe returns -EIO; task exits immediately without awaiting second stripe.
    EXPECT_CALL(*disk_a, async_iov(_, _, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, async_iov(_, _, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_c, async_iov(_, _, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 64 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u); // both dispatched before first co_await

    // First CQE delivers error — task short-circuits and is done immediately.
    auto completions = mock->inject_cqe(0, -EIO);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EIO);
}
