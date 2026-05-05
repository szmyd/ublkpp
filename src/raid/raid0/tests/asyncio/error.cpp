#include "async_raid0_common.hpp"

TEST_F(AsyncRaid0Fixture, PreSqeErrorPropagatesEio) {
    // async_iov fails before any SQE is submitted → __distribute returns error → task co_returns -EIO.
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _))
        .WillOnce(Return(std::unexpected(std::make_error_condition(std::errc::io_error))));

    auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 0u); // no states registered — task already done with -EIO

    auto completions = mock->inject_cqe(0, 0);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EIO);
}

TEST_F(AsyncRaid0Fixture, PostCqeNegativeResultDrainsAndPropagatesError) {
    // First stripe errors; second stripe succeeds. Both must be awaited before result
    // to avoid resuming a destroyed coroutine frame when the second CQE arrives.
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_c, submit_iov(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 64 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u); // both dispatched before first co_await

    // First CQE delivers error — task NOT done yet; must drain stripe B first.
    auto mid = mock->inject_cqe(0, -EIO);
    EXPECT_TRUE(mid.empty()); // task still awaiting second stripe

    // Second CQE delivers success — task now complete, first error wins.
    auto completions = mock->inject_cqe(0, static_cast< int >(k_stripe_size));
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EIO);
}
