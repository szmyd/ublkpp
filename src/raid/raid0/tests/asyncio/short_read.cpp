#include "async_raid0_common.hpp"

// A sub-read completing short must fail the whole I/O: the summed result carries no positional
// information, so a positive total would let ublk_drv front-align a partial completion over a
// range no sub-read ever filled.
TEST_F(AsyncRaid0Fixture, ShortChildStripeReadReturnsEIO) {
    // 64KB at sector 0 spans stripe 0 (disk_a) and stripe 1 (disk_b).
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_c, submit_iov(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 64 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    // Stripe 0 completes short (16Ki of 32Ki); stripe 1 completes full. The positive sum (48Ki)
    // is not the requested 64Ki and must surface as -EIO, not as a partial success.
    EXPECT_TRUE(mock->inject_cqe(0, 16 * Ki).empty());
    auto completions = mock->inject_cqe(0, 32 * Ki);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EIO);
}

TEST_F(AsyncRaid0Fixture, ShortChildStripeWriteReturnsEIO) {
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_c, submit_iov(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 64 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    EXPECT_TRUE(mock->inject_cqe(0, 32 * Ki).empty()); // stripe 0 full
    auto completions = mock->inject_cqe(0, 16 * Ki);   // stripe 1 short
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EIO);
}
