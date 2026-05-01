#include "async_raid0_common.hpp"

// 3-stripe layout: stripe_size=32Ki, stride_width=96Ki
// addr passed to __distribute = (start_sector << 9) + 96Ki
// Stripe selection: (addr % stride_width) / stripe_size
//   sector 0   → addr=96Ki → stripe 0 (disk_a)
//   sector 64  → addr=128Ki → stripe 1 (disk_b)
//   sector 128 → addr=160Ki → stripe 2 (disk_c)

TEST_F(AsyncRaid0Fixture, ReadSingleStripe) {
    // 4KB at sector 0 stays within stripe 0 (disk_a) only.
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(0);
    EXPECT_CALL(*disk_c, submit_iov(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 1u); // 1 CqeState registered

    auto completions = mock->inject_cqe(0, 4 * Ki);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].tag, 0);
    EXPECT_EQ(completions[0].result, 4 * Ki);
}

TEST_F(AsyncRaid0Fixture, ReadCrossStripe) {
    // 64KB at sector 0 spans stripe 0 (32KB) and stripe 1 (32KB).
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_c, submit_iov(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 64 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    // First stripe completes — task suspends on second
    EXPECT_TRUE(mock->inject_cqe(0, 32 * Ki).empty());
    // Second stripe completes — task done
    auto completions = mock->inject_cqe(0, 32 * Ki);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, 64 * Ki);
}

TEST_F(AsyncRaid0Fixture, ReadAllStripes) {
    // 96KB spans all three stripes (32KB each).
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_c, submit_iov(_, _, _, _, _)).Times(1);

    auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 96 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 3u);

    EXPECT_TRUE(mock->inject_cqe(0, 32 * Ki).empty()); // stripe 0
    EXPECT_TRUE(mock->inject_cqe(0, 32 * Ki).empty()); // stripe 1
    auto completions = mock->inject_cqe(0, 32 * Ki);   // stripe 2
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, 96 * Ki);
}

TEST_F(AsyncRaid0Fixture, WriteSingleStripe) {
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(0);
    EXPECT_CALL(*disk_c, submit_iov(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);

    auto completions = mock->inject_cqe(0, 4 * Ki);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, 4 * Ki);
}

TEST_F(AsyncRaid0Fixture, WriteSecondStripe) {
    // 4KB at sector 64 (=32KB offset) routes to stripe 1 (disk_b).
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(0);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_c, submit_iov(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 64, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);

    auto completions = mock->inject_cqe(0, 4 * Ki);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, 4 * Ki);
}
