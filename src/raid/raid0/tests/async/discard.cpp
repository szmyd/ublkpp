#include "async_raid0_common.hpp"

TEST_F(AsyncRaid0Fixture, DiscardSingleStripe) {
    // 4KB discard at sector 0 → only disk_a.
    // handle_discard creates CqeState via io->ensure; inject_cqe delivers the result.
    EXPECT_CALL(*disk_a, handle_discard(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, handle_discard(_, _, _, _, _)).Times(0);
    EXPECT_CALL(*disk_c, handle_discard(_, _, _, _, _)).Times(0);

    auto res = mock->submit_io(0, UBLK_IO_OP_DISCARD, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);

    auto completions = mock->inject_cqe(0, 0);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, 0);
}

TEST_F(AsyncRaid0Fixture, DiscardAllStripes) {
    // 96KB discard spanning all three stripes — merged_subcmds fans out to each.
    EXPECT_CALL(*disk_a, handle_discard(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, handle_discard(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_c, handle_discard(_, _, _, _, _)).Times(1);

    auto res = mock->submit_io(0, UBLK_IO_OP_DISCARD, 0, 96 * Ki / 512, nullptr);
    ASSERT_TRUE(res);

    EXPECT_TRUE(mock->inject_cqe(0, 0).empty()); // stripe 0
    EXPECT_TRUE(mock->inject_cqe(0, 0).empty()); // stripe 1
    auto completions = mock->inject_cqe(0, 0);   // stripe 2
    ASSERT_EQ(completions.size(), 1u);
}
