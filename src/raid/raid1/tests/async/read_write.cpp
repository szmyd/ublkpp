#include "async_raid1_common.hpp"

// Healthy read: routes to one device, 1 CqeState registered.
// thread_local last_read starts at DEVB on a fresh thread, so first read → DEVA (disk_a).
TEST_F(AsyncRaid1Fixture, ReadSingleDevice) {
    std::thread([this] {
        auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        auto completions = mock->inject_cqe(0, 4 * Ki);
        ASSERT_EQ(completions.size(), 1u);
        EXPECT_EQ(completions[0].tag, 0);
        EXPECT_EQ(completions[0].result, 4 * Ki);
    }).join();
}

// Healthy write: replicates to both devices.
// inject active CQE first (task suspends on backup), then inject backup CQE (task done).
TEST_F(AsyncRaid1Fixture, WriteBothDevices) {
    EXPECT_CALL(*disk_a, async_iov(_, _, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, async_iov(_, _, _, _, _, _)).Times(1);

    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u); // active + backup CqeStates registered

    // First inject → active done, RAID1 suspends on backup → no completion yet
    EXPECT_TRUE(mock->inject_cqe(0, 4 * Ki).empty());
    // Second inject → backup done, RAID1 completes → returns active bytes
    auto completions = mock->inject_cqe(0, 4 * Ki);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].tag, 0);
    EXPECT_EQ(completions[0].result, 4 * Ki);
}

// Two concurrent write slots: each independent tag resolves separately.
TEST_F(AsyncRaid1Fixture, TwoTagsWriteIndependent) {
    auto res0 = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    auto res1 = mock->submit_io(1, UBLK_IO_OP_WRITE, 64, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res0);
    ASSERT_TRUE(res1);
    EXPECT_EQ(res0.value(), 2u);
    EXPECT_EQ(res1.value(), 2u);

    // Resolve tag 1 first (inject active then backup)
    EXPECT_TRUE(mock->inject_cqe(1, 4 * Ki).empty());
    auto c1 = mock->inject_cqe(1, 4 * Ki);
    ASSERT_EQ(c1.size(), 1u);
    EXPECT_EQ(c1[0].tag, 1);
    EXPECT_EQ(c1[0].result, 4 * Ki);

    // Resolve tag 0
    EXPECT_TRUE(mock->inject_cqe(0, 4 * Ki).empty());
    auto c0 = mock->inject_cqe(0, 4 * Ki);
    ASSERT_EQ(c0.size(), 1u);
    EXPECT_EQ(c0[0].tag, 0);
    EXPECT_EQ(c0[0].result, 4 * Ki);
}
