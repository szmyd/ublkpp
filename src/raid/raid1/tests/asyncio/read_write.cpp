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
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);

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

// Two reads issued from the same thread go to different devices (round-robin balancer).
// On a fresh thread, last_read=DEVB so the first read routes to DEVA (disk_a) and the
// second routes to DEVB (disk_b).
TEST_F(AsyncRaid1Fixture, ReadRoundRobin) {
    std::thread([this] {
        EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(1);
        EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);

        auto res0 = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res0);
        EXPECT_EQ(res0.value(), 1u);

        auto res1 = mock->submit_io(1, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res1);
        EXPECT_EQ(res1.value(), 1u);

        auto comp0 = mock->inject_cqe(0, 4 * Ki);
        ASSERT_EQ(comp0.size(), 1u);
        EXPECT_EQ(comp0[0].tag, 0);

        auto comp1 = mock->inject_cqe(1, 4 * Ki);
        ASSERT_EQ(comp1.size(), 1u);
        EXPECT_EQ(comp1[0].tag, 1);
    }).join();
}

// After the active device (disk_a) fails, subsequent writes go only to the surviving
// device (disk_b). replica_states() must reflect disk_a as ERROR and disk_b as CLEAN.
TEST_F(AsyncRaid1Fixture, WriteDegradedSkipsReplica) {
    // Fail disk_a (active): disk_b (backup) succeeds → array becomes degraded.
    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);
    EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty()); // active fails
    auto comp1 = mock->inject_cqe(0, 4 * Ki);       // backup succeeds → degraded
    ASSERT_EQ(comp1.size(), 1u);
    EXPECT_EQ(comp1[0].result, 4 * Ki);

    EXPECT_EQ(raid->replica_states().device_a, ublkpp::raid1::replica_state::ERROR);
    EXPECT_EQ(raid->replica_states().device_b, ublkpp::raid1::replica_state::CLEAN);

    // Second write: only disk_b (now the active surviving device) should be called.
    EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(0);
    EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1);

    auto res2 = mock->submit_io(1, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res2);
    EXPECT_EQ(res2.value(), 1u); // single CqeState — no backup

    auto comp2 = mock->inject_cqe(1, 4 * Ki);
    ASSERT_EQ(comp2.size(), 1u);
    EXPECT_EQ(comp2[0].result, 4 * Ki);
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
