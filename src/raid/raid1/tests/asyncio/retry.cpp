#include "async_raid1_common.hpp"

// A read failure on the primary device sets UNAVAIL (not ERROR/degraded).
// The route stays EITHER; bytes_to_sync stays 0.
TEST_F(AsyncRaid1Fixture, ReadFailureSetsUnavail) {
    std::thread([this] {
        // First read routes to disk_a (fresh thread: last_read=DEVB → DEVA).
        // disk_a fails → RAID1 falls over to disk_b → disk_a gets UNAVAIL flag.
        auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty()); // disk_a fails → failover task started
        auto comp = mock->inject_cqe(0, 4 * Ki);        // disk_b succeeds
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, 4 * Ki);
    }).join();

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::UNAVAIL);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u); // route still EITHER — not degraded
}

// Both devices fail a read. Only the first-tried device (disk_a) gets UNAVAIL;
// the failover device (disk_b) does not — the read path returns the error without
// marking disk_b unavail. Array route stays EITHER.
TEST_F(AsyncRaid1Fixture, ReadBothFailStaysHealthy) {
    std::thread([this] {
        auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty()); // disk_a fails → failover
        auto comp = mock->inject_cqe(0, -EIO);          // disk_b also fails → -EIO
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, -EIO);
    }).join();

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::UNAVAIL);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u);
}

// Write failure on the backup (disk_b) degrades the array. The failed device must
// show ERROR (not UNAVAIL), and bytes_to_sync must be > 0 (dirty bitmap).
TEST_F(AsyncRaid1Fixture, WriteReplicaFailShowsError) {
    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    EXPECT_TRUE(mock->inject_cqe(0, 4 * Ki).empty()); // active (disk_a) succeeds
    auto comp = mock->inject_cqe(0, -EIO);            // backup (disk_b) fails → degraded
    ASSERT_EQ(comp.size(), 1u);
    EXPECT_EQ(comp[0].result, 4 * Ki);

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::ERROR);
    EXPECT_GT(states.bytes_to_sync, 0u);
}

// After UNAVAIL is set on disk_a by a read failure, a subsequent read that would
// normally route to disk_a (via round-robin) is redirected to disk_b instead.
// Thread sequence: read-0 → disk_a (fails, UNAVAIL set), read-1 → disk_b (round-robin),
// read-2 → would be disk_a but UNAVAIL → reroutes to disk_b.
TEST_F(AsyncRaid1Fixture, UnavailReadReroutes) {
    std::thread([this] {
        // Read 0: fresh thread → disk_a first; disk_a fails → disk_b succeeds → UNAVAIL on disk_a.
        {
            auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
            ASSERT_TRUE(res);
            EXPECT_EQ(res.value(), 1u);
            EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());
            auto c = mock->inject_cqe(0, 4 * Ki);
            ASSERT_EQ(c.size(), 1u);
        }
        // Confirm disk_a is UNAVAIL.
        ASSERT_EQ(raid->replica_states().device_a, ublkpp::raid1::replica_state::UNAVAIL);

        // Read 1: last_read=DEVA → next = DEVB. disk_b succeeds normally.
        {
            EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(0);
            EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1).WillRepeatedly(make_async_iov_action());
            auto res = mock->submit_io(1, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
            ASSERT_TRUE(res);
            auto c = mock->inject_cqe(1, 4 * Ki);
            ASSERT_EQ(c.size(), 1u);
        }

        // Read 2: last_read=DEVB → round-robin gives DEVA, but UNAVAIL → reroutes to DEVB.
        {
            EXPECT_CALL(*disk_a, submit_iov(_, _, _, _, _)).Times(0);
            EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _)).Times(1).WillRepeatedly(make_async_iov_action());
            auto res = mock->submit_io(2, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
            ASSERT_TRUE(res);
            auto c = mock->inject_cqe(2, 4 * Ki);
            ASSERT_EQ(c.size(), 1u);
        }
    }).join();
}
