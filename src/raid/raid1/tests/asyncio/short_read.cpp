#include "async_raid1_common.hpp"

// A short positive read completion on the primary is an integrity failure, not back-pressure:
// the leg is marked UNAVAIL (same as a read error) and the read fails over to the peer, which
// serves the full length. The caller sees a full-length success.
TEST_F(AsyncRaid1Fixture, ShortPrimaryReadFailsOver) {
    std::thread([this] {
        // Fresh thread: first read routes to disk_a (last_read=DEVB -> DEVA).
        auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        EXPECT_TRUE(mock->inject_cqe(0, 2 * Ki).empty()); // disk_a short (2Ki of 4Ki): failover started
        auto comp = mock->inject_cqe(0, 4 * Ki);          // disk_b serves the full read
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, 4 * Ki);
    }).join();

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::UNAVAIL);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u); // route still EITHER, not degraded
}

// Short completions on both legs fail the read with -EIO, never -EAGAIN: a requeue would spin
// against the same misbehaving legs while the buffer content is untrustworthy. A zero-length
// completion (the read-past-EOF signature) takes the same path as a partial one.
TEST_F(AsyncRaid1Fixture, ShortBothLegsReturnsEIO) {
    std::thread([this] {
        auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, 4 * Ki / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        EXPECT_TRUE(mock->inject_cqe(0, 2 * Ki).empty()); // disk_a short: failover started
        auto comp = mock->inject_cqe(0, 0);               // disk_b zero-length: -EIO
        ASSERT_EQ(comp.size(), 1u);
        EXPECT_EQ(comp[0].result, -EIO);
    }).join();

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::UNAVAIL);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u);
}
