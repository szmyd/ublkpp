#include "async_raid1_common.hpp"

// idle_transition(true) on a healthy array — both devices available, no probing triggered.
TEST_F(AsyncRaid1Fixture, IdleTransitionEnter) {
    raid->idle_transition(nullptr, true);

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u);
}

// idle_transition enter+exit round trip — satisfies the ublksrv enter-before-exit contract.
TEST_F(AsyncRaid1Fixture, IdleTransitionRoundTrip) {
    raid->idle_transition(nullptr, true);
    raid->idle_transition(nullptr, false);

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u);
}
