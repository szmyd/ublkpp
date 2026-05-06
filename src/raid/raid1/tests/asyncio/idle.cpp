#include "async_raid1_common.hpp"

// probe_tick on a healthy array — probes both devices, states remain CLEAN.
TEST_F(AsyncRaid1Fixture, ProbeTickHealthyArray) {
    raid->probe_tick(nullptr);

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u);
}

// probe_tick called twice — idempotent, no crash.
TEST_F(AsyncRaid1Fixture, ProbeTickIdempotent) {
    raid->probe_tick(nullptr);
    raid->probe_tick(nullptr);

    auto const states = raid->replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0u);
}
