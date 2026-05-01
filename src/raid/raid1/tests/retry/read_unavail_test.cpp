#include "../test_raid1_common.hpp"

// Sync I/O path also tracks read failures: device_a fails sync_iov(READ), device_b succeeds.
// After failover, device_a shows UNAVAIL and the route stays EITHER (not degraded).
TEST(Raid1, SyncIoTracksReadFailures) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Device A fails sync read, device B succeeds
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return 1;
    });

    // Use fresh thread so last_read=DEVB → routes to device_a first
    std::vector< iovec > iov(1);
    iov[0].iov_len = 4 * Ki;
    RUN_IN_THREAD({ ASSERT_TRUE(raid_device.sync_iov(UBLK_IO_OP_READ, iov.data(), 1, 12 * Ki)); });

    auto states = raid_device.replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::UNAVAIL);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Idle probe clears UNAVAIL when the device recovers.
// The sync_iov probe at reserved_size() succeeds → unavail flag cleared.
TEST(Raid1, IdleProbeRecoversSingleUnavailDevice) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Induce UNAVAIL on device_a via a failing sync read
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return 1;
    });

    // Use fresh thread so last_read=DEVB → routes to device_a first
    std::vector< iovec > iov(1);
    iov[0].iov_len = 4 * Ki;
    RUN_IN_THREAD({ ASSERT_TRUE(raid_device.sync_iov(UBLK_IO_OP_READ, iov.data(), 1, 12 * Ki)); });
    ASSERT_EQ(raid_device.replica_states().device_a, ublkpp::raid1::replica_state::UNAVAIL);

    // Probe on device_a succeeds — should clear UNAVAIL
    auto const rs = raid_device.reserved_size();
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, rs)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return ublkpp::raid1::k_page_size;
    });

    raid_device.idle_transition(nullptr, true);

    auto states = raid_device.replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Idle probe keeps UNAVAIL when the probe itself fails.
TEST(Raid1, IdleProbeKeepsUnavailOnProbeFailure) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Induce UNAVAIL on device_a
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return 1;
    });

    // Use fresh thread so last_read=DEVB → routes to device_a first
    std::vector< iovec > iov(1);
    iov[0].iov_len = 4 * Ki;
    RUN_IN_THREAD({ ASSERT_TRUE(raid_device.sync_iov(UBLK_IO_OP_READ, iov.data(), 1, 12 * Ki)); });

    // Probe on device_a fails again — UNAVAIL must remain
    auto const rs = raid_device.reserved_size();
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, rs)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    });

    raid_device.idle_transition(nullptr, true);

    auto probe_fail_states = raid_device.replica_states();
    EXPECT_EQ(probe_fail_states.device_a, ublkpp::raid1::replica_state::UNAVAIL);
    EXPECT_EQ(probe_fail_states.device_b, ublkpp::raid1::replica_state::CLEAN);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Idle exit (enter=false) skips the probe entirely.
// Simulates 2 hw queues so idle_transition(true) with count=1 < 2 returns early without probing.
TEST(Raid1, IdleExitSkipsProbe) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Induce UNAVAIL on device_a
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return 1;
    });

    // Use fresh thread so last_read=DEVB → routes to device_a first
    std::vector< iovec > iov(1);
    iov[0].iov_len = 4 * Ki;
    RUN_IN_THREAD({ ASSERT_TRUE(raid_device.sync_iov(UBLK_IO_OP_READ, iov.data(), 1, 12 * Ki)); });

    // Simulate 2 queues so idle_transition(true) returns early (count 1 < 2) without probing
    // the UNAVAIL device — we want to verify that idle exit alone skips the probe.
    raid_device.prepare(nullptr, 0);
    raid_device.prepare(nullptr, 0);
    // Queue 0 enters idle: count = 1 < 2, no probe fired (satisfies the enter-before-exit contract)
    raid_device.idle_transition(nullptr, true);
    // Queue 0 exits idle — no probe sync_iov expected (GMock will catch unexpected calls)
    raid_device.idle_transition(nullptr, false);

    // UNAVAIL must remain unchanged
    auto exit_states = raid_device.replica_states();
    EXPECT_EQ(exit_states.device_a, ublkpp::raid1::replica_state::UNAVAIL);
    EXPECT_EQ(exit_states.device_b, ublkpp::raid1::replica_state::CLEAN);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Idle probe is a no-op when both devices are clean (no UNAVAIL flag set).
TEST(Raid1, IdleProbeNoOpWhenBothClean) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Both CLEAN — no extra sync_iov expected (GMock catches unexpected calls)
    raid_device.idle_transition(nullptr, true);

    auto clean_states = raid_device.replica_states();
    EXPECT_EQ(clean_states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(clean_states.device_b, ublkpp::raid1::replica_state::CLEAN);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
