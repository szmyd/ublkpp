#include "../test_raid1_common.hpp"

// Sync I/O path also tracks read failures: device_a fails sync_iov(READ), device_b succeeds.
// After failover, device_a shows UNAVAIL and the route stays EITHER (not degraded).
TEST(Raid1, SyncIoTracksReadFailures) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

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
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

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

    // Probe on device_a succeeds — should clear UNAVAIL; device_b also probed (unconditional).
    auto const rs = raid_device.reserved_size();
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, rs)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return ublkpp::raid1::k_page_size;
    });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, rs)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return ublkpp::raid1::k_page_size;
    });

    raid_device.probe_tick(nullptr);

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
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

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

    // Probe on device_a fails again — UNAVAIL must remain; device_b also probed (unconditional).
    auto const rs = raid_device.reserved_size();
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, rs)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, rs)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return ublkpp::raid1::k_page_size;
    });

    raid_device.probe_tick(nullptr);

    auto probe_fail_states = raid_device.replica_states();
    EXPECT_EQ(probe_fail_states.device_a, ublkpp::raid1::replica_state::UNAVAIL);
    EXPECT_EQ(probe_fail_states.device_b, ublkpp::raid1::replica_state::CLEAN);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// probe_tick is a no-op when both devices are clean (no UNAVAIL flag set).
// probe_tick probes all mirrors unconditionally — detects both unavail-recovery and
// healthy→failed transitions during idle. Both devices get a probe read even when CLEAN.
TEST(Raid1, ProbeTickProbsBothDevices) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto const rs = raid_device.reserved_size();
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, rs)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return ublkpp::raid1::k_page_size;
    });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, rs)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return ublkpp::raid1::k_page_size;
    });

    raid_device.probe_tick(nullptr);

    auto clean_states = raid_device.replica_states();
    EXPECT_EQ(clean_states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(clean_states.device_b, ublkpp::raid1::replica_state::CLEAN);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
