#include "test_raid1_common.hpp"

// Double Failure: Async failure on active device while degraded, then retry fails
// This tests the __handle_async_retry path (raid1.cpp:531) where:
// 1. Array is degraded (device_b unavailable)
// 2. Write to device_a succeeds initially but fails async (on_io_complete with -EIO)
// 3. Retry of the write on device_a fails → double failure detected in __handle_async_retry
TEST(Raid1, WriteDoubleFailure) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    {
        ublkpp::sub_cmd_t working_sub;
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&working_sub](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec*,
                                     uint32_t, uint64_t) {
                working_sub = sub_cmd;
                return 1;
            });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
        raid_device.on_io_complete(&ublk_data, working_sub, 0);
        remove_io_data(ublk_data);
    }

    // Follow up on device A fails without operations on B
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return 1;
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 12 * Ki, 16 * Ki);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());

        raid_device.on_io_complete(&ublk_data, 0b100, -EIO); // Async failure

        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        ASSERT_FALSE(res);
        remove_io_data(ublk_data);
    }

    // Follow up on device A fails without operations on B
    {
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 28 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // expect attempt to flush bitmap
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([&raid_device](uint8_t, iovec*, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);  // Expect write to bitmap!
            EXPECT_LT(addr, raid_device.reserved_size()); // Expect write to bitmap!
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
}

// Double Failure: Both devices fail immediately when array is healthy
// This tests the __replicate recovery path (raid1.cpp:597-602) where:
// 1. Array starts healthy (both devices available)
// 2. First write to active device fails immediately
// 3. __become_degraded succeeds (superblock write works)
// 4. Attempt to write to backup device also fails immediately → catastrophic double failure
// This is different from WriteDoubleFailureImmediate which starts already degraded
TEST(Raid1, WriteDoubleFailureHealthy) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Array starts healthy, both devices available
    // First write: device_a (active) fails, device_b (backup) also fails
    {
        // Expect superblock write to device_b when degrading (device_a fails)
        EXPECT_TO_WRITE_SB(device_b);

        // First attempt: write to device_a fails immediately
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        // Second attempt: write to device_b (after degrading) also fails immediately
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);

        // Both devices failed → catastrophic failure
        ASSERT_FALSE(res);
    }

    // expect attempt to flush bitmap on shutdown
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([&raid_device](uint8_t, iovec*, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);  // Expect write to bitmap!
            EXPECT_LT(addr, raid_device.reserved_size()); // Expect write to bitmap!
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
}

// Double Failure: Immediate (synchronous) failure on active device while degraded
// This tests the __replicate path (raid1.cpp:585-586) where:
// 1. Array is degraded (device_b unavailable)
// 2. Write to device_a (active) fails IMMEDIATELY (not async)
// 3. Since already degraded with active device failing, return error immediately
// This is different from WriteDoubleFailure which uses async completion + retry
TEST(Raid1, WriteDoubleFailureImmediate) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // First, degrade the array by failing device_b
    {
        ublkpp::sub_cmd_t working_sub;
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&working_sub](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec*,
                                     uint32_t, uint64_t) {
                working_sub = sub_cmd;
                return 1;
            });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value()); // One write succeeded
        raid_device.on_io_complete(&ublk_data, working_sub, 0);
        remove_io_data(ublk_data);
    }

    // Now array is degraded (route=DEVA, device_b unavailable)
    // Subsequent write to device_a fails immediately → double failure
    {
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0); // Should not attempt device_b
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                // Immediate failure (not async)
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 32 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        // Should return error immediately without attempting backup device
        ASSERT_FALSE(res);
    }

    // expect attempt to flush bitmap on shutdown
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([&raid_device](uint8_t, iovec*, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);  // Expect write to bitmap!
            EXPECT_LT(addr, raid_device.reserved_size()); // Expect write to bitmap!
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
}
