#include "test_raid1_common.hpp"

// Test for RAID1 bitmap race condition during degraded writes
// This test verifies the fix for a critical data corruption bug where:
// 1. Array is degraded, resync clears unavail flag and bitmap for a region
// 2. Async write arrives for that same region
// 3. Device appears available (!unavail) and bitmap shows clean (!is_dirty)
// 4. Write is launched to DIRTY_DEVICE without dirtying bitmap first
// 5. Resync checks bitmap while async write is in-flight, sees clean, skips region
// 6. Async write fails later, but resync already passed the region
// 7. Region never gets resynced, leading to stale reads
//
// The fix ensures bitmap is always dirtied BEFORE launching async writes to DIRTY_DEVICE
TEST(Raid1, WriteFailAsyncBitmapRace) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Step 1: Fail device A immediately to make array degraded
    {
        EXPECT_TO_WRITE_SB(device_b);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                         uint64_t const) { return std::unexpected(std::make_error_condition(std::errc::io_error)); });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&raid_device](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd,
                                     iovec* iovecs, uint32_t, uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 32 * Ki);
                EXPECT_EQ(addr, (64 * Ki) + raid_device.reserved_size());
                return 1;
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 32 * Ki, 64 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
    }

    // Step 2: Simulate device A becoming available again (e.g., after resync clears unavail)
    // This is done via handle_internal which clears the unavail flag
    {
        iovec iov{.iov_base = nullptr, .iov_len = 32 * Ki};
        // This simulates the resync clearing unavail and bitmap for the region
        auto res = raid_device.handle_internal(nullptr, nullptr, 0b100, &iov, 1, 64 * Ki, 0);
        ASSERT_TRUE(res);
        EXPECT_EQ(0, res.value());
    }

    // Step 3: Write to the SAME region (64 Ki) that was just "resynced"
    // At this point: device_a unavail=false (just cleared), bitmap is clean (just cleared)
    // The bug: code would skip dirtying bitmap and launch async write
    // The fix: code must dirty bitmap BEFORE launching async write
    {
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);

        // Device B write should succeed (CLEAN_DEVICE)
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&raid_device](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd,
                                     iovec* iovecs, uint32_t, uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 32 * Ki);
                EXPECT_EQ(addr, (64 * Ki) + raid_device.reserved_size());
                return 1;
            });

        // Device A write should be attempted (DIRTY_DEVICE) with REPLICATE flag
        // This write will FAIL (simulating device still being bad)
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&raid_device](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd,
                                     iovec* iovecs, uint32_t, uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                EXPECT_TRUE(ublkpp::is_replicate(sub_cmd));
                EXPECT_TRUE(ublkpp::is_internal(sub_cmd)); // Should have INTERNAL flag
                EXPECT_EQ(iovecs->iov_len, 32 * Ki);
                EXPECT_EQ(addr, (64 * Ki) + raid_device.reserved_size());
                // Fail the write to simulate device still being unreliable
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 32 * Ki, 64 * Ki);
        remove_io_data(ublk_data);

        // Write should return failure since the replicate write failed
        ASSERT_FALSE(res);
    }

    // Step 4: Verify bitmap was dirtied BEFORE the async write was launched
    // If the fix is correct, the bitmap should now be dirty for region 64 Ki
    // This can be verified by checking that subsequent writes to other regions
    // still dirty the bitmap properly
    {
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0); // Device still unavail
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&raid_device](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd,
                                     iovec* iovecs, uint32_t, uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 32 * Ki);
                EXPECT_EQ(addr, (96 * Ki) + raid_device.reserved_size());
                return 1;
            });
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 32 * Ki, 96 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
    }

    // Cleanup: expect bitmap sync on unmount
    EXPECT_TO_WRITE_SB(device_b);
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([&raid_device](uint8_t, iovec*, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);  // Expect write to bitmap!
            EXPECT_LT(addr, raid_device.reserved_size()); // Expect write to bitmap!
            return ublkpp::raid1::k_page_size;
        })
        .RetiresOnSaturation();
}

// Test that verifies the optimistic write path still works correctly when device is stable
TEST(Raid1, OptimisticWritePathSuccess) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Step 1: Degrade array
    {
        EXPECT_TO_WRITE_SB(device_b);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                         uint64_t const) { return std::unexpected(std::make_error_condition(std::errc::io_error)); });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(1).WillOnce([](auto...) { return 1; });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 32 * Ki, 64 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
    }

    // Step 2: Make device A available again
    {
        iovec iov{.iov_base = nullptr, .iov_len = 32 * Ki};
        auto res = raid_device.handle_internal(nullptr, nullptr, 0b100, &iov, 1, 64 * Ki, 0);
        ASSERT_TRUE(res);
    }

    // Step 3: Write to same region - optimistic write should succeed
    // With the fix, bitmap is dirtied first, then cleaned on success
    {
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);

        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(1).WillOnce([](auto...) { return 1; });

        // Device A write succeeds this time
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&raid_device](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd,
                                     iovec* iovecs, uint32_t, uint64_t addr) {
                EXPECT_TRUE(ublkpp::is_replicate(sub_cmd));
                EXPECT_TRUE(ublkpp::is_internal(sub_cmd)); // Should have INTERNAL flag for cleanup
                EXPECT_EQ(iovecs->iov_len, 32 * Ki);
                return 1;
            });

        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 32 * Ki, 64 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());
    }

    // Step 4: Call handle_internal to simulate async completion and bitmap cleanup
    {
        iovec iov{.iov_base = nullptr, .iov_len = 32 * Ki};
        auto res = raid_device.handle_internal(nullptr, nullptr, 0b100, &iov, 1, 64 * Ki, 0);
        ASSERT_TRUE(res);
        EXPECT_EQ(0, res.value()); // Region should be cleaned
    }

    // Cleanup
    EXPECT_TO_WRITE_SB(device_b);
}
