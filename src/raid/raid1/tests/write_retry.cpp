#include "test_raid1_common.hpp"

#include <isa-l/mem_routines.h>

// Retry write that failed on DeviceA and check next write does not dirty bitmap again
TEST(Raid1, WriteRetryA) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_TO_WRITE_SB(device_b);
        EXPECT_TO_WRITE_SB_ASYNC(device_b);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());
    }
    // Expect an extra async results
    auto compls = std::list< ublkpp::async_result >();
    raid_device.collect_async(nullptr, compls);
    EXPECT_EQ(compls.size(), 1);
    compls.clear();

    // Queued Retries should not Fail Immediately, and not dirty header or pages
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
    }
    raid_device.collect_async(nullptr, compls);
    EXPECT_EQ(compls.size(), 1);
    compls.clear();

    {
        // Subsequent writes to dirty regions should not go to the degraded device or dirty already dirty pages
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_EQ(addr, (8 * Ki) + reserved_size);
                return 1;
            });
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
    }

    {
        // Subsequent unaligned writes to clean regions should go to the degraded device and dirty new pages
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
            });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(2)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_EQ(addr, (180 * Ki) + reserved_size);
                return 1;
            })
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_GE(addr, ublkpp::raid1::k_page_size); // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);              // Expect write to bitmap!
                return 1;
            });
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 180 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());
    }

    {
        // Same with non-immediate failures
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE, 4 * Ki, 220 * Ki);
        ublkpp::sub_cmd_t internal_sub_cmd;
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&internal_sub_cmd](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec*,
                                          uint32_t, uint64_t) {
                EXPECT_FALSE(ublkpp::is_internal(sub_cmd));
                internal_sub_cmd = sub_cmd;
                return 1;
            });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_EQ(addr, (220 * Ki) + reserved_size);
                return 1;
            });
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 220 * Ki);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());

        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_GE(addr, ublkpp::raid1::k_page_size); // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);              // Expect write to bitmap!
                return 1;
            });
        res = raid_device.queue_internal_resp(nullptr, &ublk_data, internal_sub_cmd, -5);
        EXPECT_EQ(1, res.value());
        EXPECT_TRUE(res);
        remove_io_data(ublk_data);
    }

    {
        // Subsequent writes that encompass clean regions should go to the degraded device and dirty new pages if it
        // fails
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
            });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(2)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 320 * Ki);
                EXPECT_EQ(addr, reserved_size);
                return 1;
            })
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_GE(addr, ublkpp::raid1::k_page_size); // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);              // Expect write to bitmap!
                return 1;
            });
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 320 * Ki, 0UL);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());
    }
    raid_device.collect_async(nullptr, compls);
    EXPECT_EQ(compls.size(), 0);
    compls.clear();

    {
        // Subsequent writes that encompass dirty regions should go to the degraded device and clean dirty new pages
        // if it works
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE, 320 * Ki, 0UL);
        ublkpp::sub_cmd_t internal_sub_cmd;
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&internal_sub_cmd](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd,
                                          iovec* iovecs, uint32_t, uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                // Is now the REPLICA!
                EXPECT_TRUE(ublkpp::is_replicate(sub_cmd));
                EXPECT_TRUE(ublkpp::is_internal(sub_cmd));
                internal_sub_cmd = sub_cmd;
                EXPECT_EQ(iovecs->iov_len, 320 * Ki);
                EXPECT_EQ(addr, reserved_size);
                return 1;
            });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 320 * Ki);
                EXPECT_EQ(addr, reserved_size);
                return 1;
            });
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 320 * Ki, 0UL);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());

        // expect clean SB written on both devices
        EXPECT_TO_WRITE_SB(device_b);
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_GE(addr, ublkpp::raid1::k_page_size);                                  // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);                                               // Expect write to bitmap!
                EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All zeros
                return 1;
            });
        res = raid_device.queue_internal_resp(nullptr, &ublk_data, internal_sub_cmd, 0);
        EXPECT_EQ(1, res.value());
        EXPECT_TRUE(res);
        remove_io_data(ublk_data);
    }

    // expect unmount_clean on both devices
    EXPECT_TO_WRITE_SB(device_b);
    EXPECT_TO_WRITE_SB(device_a);
}

// Retry write that failed on DeviceB
TEST(Raid1, WriteRetryB) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_TO_WRITE_SB_ASYNC(device_a);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101},
                                         ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value()); // Only one here since failing command was replicated
    }

    // Queued Retries should not Fail Immediately, and not dirty of bitmap
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101},
                                         ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(0, res.value());
    }
    // Expect no extra async results
    auto compls = std::list< ublkpp::async_result >();
    raid_device.collect_async(nullptr, compls);
    EXPECT_EQ(compls.size(), 0);
    compls.clear();

    {
        // Subsequent unaligned writes to clean regions should go to the degraded device and dirty new pages
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
            });
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(3)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 64 * Ki);
                EXPECT_EQ(addr, Gi - (32 * Ki) + reserved_size);
                return 1;
            })
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_GE(addr, ublkpp::raid1::k_page_size); // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);              // Expect write to bitmap!
                return 1;
            })
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_GE(addr, ublkpp::raid1::k_page_size); // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);              // Expect write to bitmap!
                return 1;
            });
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 64 * Ki, Gi - (32 * Ki));
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(3, res.value());
    }

    {
        // Subsequent writes that encompass dirty regions should go to the degraded device and clean dirty new pages
        // if it works
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE, 320 * Ki, Gi - (64 * Ki));
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 320 * Ki);
                EXPECT_EQ(addr, Gi - (64 * Ki) + reserved_size);
                return 1;
            });
        ublkpp::sub_cmd_t internal_sub_cmd;
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&internal_sub_cmd](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd,
                                          iovec* iovecs, uint32_t, uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_TRUE(ublkpp::is_replicate(sub_cmd));
                EXPECT_TRUE(ublkpp::is_internal(sub_cmd));
                internal_sub_cmd = sub_cmd;
                EXPECT_EQ(iovecs->iov_len, 320 * Ki);
                EXPECT_EQ(addr, Gi - (64 * Ki) + reserved_size);
                return 1;
            });
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 320 * Ki, Gi - (64 * Ki));
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());

        // expect clean SB written on both devices
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(2)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_GE(addr, ublkpp::raid1::k_page_size);                                  // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);                                               // Expect write to bitmap!
                EXPECT_EQ(1, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // Beginning still dirty
                return 1;
            })
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_GE(addr, ublkpp::raid1::k_page_size);                                  // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);                                               // Expect write to bitmap!
                EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All zeros
                return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
            });
        res = raid_device.queue_internal_resp(nullptr, &ublk_data, internal_sub_cmd, 0);
        EXPECT_EQ(1, res.value());
        EXPECT_TRUE(res);
        remove_io_data(ublk_data);
    }

    // expect unmount_clean on Device A
    EXPECT_TO_WRITE_SB(device_a);
}
