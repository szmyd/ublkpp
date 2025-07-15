#include "test_raid1_common.hpp"

// Brief: Test that a simple WRITE operation is replicated to both underlying Devices.
TEST(Raid1, SimpleWrite) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            // Route is for Device A
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            // SubCommand does not have the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 16 * Ki);
            EXPECT_EQ(addr, (12 * Ki) + reserved_size);
            return 1;
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            // SubCommand has the replicated bit set
            EXPECT_TRUE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 16 * Ki);
            EXPECT_EQ(addr, (12 * Ki) + reserved_size);
            return 1;
        });

    auto ublk_data = make_io_data(UBLK_IO_OP_WRITE, 16 * Ki, 12 * Ki);
    // Construct a Retry Route that points to Device A in a RAID1 device
    auto const current_sub_cmd = 0b10;
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, current_sub_cmd);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(2, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Retry write that failed on DeviceA and check next write does not dirty bitmap again
TEST(Raid1, WriteRetryA) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

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

    // Queued Retries should not Fail Immediately, and not dirty header
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_TO_WRITE_SB_ASYNC(device_b);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());
    }
    // Expect an extra async results
    auto compls = std::list< ublkpp::async_result >();
    raid_device.collect_async(nullptr, compls);
    EXPECT_EQ(compls.size(), 2);
    compls.clear();

    // Subsequent writes should not go to device A
    auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    raid_device.collect_async(nullptr, compls);
    EXPECT_EQ(compls.size(), 0);
    compls.clear();

    // expect unmount_clean on Device B
    EXPECT_TO_WRITE_SB(device_b);
}

// Retry write that failed on DeviceA and check that a failure to update the SB is terminal
TEST(Raid1, WriteRetryAFailure) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB_F(device_b, true);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // Queued Retries should attempt to update the SB as well
    {
        EXPECT_TO_WRITE_SB_F(device_b, true);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // Subsequent writes should continue to go to side A first
    auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        });
    // The primary device has not been rotated to B from the retries above since updating the SB failed
    // and have not successfully become degraded yet
    EXPECT_TO_WRITE_SB_F(device_b, true);
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
    remove_io_data(ublk_data);
    ASSERT_FALSE(res);

    // expect unmount_clean attempt on both devices
    EXPECT_TO_WRITE_SB_F(device_a, true);
    EXPECT_TO_WRITE_SB_F(device_b, true);
}

// Retry write that failed on DeviceB
TEST(Raid1, WriteRetryB) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_TO_WRITE_SB_ASYNC(device_a);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101},
                                         ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());
    }

    // Queued Retries should not Fail Immediately, and not dirty of bitmap
    {
        EXPECT_TO_WRITE_SB_ASYNC(device_a);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101},
                                         ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());
    }
    // Expect an extra async results
    auto compls = std::list< ublkpp::async_result >();
    raid_device.collect_async(nullptr, compls);
    EXPECT_EQ(compls.size(), 2);
    compls.clear();

    // expect unmount_clean on Device A
    EXPECT_TO_WRITE_SB(device_a);
}

// Double Failure returns I/O error
TEST(Raid1, WriteDoubleFailure) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_TO_WRITE_SB_ASYNC(device_a);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101},
                                         ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());
    }

    // Follow up retry on device A fails without operations on B
    {
        RLOGI("Here");
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // expect unmount_clean on last working device
    EXPECT_TO_WRITE_SB_F(device_a, true);
}

// Immediate Write Fail
TEST(Raid1, WriteFailImmediateDevA) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB(device_b);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce(
                [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t const) {
                    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
                });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(2)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_GE(addr, ublkpp::raid1::SuperBlock::SIZE); // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);                   // Expect write to bitmap!
                return 1;
            })
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_EQ(addr, (8 * Ki) + reserved_size);
                return 1;
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());
    }

    // Subsequent writes should not go to device A
    auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // expect unmount_clean on Device B
    EXPECT_TO_WRITE_SB(device_b);
}

// Immediate Write Fail
TEST(Raid1, WriteFailImmediateDevB) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB_F(device_a, true);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                         uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                // SubCommand has the replicated bit set now
                EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_EQ(addr, (8 * Ki) + reserved_size);
                return 1;
            });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce(
                [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t const) {
                    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
                });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // Subsequent writes should go to both devices
    auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
    EXPECT_TO_WRITE_SB_F(device_b, true);
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t const) {
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        });
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
    remove_io_data(ublk_data);
    ASSERT_FALSE(res);

    // expect unmount_clean on Device B
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Immediate Write Fail
TEST(Raid1, WriteFailImmediateFailFailSBUpdate) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce(
                [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t const) {
                    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
                });
        EXPECT_TO_WRITE_SB_F(device_b, true); // Fail the attempt to dirty the SB

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // Subsequent writes should continue to try device A this time succeeding dirty of header and bitmap update
    auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
    EXPECT_TO_WRITE_SB(device_b);
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t const) {
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(2)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_GE(addr, ublkpp::raid1::SuperBlock::SIZE); // Expect write to bitmap!
            EXPECT_LT(addr, reserved_size);                   // Expect write to bitmap!
            return 1;
        })
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(2, res.value());

    // expect unmount_clean on Device B
    EXPECT_TO_WRITE_SB(device_b);
}

//  Failed Discards should flip the route too
TEST(Raid1, Discard) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce(
            [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, uint32_t len, uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
                EXPECT_EQ(len, 4 * Ki);
                EXPECT_EQ(addr, (8 * Ki) + reserved_size);
                return 1;
            });
    EXPECT_CALL(*device_b, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce(
            [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, uint32_t len, uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_TRUE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
                EXPECT_EQ(len, 4 * Ki);
                EXPECT_EQ(addr, (8 * Ki) + reserved_size);
                return 1;
            });

    auto ublk_data = make_io_data(UBLK_IO_OP_DISCARD, 4 * Ki, 8 * Ki);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, 0b10);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(2, res.value());
    // expect unmount_clean on devices
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

//  Failed Discards should flip the route and dirty the bitmap too
TEST(Raid1, DiscardRetry) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_TO_WRITE_SB_ASYNC(device_a);
        EXPECT_CALL(*device_a, handle_discard(_, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, handle_discard(_, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101},
                                         ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATED);
        auto res = raid_device.handle_discard(nullptr, &ublk_data, sub_cmd, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        // No need to re-write on A side
        EXPECT_EQ(2, res.value());
    }
    auto compls = std::list< ublkpp::async_result >();
    raid_device.collect_async(nullptr, compls);
    EXPECT_EQ(compls.size(), 1);
    compls.clear();

    // Subsequent reads should not go to device B
    auto ublk_data = make_io_data(UBLK_IO_OP_READ);
    EXPECT_CALL(*device_a, async_iov(UBLK_IO_OP_READ, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());
    // expect unmount_clean on Device A
    EXPECT_TO_WRITE_SB(device_a);
}
// Test basic R/W on the Raid1Disk::sync_io
TEST(Raid1, SyncIoSuccess) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto test_op = UBLK_IO_OP_READ;
    auto test_off = 8 * Ki;
    auto test_sz = 12 * Ki;

    // Reads will only go to device_a at start
    EXPECT_SYNC_OP(test_op, device_a, false, test_sz, test_off + reserved_size);

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    EXPECT_EQ(test_sz, res.value());

    test_op = UBLK_IO_OP_WRITE;
    test_off = 1024 * Ki;
    test_sz = 16 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, false, test_sz, test_off + reserved_size);
    EXPECT_SYNC_OP(test_op, device_b, false, test_sz, test_off + reserved_size);

    res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    EXPECT_EQ(test_sz, res.value());

    // expect unmount_clean on devices
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

TEST(Raid1, SyncIoWriteFailA) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + reserved_size); // Fail this write
    EXPECT_CALL(*device_b, sync_iov(test_op, _, _, _))
        .Times(3)
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::SuperBlock::SIZE); // Expect write to bitmap!
            EXPECT_LT(addr, reserved_size);                   // Expect write to bitmap!
            return iov->iov_len;
        })
        .WillOnce([test_sz](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ(test_off + reserved_size, addr);
            return iov->iov_len;
        });

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    // No need to re-write on A side
    EXPECT_EQ(test_sz, res.value());

    // expect unmount_clean on Device B
    EXPECT_TO_WRITE_SB(device_b);
}

TEST(Raid1, SyncIoWriteFailB) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_CALL(*device_a, sync_iov(test_op, _, _, _))
        .Times(3)
        .WillOnce([test_off, test_sz](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ(test_off + reserved_size, addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::SuperBlock::SIZE); // Expect write to bitmap!
            EXPECT_LT(addr, reserved_size);                   // Expect write to bitmap!
            return iov->iov_len;
        });
    EXPECT_SYNC_OP(test_op, device_b, true, test_sz, test_off + reserved_size);

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    // No need to re-write on A side
    EXPECT_EQ(test_sz, res.value());

    // expect unmount_clean on Device A
    EXPECT_TO_WRITE_SB(device_a);
}

TEST(Raid1, BITMAPMultiWordUpdate) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = ((32 * Ki) * 62) + (4 * Ki);
    auto const test_sz = (32 * Ki) * 5;

    EXPECT_CALL(*device_a, sync_iov(test_op, _, _, _))
        .Times(3)
        .WillOnce([test_off, test_sz](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ(test_off + reserved_size, addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::SuperBlock::SIZE); // Expect write to bitmap!
            EXPECT_LT(addr, reserved_size);                   // Expect write to bitmap!
            return iov->iov_len;
        });
    EXPECT_SYNC_OP(test_op, device_b, true, test_sz, test_off + reserved_size);

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    // No need to re-write on A side
    EXPECT_EQ(test_sz, res.value());

    // expect unmount_clean on Device A
    EXPECT_TO_WRITE_SB(device_a);
}

// Test the failure to update the BITMAP but not the SB header itself
TEST(Raid1, BITMAPUpdateFail) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        auto const test_op = UBLK_IO_OP_WRITE;
        auto const test_off = 8 * Ki;
        auto const test_sz = 12 * Ki;

        EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + reserved_size);
        EXPECT_CALL(*device_b, sync_iov(test_op, _, _, _))
            .Times(2)
            .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
                EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
                EXPECT_EQ(0UL, addr);
                return iov->iov_len;
            })
            .WillOnce([](uint8_t, iovec*, uint32_t, off_t addr) -> io_result {
                EXPECT_GE(addr, ublkpp::raid1::SuperBlock::SIZE); // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);                   // Expect write to bitmap!
                return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
            });

        auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
        ASSERT_FALSE(res);
    }

    // Subsequent reads should not go to device A
    auto ublk_data = make_io_data(UBLK_IO_OP_READ);
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*device_b, async_iov(UBLK_IO_OP_READ, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    EXPECT_TO_WRITE_SB(device_b);
}

// This test fails the initial sync_io to the working device and then fails the SB update to dirty the bitmap on
// the replica. The I/O should fail in this case.
TEST(Raid1, SyncIoWriteFailDirty) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + reserved_size);
    EXPECT_SB_OP(test_op, device_b, true);

    ASSERT_FALSE(raid_device.sync_io(UBLK_IO_OP_WRITE, nullptr, test_sz, test_off));

    // Even though I/O failed, the status is still OK since devices are in same state pre-I/O
    // expect attempt to sync on last working disk
    EXPECT_TO_WRITE_SB_F(device_a, true);
    EXPECT_TO_WRITE_SB_F(device_b, true);
}

// Fail a write that crosses pages on the bitmap dirtying two pages
TEST(Raid1, WriteFailAcrossPages) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = (1 * Gi) - (4 * Ki);
    auto const test_sz = 40 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + reserved_size); // Fail this write
    EXPECT_CALL(*device_b, sync_iov(test_op, _, _, _))
        .Times(4)
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::SuperBlock::SIZE); // Expect write to bitmap!
            EXPECT_LT(addr, reserved_size);                   // Expect write to bitmap!
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::SuperBlock::SIZE); // Expect write to bitmap!
            EXPECT_LT(addr, reserved_size);                   // Expect write to bitmap!
            return iov->iov_len;
        })
        .WillOnce([test_sz](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ(test_off + reserved_size, addr);
            return iov->iov_len;
        });

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    // No need to re-write on A side
    EXPECT_EQ(test_sz, res.value());

    // expect unmount_clean on Device B
    EXPECT_TO_WRITE_SB(device_b);
}

// This test fails the initial WRITE sync_io to the working device and then succeeds the SB update to dirty the bitmap
// on the replica, however the WRITE fails on the replica. The device *IS* degraded after this.
TEST(Raid1, SyncIoWriteFailBoth) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + reserved_size);
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(3)
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::SuperBlock::SIZE); // Expect write to bitmap!
            EXPECT_LT(addr, reserved_size);                   // Expect write to bitmap!
            return iov->iov_len;
        })
        .WillOnce([test_off, test_sz](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ(test_off + reserved_size, addr);
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        });

    ASSERT_FALSE(raid_device.sync_io(UBLK_IO_OP_WRITE, nullptr, test_sz, test_off));

    // expect attempt to sync on last working disk
    EXPECT_TO_WRITE_SB_F(device_b, true);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
