#include "test_raid1_common.hpp"

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
                                         ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
    }

    // Follow up retry on device A fails without operations on B
    {
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // Follow up on device A fails without operations on B
    {
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // Failure to dirty pages
    {
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
            });
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(2)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return 1;
            })
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 12 * Ki, 256 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }
    // expect unmount_clean on last working device
    EXPECT_TO_WRITE_SB_F(device_a, true);
}
