#include "test_raid1_common.hpp"

// Test the failure to update the BITMAP but not the SB header itself
TEST(Raid1, BITMAPUpdateFail) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    {
        auto const test_op = UBLK_IO_OP_WRITE;
        auto const test_off = 8 * Ki;
        auto const test_sz = 12 * Ki;

        EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + reserved_size);
        EXPECT_CALL(*device_b, sync_iov(test_op, _, _, _))
            .Times(2)
            .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
                EXPECT_EQ(ublkpp::raid1::k_page_size, iov->iov_len);
                EXPECT_EQ(0UL, addr);
                return iov->iov_len;
            })
            .WillOnce([](uint8_t, iovec*, uint32_t, off_t addr) -> io_result {
                EXPECT_GE(addr, ublkpp::raid1::k_page_size); // Expect write to bitmap!
                EXPECT_LT(addr, reserved_size);              // Expect write to bitmap!
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
            EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
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
