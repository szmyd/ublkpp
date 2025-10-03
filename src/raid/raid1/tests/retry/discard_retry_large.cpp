#include "test_raid1_common.hpp"

// Dirty an entire multi-page bitmap from a single discard operation
TEST(Raid1, LargeDiscardRetry) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = 2 * Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = 2 * Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

    auto ublk_data = make_io_data(UBLK_IO_OP_DISCARD, 2 * Gi, 0UL);
    auto const retried_route =
        ublkpp::set_flags(ublkpp::sub_cmd_t{0b101}, ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATE);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, retried_route);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(0, res.value());

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size); // Expect write to bitmap!
            EXPECT_LT(addr, reserved_size);              // Expect write to bitmap!
            return iov->iov_len;
        })
        .RetiresOnSaturation();
}
