#include "test_raid1_common.hpp"

// Brief: Identical to ReadRetryA but for Device B.
TEST(Raid1, ReadRetryB) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                     uint64_t addr) {
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            // It should also have the RETRIED bit set
            EXPECT_TRUE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(iovecs->iov_len, 64 * Ki);
            EXPECT_EQ(addr, (32 * Ki) + reserved_size);
            return 1;
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

    auto ublk_data = make_io_data(UBLK_IO_OP_READ);
    auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101}, ublkpp::sub_cmd_flags::RETRIED);
    auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 64 * Ki, 32 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
