#include "test_raid1_common.hpp"

// Retry write that failed on DeviceA and check that a failure to update the SB is terminal
TEST(Raid1, WriteRetryAFailure) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB_F(device_b, true);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // Queued Retries should attempt to update the SB as well
    {
        EXPECT_TO_WRITE_SB_F(device_b, true);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // expect unmount_clean attempt on both devices
    EXPECT_TO_WRITE_SB_F(device_a, true);
    EXPECT_TO_WRITE_SB_F(device_b, true);
}
