#include "test_raid1_common.hpp"

// This test fails the initial sync_io to the working device and then fails the SB update to dirty the bitmap on
// the replica. The I/O should fail in this case.
TEST(Raid1, SyncIoWriteFailDirty) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, false, true, test_sz, test_off + raid_device.reserved_size());
    EXPECT_SB_OP(test_op, device_b, true, true);

    auto iov = iovec{.iov_base = nullptr, .iov_len = test_sz};
    ASSERT_FALSE(raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, test_off));

    // device_a is ERROR; device_b is the sole surviving device.
    // Shutdown issues bitmap-page write(s) + SB write to device_b only -- all fail.
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
}
