#include "test_raid1_common.hpp"

TEST(Raid1, SyncIoWriteFailB) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_CALL(*device_a, sync_iov(test_op, _, _, _))
        .Times(2)
        .WillOnce([test_off, test_sz, &raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ((off_t)(test_off + raid_device.reserved_size()), addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(ublkpp::raid1::k_page_size, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        });
    EXPECT_SYNC_OP(test_op, device_b, true, true, test_sz, test_off + raid_device.reserved_size());

    iovec iov{nullptr, test_sz};
    auto res = raid_device.sync_iov(test_op, &iov, 1, test_off);
    ASSERT_TRUE(res);
    // No need to re-write on A side
    EXPECT_EQ((int)test_sz, res.value());

    // Keep device_b permanently unavailable so the resync task (which launched inside
    // __become_degraded) cannot clear its unavail flag via probe_mirror and write extra bitmap pages
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
    // expect unmount_clean on Device A
    EXPECT_TO_WRITE_SB(device_a);
    // Destructor sync_to() writes dirty bitmap pages to the active device (highest LIFO — handles addr>0)
    EXPECT_CALL(*device_a, sync_iov(test_op, _, _, testing::Gt((off_t)0)))
        .Times(1)
        .WillOnce([&raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_LT(addr, (off_t)raid_device.reserved_size());
            return iov->iov_len;
        });
}
