#include "test_raid1_common.hpp"

TEST(Raid1, SyncIoWriteFailA) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, false, true, test_sz, test_off + raid_device.reserved_size()); // Fail this write
    EXPECT_CALL(*device_b, sync_iov(test_op, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            // __become_degraded writes the SB to device_b first
            EXPECT_EQ(ublkpp::raid1::k_page_size, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([test_sz, test_off, &raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            // Then sync_iov issues the backup data write
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ((off_t)(test_off + raid_device.reserved_size()), addr);
            return iov->iov_len;
        });

    // Keep device_a permanently unavailable so the resync task (which launched inside
    // __become_degraded) cannot clear its unavail flag via probe_mirror and write extra bitmap pages.
    // Must be registered before sync_iov() fires __become_degraded, or the probe thread (under TSan
    // overhead) saturates the Times(1) SB-read expectation set by CREATE_DISK_A.
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    iovec iov{nullptr, test_sz};
    auto res = raid_device.sync_iov(test_op, &iov, 1, test_off);
    ASSERT_TRUE(res);
    // No need to re-write on A side
    EXPECT_EQ((int)test_sz, res.value());
    // expect unmount_clean on Device B
    EXPECT_TO_WRITE_SB(device_b);
    // Destructor sync_to() writes dirty bitmap pages to the active device (highest LIFO — handles addr>0)
    EXPECT_CALL(*device_b, sync_iov(test_op, _, _, testing::Gt((off_t)0)))
        .Times(1)
        .WillOnce([&raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_LT(addr, (off_t)raid_device.reserved_size());
            return iov->iov_len;
        });
}
