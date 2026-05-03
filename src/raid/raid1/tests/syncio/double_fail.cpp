#include "test_raid1_common.hpp"

// Fails write on device_a (active). SB dirty and bitmap dirty succeed on device_b. Then the WRITE
// on device_b (backup) also fails. The device IS fully degraded after this.
TEST(Raid1, SyncIoWriteFailBoth) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, false, true, test_sz, test_off + raid_device.reserved_size());
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            // __become_degraded writes the SB to device_b first
            EXPECT_EQ(ublkpp::raid1::k_page_size, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([test_off, test_sz, &raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            // Then sync_iov issues the backup data write (which also fails)
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ((off_t)(test_off + raid_device.reserved_size()), addr);
            return std::unexpected(std::make_error_condition(std::errc::io_error));
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
    ASSERT_FALSE(raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, test_off));
    // expect attempt to sync on last working disk
    EXPECT_TO_WRITE_SB_F(device_b, true);
    // Destructor sync_to() writes dirty bitmap pages to the active device (highest LIFO — handles addr>0)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(1)
        .WillOnce([&raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_LT(addr, (off_t)raid_device.reserved_size());
            return iov->iov_len;
        });
}
