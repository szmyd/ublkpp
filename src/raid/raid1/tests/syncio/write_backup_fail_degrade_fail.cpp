// Regression: active write succeeds, backup write fails, and the subsequent __become_degraded
// SB write to the active device also fails. The I/O must return resource_unavailable_try_again
// -- the write reached the active device but degradation is not yet durable on disk.

#include "test_raid1_common.hpp"

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::StrictMock;

TEST(Raid1, SyncIoWriteBackupFailDegradeFail) {
    auto raw_a = std::make_shared< StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto raw_b = std::make_shared< StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    // Normal healthy superblock on both sides for init.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) {
                memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                static_cast< ublkpp::raid1::SuperBlock* >(iov->iov_base)->fields.device_b = 1;
            }
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), raw_a, raw_b);
    raid_device.toggle_resync(false);

    auto const test_sz = 12 * Ki;
    auto const test_off = 8 * Ki;

    // Active (raw_a) data write succeeds; subsequent calls (destructor bitmap pages) also succeed.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Ne((off_t)0)))
        .WillOnce([test_sz](uint8_t, iovec*, uint32_t, off_t) -> io_result { return test_sz; })
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
    // __become_degraded writes the degraded SB to raw_a at offset 0 -- fail it. Destructor retries succeed.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    iovec iov{nullptr, test_sz};
    auto const res = raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, test_off);
    ASSERT_FALSE(res);
    EXPECT_EQ(std::errc::resource_unavailable_try_again, res.error());
}
