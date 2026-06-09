// Regression: active write succeeds, backup write fails, and the subsequent __become_degraded
// SB write to the active device also fails. The I/O must return -EAGAIN -- the write reached the
// active device but the degradation is not yet durable; a crash would self-heal incorrectly.

#include "async_raid1_common.hpp"

using ::testing::AnyNumber;

TEST_F(AsyncRaid1Fixture, WriteBackupFailDegradeFail) {
    // Catch-all for bitmap-page writes (addr > 0) to disk_a during shutdown; registered first so
    // the addr=0 EXPECT_CALL (registered second) takes LIFO priority for superblock writes.
    EXPECT_CALL(*disk_a, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    // __become_degraded(false) writes the degraded SB to disk_a at offset 0; fail it once.
    // Destructor retry must succeed so the test exits cleanly.
    EXPECT_CALL(*disk_a, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        })
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto res = mock->submit_io(0, UBLK_IO_OP_WRITE, 0, 4 * Ki / 512, nullptr);
    ASSERT_TRUE(res);
    EXPECT_EQ(res.value(), 2u);

    // Active (disk_a) succeeds → coroutine awaits backup.
    EXPECT_TRUE(mock->inject_cqe(0, 4 * Ki).empty());
    // Backup (disk_b) fails → __become_degraded(false) fires → SB write to disk_a fails → -EAGAIN.
    auto completions = mock->inject_cqe(0, -EIO);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].result, -EAGAIN);
}
