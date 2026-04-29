// Regression: degraded array + dirty bitmap region + active device read failure must return
// EAGAIN (transient, kernel can retry), not fall through to the backup which holds stale data.

#include "test_raid1_common.hpp"

using ::testing::_;
using ::testing::NiceMock;

TEST(Raid1, SyncIoReadDegradedDirtyActiveFailNoFallback) {
    auto raw_a = std::make_shared< NiceMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto raw_b = std::make_shared< NiceMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    // Degraded superblock: route=DEVA, unclean shutdown → __init_bitmap_and_degraded_route
    // calls dirty_region(0, capacity()) without going through __become_degraded, so
    // backup_dev->unavail stays clear (backup is reachable but has stale data).
    auto degraded_sb = normal_superblock;
    degraded_sb.fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVA);
    degraded_sb.fields.clean_unmount = 0;
    degraded_sb.fields.bitmap.age = htobe64(2);

    ON_CALL(*raw_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillByDefault([degraded_sb](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &degraded_sb, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    ON_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillByDefault([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
    ON_CALL(*raw_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillByDefault([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) {
                memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                auto* sb = static_cast< ublkpp::raid1::SuperBlock* >(iov->iov_base);
                sb->fields.device_b = 1;
                sb->fields.bitmap.age = htobe64(1);
            }
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), raw_a, raw_b);

    // Confirm: degraded, bitmap fully dirty, backup not marked unavail.
    ASSERT_GT(raid_device.replica_states().bytes_to_sync, 0u);

    // Stop resync to prevent it racing with the EXPECT_CALLs below.
    raid_device.toggle_resync(false);

    // Allow destructor writes to raw_a (bitmap pages + SB) without over-saturating the READ expectation.
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _)).Times(testing::AnyNumber());
    // Active device fails the user-data read; backup must not be touched (it has stale data).
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_READ, _, _, _)).WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(0);

    iovec iov{nullptr, 4 * Ki};
    auto const res = raid_device.sync_iov(UBLK_IO_OP_READ, &iov, 1, 0);
    ASSERT_FALSE(res);
    EXPECT_EQ(std::errc::resource_unavailable_try_again, res.error());
}
