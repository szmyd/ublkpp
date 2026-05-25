// Regression (replaces the deleted OPTIMISTIC path): in degraded mode with a *reachable* backup
// (unavail clear) but a dirty target region, a write must route active-only and leave the region
// dirty. The resync task is the sole authority on cleaning -- the I/O path never writes the backup
// for a dirty region, even when that backup is online.

#include "test_raid1_common.hpp"

using ::testing::_;
using ::testing::NiceMock;

TEST(Raid1, SyncIoWriteDegradedDirtySkipsAvailableBackup) {
    auto raw_a = std::make_shared< NiceMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto raw_b = std::make_shared< NiceMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    // Degraded superblock: route=DEVA, unclean shutdown -> __init_bitmap_and_degraded_route
    // calls dirty_region(0, capacity()) without going through __become_degraded, so
    // backup_dev->unavail stays clear (backup is reachable but holds stale data).
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
    // Superblock/bitmap writes (addr < reserved_size) to the stale backup are permitted; the
    // strict expectation below only forbids *data* writes to the backup.
    ON_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillByDefault([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), raw_a, raw_b);

    // Stop resync so it does not race the write or clean the region under us.
    raid_device.toggle_resync(false);

    // Confirm preconditions: degraded, fully dirty, backup reachable (not unavail/error).
    auto const bytes_before = raid_device.replica_states().bytes_to_sync;
    ASSERT_GT(bytes_before, 0u);

    // The dirty-region write must reach the active device (disk_a) but NOT the backup (disk_b).
    // Data writes land at addr >= reserved_size; SB/bitmap writes (addr < reserved_size) are
    // allowed via the ON_CALL above.
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Ge((off_t)raid_device.reserved_size()))).Times(0);

    auto const test_sz = 4 * Ki;
    iovec iov{nullptr, test_sz};
    auto const res = raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 0);
    ASSERT_TRUE(res);
    EXPECT_EQ(test_sz, static_cast< size_t >(res.value()));

    // Region remains dirty -- the active-only write makes no resync progress.
    EXPECT_EQ(bytes_before, raid_device.replica_states().bytes_to_sync);
}
