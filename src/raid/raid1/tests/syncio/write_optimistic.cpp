// Tests the OPTIMISTIC backup write path in sync_iov (raid1.cpp lines 727, 881-883):
// when a degraded array receives a chunk-aligned write to a dirty bitmap region and
// the backup device has not been marked unavailable, the write goes to both devices
// and calls clean_region to clear the dirty bit in-band.
//
// Setup: load a pre-degraded superblock (read_route=DEVA, clean_unmount=0). The
// "unclean degraded shutdown" path in __init_bitmap_and_degraded_route calls
// dirty_region(0, capacity()) without going through __become_degraded, so
// backup_dev->unavail is never set. No resync task runs, so ResyncWriteGuard is a
// no-op and there is no deadlock risk.

#include "test_raid1_common.hpp"

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::StrictMock;

TEST(Raid1, SyncIoOptimisticWriteCleansBitmap) {
    auto raw_a = std::make_shared< StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto raw_b = std::make_shared< StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    // raw_a SB: route=DEVA, clean_unmount=0, age=2.
    // pick_superblock prefers raw_a (age 2 > 1) and sets read_route=DEVA.
    // __init_bitmap_and_degraded_route hits the (route!=EITHER && clean_unmount==0) branch
    // → dirty_region(0, capacity()) without calling __become_degraded → unavail stays clear.
    auto degraded_sb = normal_superblock;
    degraded_sb.fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVA);
    degraded_sb.fields.clean_unmount = 0;
    degraded_sb.fields.bitmap.age = htobe64(2);

    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([degraded_sb](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &degraded_sb, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });

    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) {
                memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                auto* sb = static_cast< ublkpp::raid1::SuperBlock* >(iov->iov_base);
                sb->fields.device_b = 1;
                sb->fields.bitmap.age = htobe64(1); // older than raw_a -> raw_a wins
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

    // Array is degraded (active=raw_a), bitmap fully dirty, raw_b->unavail=false,
    // no resync running → ResyncWriteGuard early-exits (state=IDLE).
    ASSERT_GT(raid_device.replica_states().bytes_to_sync, 0u);
    auto const initial_bytes = raid_device.replica_states().bytes_to_sync;

    // OPTIMISTIC write — chunk-aligned, degraded, dirty region, backup available.
    // __compute_backup_mode returns OPTIMISTIC → writes active + backup, then calls clean_region.
    static constexpr uint32_t chunk_sz = 32 * Ki;
    iovec iov{nullptr, chunk_sz};
    ASSERT_TRUE(raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 0));

    // clean_region wrote the bitmap page to the active device → dirty bit cleared for first chunk.
    EXPECT_LT(raid_device.replica_states().bytes_to_sync, initial_bytes);
}
