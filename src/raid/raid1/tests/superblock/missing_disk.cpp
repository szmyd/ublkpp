#include "test_raid1_common.hpp"

// If we initialize with one new device and one missing, take the working device as is
TEST(Raid1, MissingDiskB) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = ublkpp::make_missing_disk();
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Only update the SuperBlock, no writes to BITMAP
    EXPECT_TO_WRITE_SB(device_a);
}

TEST(Raid1, MissingDiskA) {
    auto device_a = ublkpp::make_missing_disk();
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Only update the SuperBlock, no writes to BITMAP
    EXPECT_TO_WRITE_SB(device_b);
}

// Regression: on remount, a persisted read_route must be preserved when one device is still missing.
// Previously, __init_bitmap_and_degraded_route used role-relative active_dev->new_device to choose
// the route - which inverted to the missing slot on remount, causing a fatal superblock write error.
TEST(Raid1, MissingRemountA) {
    // device_b returns the superblock as persisted from the first degraded mount:
    // read_route=DEVB, device_b=1, clean_unmount=1 (i.e. device_a was missing last time too)
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .is_slot_b = true});
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVB);
            sb->fields.device_b = 1;
            return ublkpp::raid1::k_page_size;
        });
    // __become_active must write to device_b (the live slot), not to device_a (missing)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVB, static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            return ublkpp::raid1::k_page_size;
        });

    auto device_a = ublkpp::make_missing_disk();
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    // Set up destructor's clean-unmount write expectation (fires before raid_device leaves scope)
    EXPECT_TO_WRITE_SB(device_b);
}

TEST(Raid1, MissingRemountB) {
    // device_a returns the superblock as persisted from the first degraded mount:
    // read_route=DEVA, device_b=0, clean_unmount=1 (i.e. device_b was missing last time too)
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .is_slot_b = false});
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVA);
            sb->fields.device_b = 0;
            return ublkpp::raid1::k_page_size;
        });
    // __become_active must write to device_a (the live slot), not to device_b (missing)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA, static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            return ublkpp::raid1::k_page_size;
        });

    auto device_b = ublkpp::make_missing_disk();
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    // Set up destructor's clean-unmount write expectation (fires before raid_device leaves scope)
    EXPECT_TO_WRITE_SB(device_a);
}

// Bug M11 regression: on clean shutdown with a missing backup disk, the dirty bitmap must be
// flushed to the active device. The old destructor guard
// "is_degraded && !backup_dev->disk->is_missing()" skipped the flush when backup was a
// missing placeholder, causing the next startup to fall back to a full resync instead of
// using the persisted dirty ranges for an incremental one.
//
// Test strategy: open a degraded array (read_route=DEVA, device_b=missing). Issue a write —
// it succeeds on device_a (active) and fails silently on the missing device_b; the bitmap is
// dirtied. At shutdown the destructor must call sync_to(device_a) even though device_b is
// still the missing placeholder. Bitmap pages live at offsets in [k_page_size, _reserved_size).
// Writes in that range are tracked via a flag; the assertion checks the flag was set.
TEST(Raid1, BitmapFlushedToActiveWhenBackupMissing) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .is_slot_b = false});
    auto device_b = ublkpp::make_missing_disk();

    bool bitmap_sync_written = false;
    constexpr off_t k_bitmap_lo = static_cast< off_t >(ublkpp::raid1::k_page_size);
    constexpr off_t k_bitmap_hi = static_cast< off_t >(64 * 1024 * 1024);

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVA);
            sb->fields.device_b = 0;
            sb->fields.clean_unmount = 1;
            return ublkpp::raid1::k_page_size;
        });

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&bitmap_sync_written, k_bitmap_lo, k_bitmap_hi](uint8_t, iovec* iovecs, uint32_t nr_vecs,
                                                                         off_t addr) -> io_result {
            if (addr >= k_bitmap_lo && addr < k_bitmap_hi) bitmap_sync_written = true;
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });

    {
        auto raid = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
        raid.toggle_resync(false);

        // Write marks a bitmap chunk dirty. The missing backup absorbs the failure silently
        // (already degraded), so __become_degraded is not re-entered.
        iovec iov{nullptr, 32 * Ki};
        std::ignore = raid.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 0);
    }

    EXPECT_TRUE(bitmap_sync_written)
        << "Bug M11: bitmap was not flushed to active device at shutdown when backup is missing";
}

// We should throw if both devices are missing
TEST(Raid1, MissingDisks) {
    auto device_a = ublkpp::make_missing_disk();
    auto device_b = ublkpp::make_missing_disk();
    EXPECT_THROW(ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}
