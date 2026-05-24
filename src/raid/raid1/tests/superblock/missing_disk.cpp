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

// We should throw if both devices are missing
TEST(Raid1, MissingDisks) {
    auto device_a = ublkpp::make_missing_disk();
    auto device_b = ublkpp::make_missing_disk();
    EXPECT_THROW(ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}

// M11 regression: when the array is degraded because the backup is a missing-leg placeholder,
// the destructor must still call sync_to() to persist the dirty bitmap to the active device.
// The old condition was (is_degraded && !backup->is_missing()), which skipped the sync when
// backup was a missing placeholder — causing the next swap-in to fall back to a full resync.
TEST(Raid1, DegradedMissingBackupSyncsBitmapAtShutdown) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = ublkpp::make_missing_disk();

    // SB read during construction (normal_superblock: clean, route=EITHER).
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });

    // SB writes at offset 0: __become_active (clean_unmount=0) + destructor (clean_unmount=1).
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
        .Times(2)
        .WillRepeatedly(
            [](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result { return ublkpp::raid1::k_page_size; });

    // Writes at offset > 0: data write (via sync_iov) + bitmap page write (via sync_to in destructor).
    // The bitmap write is the M11 regression check: it only happens if sync_to() is called even
    // though the backup device is a missing placeholder.
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(2)
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });

    {
        auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

        // Write marks a bitmap page dirty; the failed write to missing_device is silently absorbed
        // (we're already degraded) and dirty_region() is called for the affected LBA range.
        auto iov = iovec{.iov_base = nullptr, .iov_len = 4 * Ki};
        ASSERT_TRUE(raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 0));

        // Destructor: sync_to() writes the dirty bitmap page to device_a (Times(2) above),
        // then the clean-unmount SB write (counted in the offset-0 Times(2) above).
    }
}
