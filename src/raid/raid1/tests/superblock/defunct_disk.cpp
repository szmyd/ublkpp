#include "test_raid1_common.hpp"

// If we initialize with one new device and one defunct, take the working device as is
TEST(Raid1, DefunctDiskB) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::DefunctDisk >();
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Only update the SuperBlock, no writes to BITMAP
    EXPECT_TO_WRITE_SB(device_a);
}

TEST(Raid1, DefunctDiskA) {
    auto device_a = std::make_shared< ublkpp::DefunctDisk >();
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Only update the SuperBlock, no writes to BITMAP
    EXPECT_TO_WRITE_SB(device_b);
}

// Regression: on remount, a persisted read_route must be preserved when one device is still defunct.
// Previously, __init_bitmap_and_degraded_route used role-relative active_dev->new_device to choose
// the route — which inverted to the defunct slot on remount, causing a fatal superblock write error.
TEST(Raid1, DefunctRemountA) {
    // device_b returns the superblock as persisted from the first degraded mount:
    // read_route=DEVB, device_b=1, clean_unmount=1 (i.e. device_a was defunct last time too)
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .is_slot_b = true});
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVB);
            sb->fields.device_b = 1;
            return ublkpp::raid1::k_page_size;
        });
    // __become_active must write to device_b (the live slot), not to device_a (defunct)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVB,
                      static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            return ublkpp::raid1::k_page_size;
        });

    auto device_a = std::make_shared< ublkpp::DefunctDisk >();
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    EXPECT_TO_WRITE_SB(device_b);
}

TEST(Raid1, DefunctRemountB) {
    // device_a returns the superblock as persisted from the first degraded mount:
    // read_route=DEVA, device_b=0, clean_unmount=1 (i.e. device_b was defunct last time too)
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .is_slot_b = false});
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVA);
            sb->fields.device_b = 0;
            return ublkpp::raid1::k_page_size;
        });
    // __become_active must write to device_a (the live slot), not to device_b (defunct)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA,
                      static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            return ublkpp::raid1::k_page_size;
        });

    auto device_b = std::make_shared< ublkpp::DefunctDisk >();
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    EXPECT_TO_WRITE_SB(device_a);
}

// We should throw if both devices are Defunct
TEST(Raid1, DefunctDisks) {
    auto device_a = std::make_shared< ublkpp::DefunctDisk >();
    auto device_b = std::make_shared< ublkpp::DefunctDisk >();
    EXPECT_THROW(ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}
