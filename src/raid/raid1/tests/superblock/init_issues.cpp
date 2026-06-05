#include "test_raid1_common.hpp"

// Brief: If either devices should not load/write superblocks correctly, initialization should throw
TEST(Raid1, ReadingSBProblems) {
    // Fail Read SB from DevA
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, true, false, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, true, true, false, false);
        EXPECT_THROW(auto raid_device =
                         ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                     std::runtime_error);
    }
    // Fail Read SB from DevB
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, false, true, true, false);
        EXPECT_THROW(auto raid_device =
                         ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                     std::runtime_error);
    }
    // Fail Read SB from Both
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, true, true, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, true, true, true, false);
        EXPECT_THROW(auto raid_device =
                         ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                     std::runtime_error);
    }
    // Should not throw just dirty SB and pages
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, false, true);
        auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
        auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
        raid_device.toggle_resync(false);
        // expect unmount_clean update
        EXPECT_TO_WRITE_SB(device_b);
    }

    // Should not throw just dirty SB
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, false, false, false, true);
        // Expect an extra WRITE to the SB when sync'ing the SB to DevB fails
        EXPECT_SYNC_OP_REPEAT(UBLK_IO_OP_WRITE, 2, device_a, false, false, ublkpp::raid1::k_page_size, 0UL);
        auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
        raid_device.toggle_resync(false);
        // expect unmount_clean update
        EXPECT_TO_WRITE_SB(device_a);
    }

    // Fail writing both SBs
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, false, true);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, false, false, false, true);
        EXPECT_THROW(auto raid_device =
                         ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                     std::runtime_error);
    }

    // Fail Second Update to DevA
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, false, false, false, true);
        // Expect an extra WRITE to the SB when sync'ing the SB to DevB fails
        EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .Times(2)
            .WillOnce([](uint8_t, iovec* iov, uint32_t nr_vecs, off_t addr) -> io_result {
                EXPECT_EQ(1, nr_vecs);
                EXPECT_EQ(ublkpp::raid1::k_page_size, iov->iov_len);
                EXPECT_EQ(0UL, addr);
                return iov->iov_len;
            })
            .WillOnce([](uint8_t, iovec* iov, uint32_t nr_vecs, off_t addr) -> io_result {
                EXPECT_EQ(1, nr_vecs);
                EXPECT_EQ(ublkpp::raid1::k_page_size, iov->iov_len);
                EXPECT_EQ(0UL, addr);
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });
        EXPECT_THROW(auto raid_device =
                         ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                     std::runtime_error);
    }
}

// The race between resync completing and stop() in the destructor can produce an on-disk state of
// DEVB + clean_unmount=1 + empty superbitmap. Before the fix this threw on second mount; after the
// fix the constructor recovers with dirty-all and the destructor writes EITHER to both devices.
TEST(Raid1, DegradedCleanEmptySuperbitmap) {
    using ublkpp::raid1::read_route;
    using ublkpp::raid1::SuperBlock;

    // disk_b carries the race-state SB: DEVB, age=1 (wins pick_superblock), clean=1, empty superbitmap.
    SuperBlock sb_b = normal_superblock;
    sb_b.fields.read_route = static_cast< uint8_t >(read_route::DEVB);
    sb_b.fields.device_b = 1;
    sb_b.fields.bitmap.age = htobe64(1); // age_b > age_a=0 so disk_b wins
    memset(sb_b.superbitmap_reserved, 0, sizeof(sb_b.superbitmap_reserved));

    auto device_a = std::make_shared< testing::StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto device_b =
        std::make_shared< testing::StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    // disk_a: standard EITHER SB on read; two writes — __become_active (DEVB) then destructor fix (EITHER).
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, 0UL))
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return iov->iov_len;
        });
    ublkpp::raid1::read_route last_route_a{read_route::DEVB};
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, 0UL))
        .Times(2)
        .WillRepeatedly([&last_route_a](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            last_route_a = static_cast< read_route >(static_cast< SuperBlock* >(iov->iov_base)->fields.read_route);
            return iov->iov_len;
        });

    // disk_b: race-state SB on read; one bitmap-page read (load_from, returns zeros to clear the
    // superbitmap bit that dirty_region set); two writes — __become_active (DEVB) then destructor fix (EITHER).
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, 0UL))
        .WillOnce([&sb_b](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            memcpy(iov->iov_base, &sb_b, ublkpp::raid1::k_page_size);
            return iov->iov_len;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, (off_t)ublkpp::raid1::k_page_size))
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            memset(iov->iov_base, 0, iov->iov_len);
            return iov->iov_len;
        });
    ublkpp::raid1::read_route last_route_b{read_route::DEVB};
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, 0UL))
        .Times(2)
        .WillRepeatedly([&last_route_b](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            last_route_b = static_cast< read_route >(static_cast< SuperBlock* >(iov->iov_base)->fields.read_route);
            return iov->iov_len;
        });

    EXPECT_NO_THROW({
        auto raid = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
        raid.toggle_resync(false);
    });

    EXPECT_EQ(last_route_a, read_route::EITHER);
    EXPECT_EQ(last_route_b, read_route::EITHER);
}
