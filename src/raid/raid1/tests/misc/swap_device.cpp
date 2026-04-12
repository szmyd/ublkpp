#include "test_raid1_common.hpp"

#include <isa-l/mem_routines.h>
#include <thread>

/// Test the ability to swap one device for another
TEST(Raid1, SwapDeviceB) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0, memcmp(&normal_superblock, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            auto superblock = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA,
                      static_cast< ublkpp::raid1::read_route >(superblock->fields.read_route));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });

    auto new_device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});
    EXPECT_CALL(*new_device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*new_device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([&raid_device](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);                                  // Expect write to bitmap!
            EXPECT_LT(addr, raid_device.reserved_size());                                 // Expect write to bitmap!
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All zeros
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            EXPECT_EQ(0, memcmp(&normal_superblock, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            EXPECT_EQ(1, static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b);
            return ublkpp::raid1::k_page_size;
        });

    auto old_device = raid_device.swap_device("DiskB", new_device);
    EXPECT_EQ(old_device, device_b);

    old_device = raid_device.swap_device("DiskA", device_b);
    EXPECT_EQ(old_device, device_b);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([&raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);                               // Expect write to bitmap!
            EXPECT_LT(addr, raid_device.reserved_size());                              // Expect write to bitmap!
            EXPECT_NE(0, isal_zero_detect(iov->iov_base, ublkpp::raid1::k_page_size)); // All ones
            return iov->iov_len;
        })
        .RetiresOnSaturation();
}

TEST(Raid1, SwapDeviceA) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0, memcmp(&normal_superblock, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            auto superblock = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVB,
                      static_cast< ublkpp::raid1::read_route >(superblock->fields.read_route));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });

    auto small_device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Mi, .id = "DiskD"});
    auto old_device = raid_device.swap_device("DiskA", small_device);
    EXPECT_EQ(old_device, small_device);

    auto new_device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});
    EXPECT_CALL(*new_device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*new_device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([&raid_device](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);                                  // Expect write to bitmap!
            EXPECT_LT(addr, raid_device.reserved_size());                                 // Expect write to bitmap!
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All zeros
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            EXPECT_EQ(0, memcmp(&normal_superblock, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            EXPECT_EQ(0, static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b);
            return ublkpp::raid1::k_page_size;
        });

    old_device = raid_device.swap_device("DiskA", new_device);
    EXPECT_EQ(old_device, device_a);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_b);

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([&raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);                               // Expect write to bitmap!
            EXPECT_LT(addr, raid_device.reserved_size());                              // Expect write to bitmap!
            EXPECT_NE(0, isal_zero_detect(iov->iov_base, ublkpp::raid1::k_page_size)); // All ones
            return iov->iov_len;
        })
        .RetiresOnSaturation();
}

TEST(Raid1, SwapStayingWriteFail) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    auto new_device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});
    // New device read returns a valid superblock with matching age → not a new device → no bitmap write
    EXPECT_CALL(*new_device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });

    // Staying device (device_b) write fails — triggers rollback path (lines 310-313)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    EXPECT_EQ(raid_device.swap_device("DiskA", new_device), new_device);

    // Rollback must have restored the original device pointers and EITHER route
    auto [replica_a, replica_b] = raid_device.replicas();
    EXPECT_EQ(replica_a, device_a);
    EXPECT_EQ(replica_b, device_b);

    // expect unmount_clean update (not degraded → writes to both)
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: swap_device while idle probes are running stops the stale probe jthreads (Bug 2 fix).
// Without the fix, _idle_probe_b would continue holding the outgoing MirrorDevice and
// call sync_iov on it ~avail_delay seconds after the swap, causing an unexpected-call
// failure and a potential use-after-free.
TEST(Raid1, SwapDeviceWhileIdle) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Enter idle — launches _idle_probe_a (on device_a) and _idle_probe_b (on device_b).
    // Both devices are CLEAN so immediate_probe skips (no sync_iov). Probes sleep avail_delay.
    raid_device.idle_transition(nullptr, true);

    // Swap device_b for a new device. swap_device must stop both probe jthreads so that
    // _idle_probe_b (which captured device_b's MirrorDevice) cannot probe the outgoing device.
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            auto const* sb = reinterpret_cast< ublkpp::raid1::SuperBlock const* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA, static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });

    auto new_device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});
    EXPECT_CALL(*new_device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*new_device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([&raid_device](uint8_t, iovec*, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size); // bitmap
            EXPECT_LT(addr, raid_device.reserved_size());
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(0UL, addr); // superblock
            return ublkpp::raid1::k_page_size;
        });

    auto old_device = raid_device.swap_device("DiskB", new_device);
    EXPECT_EQ(old_device, device_b);

    // Degraded unmount: bitmap sync + SB write to device_a only.
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([&raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size); // bitmap
            EXPECT_LT(addr, raid_device.reserved_size());
            EXPECT_NE(0, isal_zero_detect(iov->iov_base, ublkpp::raid1::k_page_size)); // all ones
            return iov->iov_len;
        })
        .RetiresOnSaturation();
}

TEST(Raid1, SwapFail) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    auto new_device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});
    EXPECT_CALL(*new_device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
    EXPECT_EQ(raid_device.swap_device("DiskA", new_device), new_device);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
