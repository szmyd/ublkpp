#include "../test_raid1_common.hpp"

#include <isa-l/mem_routines.h>

// Test: replica_states when both devices are healthy
TEST(Raid1, ReplicaStatesHealthy) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto states = raid_device.replica_states();

    // Both should be clean (no errors, not syncing)
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_EQ(states.bytes_to_sync, 0);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: replicas() returns both devices
TEST(Raid1, ReplicasAccess) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto [replica_a, replica_b] = raid_device.replicas();

    EXPECT_EQ(replica_a, device_a);
    EXPECT_EQ(replica_b, device_b);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: toggle_resync disables resync
TEST(Raid1, ToggleResyncDisable) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Disable resync
    raid_device.toggle_resync(false);

    // Re-enable resync
    raid_device.toggle_resync(true);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: id() returns RAID1
TEST(Raid1, IdMethod) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    EXPECT_EQ(raid_device.id(), "RAID1");

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: route_size returns 1
TEST(Raid1, RouteSize) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    EXPECT_EQ(raid_device.route_size(), 1);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Helper: set up new_device mock expectations for a successful swap of device A.
// After this swap: route=DEVB, new_device is in A slot (backup, unavail=false), device_b is active.
// Caller must register the device_b staying-write and unmount expectations separately.
static void expect_swap_a_success(std::shared_ptr< ublkpp::TestDisk >& new_device,
                                  std::shared_ptr< ublkpp::TestDisk >& device_b, ublkpp::Raid1Disk& raid_device) {
    // new device read returns all-zeros → new_device=true → init_to is called
    EXPECT_CALL(*new_device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 0x00, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*new_device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([&raid_device](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // init_to: bitmap write to new device
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_LT(addr, raid_device.reserved_size());
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // all zeros
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // __swap_device: new device superblock commit (new_device is in A slot → device_b=0)
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            EXPECT_EQ(0, static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b);
            return ublkpp::raid1::k_page_size;
        });

    // staying device (device_b) superblock write in __swap_device
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(ublkpp::raid1::read_route::DEVB,
                      static_cast< ublkpp::raid1::read_route >(
                          reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.read_route));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });
}

// Test: replica_states DEVB case — backup device (A slot) is available → SYNCING (lines 467-469)
TEST(Raid1, ReplicaStatesSyncingDEVB) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    auto new_device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});
    expect_swap_a_success(new_device, device_b, raid_device);

    // Swap succeeds; returns outgoing device_a
    EXPECT_EQ(raid_device.swap_device("DiskA", new_device), device_a);

    // Route is DEVB: backup_dev = new_device (unavail=false) → SYNCING
    auto states = raid_device.replica_states();
    EXPECT_EQ(states.device_a, ublkpp::raid1::replica_state::SYNCING);
    EXPECT_EQ(states.device_b, ublkpp::raid1::replica_state::CLEAN);
    EXPECT_GT(states.bytes_to_sync, 0UL);

    // Unmount: degraded → bitmap sync_to + SB write to active device (device_b) only
    EXPECT_TO_WRITE_SB(device_b);
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([&raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_LT(addr, raid_device.reserved_size());
            EXPECT_NE(0, isal_zero_detect(iov->iov_base, ublkpp::raid1::k_page_size)); // all ones (dirty)
            return iov->iov_len;
        })
        .RetiresOnSaturation();
}

// Test: replicas() DEVB case — returns (backup_dev, active_dev) = (new_device, device_b) (line 487)
TEST(Raid1, ReplicasAccessDEVB) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    auto new_device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});
    expect_swap_a_success(new_device, device_b, raid_device);

    // Swap succeeds; returns outgoing device_a
    EXPECT_EQ(raid_device.swap_device("DiskA", new_device), device_a);

    // Route is DEVB: replicas() returns (backup_dev->disk, active_dev->disk) = (new_device, device_b)
    auto [replica_a, replica_b] = raid_device.replicas();
    EXPECT_EQ(replica_a, new_device);
    EXPECT_EQ(replica_b, device_b);

    // Unmount: degraded → bitmap sync_to + SB write to active device (device_b) only
    EXPECT_TO_WRITE_SB(device_b);
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillOnce([&raid_device](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_LT(addr, raid_device.reserved_size());
            EXPECT_NE(0, isal_zero_detect(iov->iov_base, ublkpp::raid1::k_page_size)); // all ones (dirty)
            return iov->iov_len;
        })
        .RetiresOnSaturation();
}
