#include "test_raid1_common.hpp"

using namespace std::chrono_literals;

// Test 1: Try to swap a device that's not part of the array
TEST(Raid1, SwapUnrecognizedDevice) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi}));
    // device_c is not initialized into a RAID, so don't use CREATE_DISK macro
    auto device_c = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "device_c"});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Try to swap a device that's not in the array (should fail and return incoming device)
    // This covers line 330: refusing to replace unrecognized mirror
    auto result = raid_device.swap_device("unknown_device_id", device_c);

    // Should return the incoming device unchanged (swap refused)
    EXPECT_EQ(result->id(), device_c->id());

    // Cleanup expectations
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test 2: Try to swap a device with itself (already in array)
TEST(Raid1, SwapDeviceAlreadyInArray) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Try to swap device_a with itself (should fail and return incoming device)
    // This covers line 333: device already in array, nothing to do
    auto result = raid_device.swap_device(device_a->id(), device_a);

    // Should return device_a unchanged (swap refused)
    EXPECT_EQ(result->id(), device_a->id());

    // Cleanup expectations
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test 3: Both devices assigned same slot (initialization error)
TEST(Raid1, BothDevicesSameSlot) {
    // Create devices without setting up any expectations
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    // Set up superblocks where both devices think they're device_b
    // This should trigger the exception at line 169
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            if (nullptr != iovecs->iov_base) {
                memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                // Set device_b = 1 (this is device B)
                static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b = 1;
            }
            return ublkpp::__iovec_len(iovecs, iovecs + 1);
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            if (nullptr != iovecs->iov_base) {
                memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                // ALSO set device_b = 1 (this is ALSO device B - invalid!)
                static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b = 1;
            }
            return ublkpp::__iovec_len(iovecs, iovecs + 1);
        });

    // Should throw runtime_error: "Found both devices were assigned the same slot!"
    EXPECT_THROW(ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}

// Test 4: Unclean shutdown while degraded — both devices valid, route persisted, clean_unmount=0.
// Exercises the (read_route != EITHER && clean_unmount == 0) branch: full bitmap dirty + age bump.
TEST(Raid1, UncleanShutdownWhileDegraded) {
    // device_a wins pick_superblock (higher age); its read_route=DEVA is used.
    // Age diff of 1 keeps device_b marked valid (not new_device).
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .is_slot_b = true});

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
            sb->fields.clean_unmount = 0;
            sb->fields.bitmap.age = htobe64(2);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.device_b = 1;
            sb->fields.clean_unmount = 0;
            sb->fields.bitmap.age = htobe64(1);
            return ublkpp::raid1::k_page_size;
        });

    // device_a: SB from __become_active, bitmap page from sync_to at shutdown, SB from destructor
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(3)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // __become_active: SB written with age bumped +16, route=DEVA, clean_unmount=0
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA, static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            EXPECT_EQ(htobe64(18), sb->fields.bitmap.age); // age 2 + 16 bump
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // sync_to at shutdown: bitmap page written to active device
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size); // bitmap area, not SB
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // destructor: SB written with clean_unmount=1
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(1, sb->fields.clean_unmount);
            return ublkpp::raid1::k_page_size;
        });
    // device_b: only SB from __become_active (backup device not written at shutdown when degraded)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
}

// Test 5: Unclean shutdown while healthy — route=EITHER, clean_unmount=0.
// Exercises the final (clean_unmount == 0) branch: just a log warning, no bitmap changes.
TEST(Raid1, UncleanShutdownWhileHealthy) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .is_slot_b = true});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.clean_unmount = 0;
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.device_b = 1;
            sb->fields.clean_unmount = 0;
            return ublkpp::raid1::k_page_size;
        });

    // __become_active writes SB to both (clean_unmount=0); destructor writes SB to both (clean_unmount=1).
    // No bitmap sync — route=EITHER means not degraded, so sync_to is not called at shutdown.
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test 6: Unclean shutdown while degraded (original broken test kept for documentation)
TEST(Raid1, UncleanShutdownDegraded) {
    // Create devices without setting up any expectations
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    // Set up device_a as clean, device_b as degraded, with unclean shutdown
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            if (nullptr != iovecs->iov_base) {
                memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                auto* sb = static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
                sb->fields.device_b = 0; // device_a
                sb->fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVA);
                sb->fields.clean_unmount = 0; // UNCLEAN shutdown!
            }
            return ublkpp::__iovec_len(iovecs, iovecs + 1);
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            // Device B is new/missing
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    // This should trigger lines 222-224: unclean shutdown in degraded mode
    // Should dirty the entire bitmap and bump age
    // Note: Constructor will fail because device_b cannot be read, so we expect a throw
    EXPECT_THROW(ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}
