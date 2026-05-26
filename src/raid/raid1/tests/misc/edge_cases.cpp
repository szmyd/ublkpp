#include "test_raid1_common.hpp"

using namespace std::chrono_literals;

// Test 1: Try to swap a device that's not part of the array
TEST(Raid1, SwapUnrecognizedDevice) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi}));
    // device_c is not initialized into a RAID, so don't use CREATE_DISK macro
    auto device_c = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "device_c"});

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

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

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

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
            return ublkpp::iovec_len(iovecs, iovecs + 1);
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            if (nullptr != iovecs->iov_base) {
                memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                // ALSO set device_b = 1 (this is ALSO device B - invalid!)
                static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b = 1;
            }
            return ublkpp::iovec_len(iovecs, iovecs + 1);
        });

    // Should throw runtime_error: "Found both devices were assigned the same slot!"
    EXPECT_THROW(ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
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
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
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
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
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
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA, static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            EXPECT_EQ(htobe64(18), sb->fields.bitmap.age); // age 2 + 16 bump
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // sync_to at shutdown: bitmap page written to active device
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size); // bitmap area, not SB
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // destructor: SB written with clean_unmount=1
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
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
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
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
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.clean_unmount = 0;
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
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
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
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
            return ublkpp::iovec_len(iovecs, iovecs + 1);
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            // Device B is new/missing
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    // This should trigger lines 222-224: unclean shutdown in degraded mode
    // Should dirty the entire bitmap and bump age
    // Note: Constructor will fail because device_b cannot be read, so we expect a throw
    EXPECT_THROW(ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}

// Test L4: Device too small to hold reserved region — should throw with a clear message.
// The reserved region for a v2 superblock with default chunk_size (32KiB) is ~125 MiB.
// A 64 MiB device is guaranteed to be smaller than the reserved region.
TEST(Raid1, DeviceTooSmallThrows) {
    // 64 MiB is below the ~125 MiB v2 reserved region.
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 64 * Mi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 64 * Mi, .is_slot_b = true});

    // Both devices need to return a superblock on READ so __load_and_select_superblock succeeds.
    // __init_params will then throw before any WRITEs are issued.
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            if (iovecs->iov_base) memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            if (iovecs->iov_base) {
                memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b = 1;
            }
            return ublkpp::raid1::k_page_size;
        });

    EXPECT_THROW(
        {
            try {
                ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
            } catch (std::runtime_error const& e) {
                EXPECT_NE(std::string::npos, std::string(e.what()).find("device too small"))
                    << "Expected 'device too small' in exception message, got: " << e.what();
                throw;
            }
        },
        std::runtime_error);
}

// Test M6: Verify that bitmap.age is NOT reverted when __become_degraded's SB write fails.
// After the write failure, the in-memory age must retain the post-degradation value so that
// the next successful SB write (e.g. at clean shutdown) persists the correct age.
TEST(Raid1, BecomeDegradedAgeNotRevertedOnWriteFailure) {
    auto raw_a = std::make_shared< ::testing::StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi});
    auto raw_b =
        std::make_shared< ::testing::StrictMock< ublkpp::TestDisk > >(TestParams{.capacity = Gi, .is_slot_b = true});

    // Both devices present and healthy for superblock load
    EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            if (iov->iov_base) {
                memcpy(iov->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                static_cast< ublkpp::raid1::SuperBlock* >(iov->iov_base)->fields.device_b = 1;
            }
            return ublkpp::raid1::k_page_size;
        });

    // The age captured from the shutdown superblock write to raw_a.
    auto raw_a_shutdown_age = uint64_t{0};

    {
        // Init: __become_active writes SB to both devices
        EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .Times(::testing::AnyNumber())
            .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
        EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .Times(::testing::AnyNumber())
            .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

        auto raid =
            std::make_unique< ublkpp::raid1::Raid1Disk >(boost::uuids::string_generator()(test_uuid), raw_a, raw_b);
        raid->toggle_resync(false);

        // I/O phase: raw_a data write succeeds; raw_b data write fails → __become_degraded.
        // __become_degraded's SB write to raw_a (offset 0) also fails — this is the M6 path.
        // Subsequent SB writes (at shutdown) succeed and capture the age.
        EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, ::testing::Ne((off_t)0)))
            .WillOnce([](uint8_t, iovec* iv, uint32_t, off_t) -> io_result { return iv->iov_len; })
            .WillRepeatedly([](uint8_t, iovec* iv, uint32_t, off_t) -> io_result { return iv->iov_len; });
        EXPECT_CALL(*raw_b, sync_iov(UBLK_IO_OP_WRITE, _, _, ::testing::Ne((off_t)0)))
            .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });
        EXPECT_CALL(*raw_a, sync_iov(UBLK_IO_OP_WRITE, _, _, (off_t)0))
            // First call: __become_degraded's SB write — fail it
            .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            })
            // Subsequent calls: shutdown SB write — succeed and capture the age
            .WillRepeatedly([&raw_a_shutdown_age](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
                if (iov->iov_base)
                    raw_a_shutdown_age =
                        be64toh(static_cast< ublkpp::raid1::SuperBlock* >(iov->iov_base)->fields.bitmap.age);
                return iov->iov_len;
            });

        iovec iov{nullptr, 4 * Ki};
        std::ignore = raid->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 4 * Ki);

        // raid goes out of scope here; destructor issues sync_to (dirty bitmap pages) then SB write
    }

    // The age written at clean shutdown must be 1 (the degradation bump), NOT 0 (the pre-bump
    // value that was incorrectly restored by the old rollback).
    EXPECT_EQ(1UL, raw_a_shutdown_age)
        << "bitmap.age must not be reverted to pre-degradation value after failed __become_degraded SB write";
}

// Verifies that resync_level=0 is rejected at construction time.
// Only meaningful when the binary is invoked with --resync_level=4; skipped otherwise so
// the regular Raid1Test CTest entry (resync_level=4) is not affected.
// See CMakeLists.txt: Raid1ZeroResyncLevelThrows target.
TEST(Raid1, ZeroResyncLevelThrows) {
    if (SISL_OPTIONS["resync_level"].as< uint32_t >() != 0) GTEST_SKIP();
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    EXPECT_THROW(ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}
