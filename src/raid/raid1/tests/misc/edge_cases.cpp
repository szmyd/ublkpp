#include "raid/raid1/bitmap.hpp"
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

// Test 5: Unclean shutdown with both legs present and equal ages — the self-heal path.
// Both legs have route=EITHER, clean_unmount=0, same age.  The self-heal branch must:
//   • bump the canonical (device_a) age by +16 and pin reads to it via route=DEVA
//   • dirty the whole bitmap so a full resync runs
//   • mark device_b stale (unavail) so writes skip it until probe_mirror clears the flag
//   • skip writing device_b's SB in __become_active (preserving the on-disk age gap)
//
// Writes observed:
//   device_a — 3:  __become_active SB (age+16, route=DEVA)
//                   sync_to bitmap page (shutdown, degraded path)
//                   destructor SB (clean_unmount=1)
//   device_b — 0:  __become_active skips (unavail guard); destructor skips (degraded backup)
TEST(Raid1, DISABLED_UncleanShutdownBothPresentSelfHeal) {
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

    // device_a: __become_active SB + bitmap sync_to + destructor SB  (3 writes)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(3)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // __become_active: age bumped +16, route=DEVA, clean_unmount=0
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA, static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            EXPECT_EQ(htobe64(16), sb->fields.bitmap.age); // age 0 (normal_superblock) + 16
            EXPECT_EQ(0, sb->fields.clean_unmount);
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // sync_to: bitmap page (shutdown degraded path), not at SB offset
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, static_cast< off_t >(ublkpp::raid1::k_page_size));
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // destructor SB: clean_unmount=1, route still DEVA (array remains degraded at shutdown)
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(1, sb->fields.clean_unmount);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA, static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            return ublkpp::raid1::k_page_size;
        });
    // device_b: no writes — __become_active skips (unavail guard), destructor skips (degraded backup)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _)).Times(0);

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Reads must be pinned to canonical (device_a): route=DEVA, device_b marked stale (ERROR).
    auto const state = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, state.device_a);
    EXPECT_EQ(ublkpp::raid1::replica_state::ERROR, state.device_b);
    EXPECT_GT(state.bytes_to_sync, 0ULL);

    // Verify read routing: a sync read must dispatch to device_a only.
    // route=DEVA + device_b->unavail means __select_read_devices never returns device_b.
    // Run in a fresh thread to avoid contaminating the thread_local last_read state shared
    // across tests (same pattern used by other tests that exercise __select_read_devices).
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(0);
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result {
            memset(iov->iov_base, 0, iov->iov_len);
            return static_cast< int >(iov->iov_len);
        });
    RUN_IN_THREAD({
        alignas(4096) std::array< char, ublkpp::raid1::k_page_size > buf{};
        auto iov = iovec{.iov_base = buf.data(), .iov_len = buf.size()};
        EXPECT_TRUE(raid_device.sync_iov(UBLK_IO_OP_READ, &iov, 1, 0).has_value());
    });
}

// Test 5b: Crash-mid-resync idempotency — reassembly after a self-heal crash uses the
// existing new_device path, not the both-present-unclean branch.
//
// Simulates on-disk state after __become_active on the first self-heal assembly:
//   device_a: age=16, route=DEVA, clean_unmount=0   (canonical, bumped by self-heal)
//   device_b: age=0,  route=EITHER, clean_unmount=0  (stale, SB write was skipped)
//
// pick_superblock picks device_a (higher age).  age_diff = 16 > 1 → _device_b->new_device=true.
// __init_bitmap_and_degraded_route enters the existing one-new-device branch (not the
// both-present-unclean branch), bumps age another +16, and sets route=DEVA.
//
// Writes:
//   device_a — 3: __become_active SB (age=32, route=DEVA) + bitmap sync_to + destructor SB
//   device_b — 2: init_to bitmap pages (new_device path) + __become_active SB (age=32, route=DEVA)
//                 new_device path does NOT set unavail, so __become_active writes backup SB normally
TEST(Raid1, UncleanBothPresentSelfHealIdempotentAfterCrash) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .is_slot_b = true});

    // device_a: canonical SB from first self-heal assembly (age=16, route=DEVA)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.device_b = 0;
            sb->fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVA);
            sb->fields.clean_unmount = 0;
            sb->fields.bitmap.age = htobe64(16);
            return ublkpp::raid1::k_page_size;
        });
    // device_b: stale SB, age=0, route=EITHER (was never written during first self-heal)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.device_b = 1;
            sb->fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::EITHER);
            sb->fields.clean_unmount = 0;
            sb->fields.bitmap.age = htobe64(0);
            return ublkpp::raid1::k_page_size;
        });

    // device_a: __become_active SB (age=32, route=DEVA) + bitmap sync_to + destructor SB
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(3)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA, static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            EXPECT_EQ(htobe64(32), sb->fields.bitmap.age); // 16 + 16 bump from new_device path
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t addr) -> io_result {
            EXPECT_GE(addr, static_cast< off_t >(ublkpp::raid1::k_page_size));
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(0UL, addr);
            EXPECT_EQ(1, reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.clean_unmount);
            return ublkpp::raid1::k_page_size;
        });
    // device_b: init_to bitmap pages (new_device path), then __become_active SB
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t addr) -> io_result {
            // init_to: first (and only, for 1 GiB) bitmap page at sizeof(SuperBlock) == k_page_size
            EXPECT_EQ(addr, static_cast< off_t >(ublkpp::raid1::k_page_size));
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
            // __become_active SB: no unavail guard in new_device path, backup gets SB
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA, static_cast< ublkpp::raid1::read_route >(sb->fields.read_route));
            EXPECT_EQ(htobe64(32), sb->fields.bitmap.age);
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Should still be degraded (route=DEVA), resync pending
    auto const state = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, state.device_a);
    EXPECT_EQ(ublkpp::raid1::replica_state::SYNCING, state.device_b); // unavail NOT set by new_device path
    EXPECT_GT(state.bytes_to_sync, 0ULL);
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

// L4: Device too small to hold the reserved region — must throw with a clear message.
// A 64 MiB device is well below the minimum reserved region for any supported configuration.
TEST(Raid1, DeviceTooSmallThrows) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 64 * Mi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 64 * Mi, .is_slot_b = true});

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

// Test 7: An all-zero superbitmap on a previously-degraded array must be caught at the call site.
// superbitmap_nonempty() is the guard used in raid1.cpp before calling load_from.
TEST(Raid1, SuperbitmapNonemptyReturnsFalseOnCleanBitmap) {
    ublkpp::raid1::SuperBlock sb{};
    // sb.superbitmap_reserved is zero-initialized — no pages marked dirty.
    ublkpp::raid1::Bitmap bitmap(Gi, 32 * Ki, 4096, sb.superbitmap_reserved);
    EXPECT_FALSE(bitmap.superbitmap_nonempty());
}

TEST(Raid1, SuperbitmapNonemptyReturnsTrueAfterDirtyRegion) {
    ublkpp::raid1::SuperBlock sb{};
    ublkpp::raid1::Bitmap bitmap(Gi, 32 * Ki, 4096, sb.superbitmap_reserved);
    bitmap.dirty_region(0, 32 * Ki);
    EXPECT_TRUE(bitmap.superbitmap_nonempty());
}

// Test 8: Clean degraded startup — route=DEVA, clean_unmount=1, superbitmap has dirty pages.
// Exercises the load_from(*active_dev) call site and verifies the destructor persists
// the superbitmap (include_superbitmap=true) so the invariant holds on next startup.
TEST(Raid1, CleanDegradedStartupLoadsFromActiveDevice) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .is_slot_b = true});

    // device_a: SB read, then bitmap page-0 read (load_from follows SB read)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVA);
            sb->fields.clean_unmount = 1;
            sb->fields.bitmap.age = htobe64(10);
            sb->superbitmap_reserved[0] = 0x01; // bit 0 set → page 0 is dirty
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // Bitmap page 0 read at offset k_page_size
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(static_cast< off_t >(ublkpp::raid1::k_page_size), addr);
            memset(iovecs->iov_base, 0xFF, ublkpp::raid1::k_page_size); // non-zero → dirty page
            return ublkpp::raid1::k_page_size;
        });

    // device_b: SB read only (age=9, one behind device_a — not new_device)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.device_b = 1;
            sb->fields.bitmap.age = htobe64(9);
            return ublkpp::raid1::k_page_size;
        });

    // device_a: __become_active SB write + destructor SB write (include_superbitmap=true)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            // Destructor: clean_unmount=1 and superbitmap persisted (non-zero)
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(1, sb->fields.clean_unmount);
            EXPECT_NE(0, sb->superbitmap_reserved[0]) << "superbitmap must be persisted on shutdown";
            return ublkpp::raid1::k_page_size;
        });

    // device_b: __become_active SB write only — degraded, no destructor write to backup
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

// Test 9: Clean degraded startup with an all-zero superbitmap.
// Covers the superbitmap_nonempty() guard in the clean-degraded branch (Branch 4) of
// __init_bitmap_and_degraded_route. Before Fix 2 this threw; after Fix 2 the constructor
// warns and continues. load_from skips all pages (superbitmap empty) so bytes_to_sync=0;
// route stays DEVA until resync calls complete() on its first pass.
TEST(Raid1, CleanDegradedStartupEmptySuperbitmap) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .is_slot_b = true});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, 0UL))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::DEVA);
            sb->fields.clean_unmount = 1;
            sb->fields.bitmap.age = htobe64(10);
            // superbitmap_reserved stays zero — simulates the race-produced on-disk state
            return ublkpp::raid1::k_page_size;
        });
    // device_a gets 2 writes: __become_active (DEVA) then destructor clean_unmount (DEVA).
    // No bitmap read — superbitmap is empty so load_from skips all pages.
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, 0UL))
        .Times(2)
        .WillRepeatedly([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, 0UL))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto* sb = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            sb->fields.device_b = 1;
            sb->fields.bitmap.age = htobe64(9);
            return ublkpp::raid1::k_page_size;
        });
    // device_b gets 1 write: __become_active (DEVA). Degraded destructor skips the backup device.
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, 0UL))
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t) -> io_result { return iov->iov_len; });

    EXPECT_NO_THROW({
        auto raid = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
        raid.toggle_resync(false);
        // Fix 2 post-conditions: route=DEVA (unchanged), empty bitmap (superbitmap empty; load_from skips).
        auto const s = raid.replica_states();
        EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, s.device_a);   // active leg
        EXPECT_EQ(ublkpp::raid1::replica_state::SYNCING, s.device_b); // backup leg, route not yet EITHER
        EXPECT_EQ(0ULL, s.bytes_to_sync);                             // superbitmap empty; nothing to sync
    });
}

// Verifies that resync_level=0 is rejected at construction time.
// Only meaningful when the binary is invoked with --resync_level=0; skipped otherwise so
// the regular Raid1Test CTest entry (resync_level=4) is not affected.
// See CMakeLists.txt: Raid1ZeroResyncLevelThrows target.
TEST(Raid1, ZeroResyncLevelThrows) {
    if (SISL_OPTIONS["resync_level"].as< uint32_t >() != 0) GTEST_SKIP();
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    EXPECT_THROW(ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}
