#include "test_raid0_common.hpp"

// H2: device capacity ≤ stripe_size causes an unsigned underflow in the volume-size calculation.
// The constructor must detect this and throw rather than computing a nonsensical volume size.
TEST(Raid0, DeviceSmallerThanStripeSizeThrows) {
    // Capacity exactly equals one stripe — leaves zero usable sectors after subtracting the
    // stripe used as the superblock region. Guard must catch dev_sectors <= stripe_size_sectors.
    auto device_a = CREATE_DISK(TestParams{.capacity = 32 * Ki});
    auto device_b = CREATE_DISK(TestParams{.capacity = 32 * Ki});
    EXPECT_THROW(ublkpp::make_raid0_disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b}),
                 std::runtime_error);
}

// M2: if every child device reports max_tx() == 0, max_sectors collapses to 0 via the std::min
// loop and the subsequent volume alignment divides by zero. The constructor must throw.
TEST(Raid0, ZeroMaxSectorsThrows) {
    auto device_a = CREATE_DISK((TestParams{.capacity = 2 * Gi, .max_io = 0}));
    auto device_b = CREATE_DISK((TestParams{.capacity = 2 * Gi, .max_io = 0}));
    EXPECT_THROW(ublkpp::make_raid0_disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b}),
                 std::runtime_error);
}

// Regression: a corrupted on-disk SB with valid magic+UUID but stripe_size=0 must throw rather than
// calling ilog2(0) (UB) or dividing by zero in the C1 iovecs-per-stripe check.
TEST(Raid0, ZeroStripeSizeFromCorruptedSBThrows) {
    // Devices return a valid SB header (magic+UUID) but stripe_size=0.
    // device_a gets stripe_off=0, device_b gets stripe_off=1 (matches array index).
    // Both reads AND writes fire before the guard (SB is committed per-device in the loop).
    auto make_dev = [](TestParams params, uint16_t stripe_off) {
        auto device = std::make_shared< ublkpp::TestDisk >(params);
        EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
            .Times(1)
            .WillOnce([stripe_off](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
                auto sb = normal_superblock;
                sb.fields.stripe_off = htobe16(stripe_off);
                sb.fields.stripe_size = 0; // zero stripe_size — corrupted
                memcpy(iovecs->iov_base, &sb, sizeof(ublkpp::raid0::SuperBlock));
                return sizeof(ublkpp::raid0::SuperBlock);
            });
        // No write expectation: load_superblock only writes on the new-device path (no magic).
        // An existing SB (valid magic) is accepted as-is; the corrupted stripe_size propagates
        // into _stripe_size, and the guard fires before any write occurs.
        return device;
    };
    auto device_a = make_dev(TestParams{.capacity = Gi}, 0);
    auto device_b = make_dev(TestParams{.capacity = Gi}, 1);
    EXPECT_THROW(ublkpp::make_raid0_disk(boost::uuids::string_generator()(test_uuid), 32 * Ki,
                                         std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b}),
                 std::runtime_error);
}

// Brief: If any device should not load/write superblocks correctly, initialization should throw
TEST(Raid0, FailedReadSB) {
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, true, false, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, true, false, false);
        EXPECT_THROW(auto raid_device =
                         ublkpp::make_raid0_disk(boost::uuids::random_generator()(), 32 * Ki,
                                                 std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b}),
                     std::runtime_error);
    }
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, false, true, false, false);
        EXPECT_THROW(auto raid_device =
                         ublkpp::make_raid0_disk(boost::uuids::random_generator()(), 32 * Ki,
                                                 std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b}),
                     std::runtime_error);
    }
}
