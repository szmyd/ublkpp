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

// C1 validation: with stripe_size=2KiB and 2 disks, stride=4KiB. The default max_io of 512KiB
// needs ceil(512KiB/4KiB)=128 iovecs per stripe, exceeding _max_stripe_cnt=64. Must throw.
TEST(Raid0, StripeToSmallForMaxIoThrows) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    EXPECT_THROW(ublkpp::make_raid0_disk(boost::uuids::random_generator()(), 2 * Ki,
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
