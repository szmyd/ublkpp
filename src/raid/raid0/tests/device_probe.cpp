#include "test_raid0_common.hpp"

// Brief: Test that RAID0 devices correctly report paramaters based on Devices A and B
//
// Construct a RAID0 device with 3 Identical underlying devices that match on every
// parameter. The final RAID0 parameters should be equivalent to the underlying
// devices themselves with the capacity being 3x the device size.
TEST(Raid0, IdenticalDeviceProbing) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});

    auto raid_device =
        ublkpp::make_raid0_disk(boost::uuids::random_generator()(), 32 * Ki,
                                std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b, device_c});
    // Aligned to max_tx size
    EXPECT_EQ(raid_device->capacity(), (3 * Gi) - (512 * Ki));
    EXPECT_STREQ(raid_device->id().c_str(), "RAID0");

    // CanDiscard and DirectIO `true` be default.
    EXPECT_EQ(raid_device->can_discard(), true);
    EXPECT_EQ(raid_device->direct_io(), true);
}

// Brief: A leg whose capacity is not a multiple of the stripe size must not inflate the array size.
//
// RAID0 can only stripe across whole stripes present on every leg. The trailing partial stripe on
// each leg (leg_capacity % stripe_size) is unusable and must be discarded BEFORE multiplying by the
// leg count. Pre-fix the reported capacity carried (remainder * leg_count) of phantom space, so a
// top-of-device read mapped to a per-leg offset past the end of the backing device (observed on a
// 50-leg RAID10 of 3 TiB legs after raid1 SuperBlock v2 dropped the alignment that had masked this).
TEST(Raid0, PartialStripeLegNotOverReported) {
    constexpr uint64_t leg_cap = Gi + 64 * Ki; // 4 KiB-aligned, but NOT a 128 KiB stripe multiple
    constexpr uint32_t stripe = 128 * Ki;
    constexpr size_t n = 8;

    std::vector< std::shared_ptr< ublk_disk > > disks;
    for (size_t i = 0; i < n; ++i) {
        disks.push_back(CREATE_DISK(TestParams{.capacity = leg_cap}));
    }

    auto raid_device = ublkpp::make_raid0_disk(boost::uuids::random_generator()(), stripe, std::move(disks));

    // Largest size that keeps every per-leg offset within the backing device: discard the partial
    // trailing stripe on each leg, reserve one stripe at the head for our superblock, then stripe.
    uint64_t const safe_max = n * ((leg_cap / stripe) - 1) * stripe;
    // Pre-fix this returned n * (leg_cap - stripe) = 8,589,410,304, which is (64 KiB * 8) too large
    // and would route reads past the end of each leg.
    EXPECT_EQ(raid_device->capacity(), safe_max);
}
