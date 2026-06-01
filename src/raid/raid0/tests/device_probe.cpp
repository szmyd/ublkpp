#include "test_raid0_common.hpp"

// max_discard_sectors is propagated from child devices (min * stripe count)
TEST(Raid0, MaxDiscardSectorsPropagated) {
    constexpr uint32_t k_child_max_discard = 1000; // sectors
    // Extra parens protect the comma inside the brace-init from being treated as a macro
    // argument separator, while keeping the expression self-contained inside the []lambda.
    auto device_a = CREATE_DISK((TestParams{.capacity = Gi, .max_discard_sectors = k_child_max_discard}));
    auto device_b = CREATE_DISK((TestParams{.capacity = Gi, .max_discard_sectors = k_child_max_discard * 2}));

    auto raid_device = ublkpp::make_raid0_disk(boost::uuids::random_generator()(), 32 * Ki,
                                               std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b});

    // min child (1000) * 2 stripes = 2000 sectors
    EXPECT_EQ(raid_device->max_discard_sectors(), k_child_max_discard * 2u);
}

// Three-stripe case: verifies min-child selection and multiplication with stripe count > 2.
// device_c has the tightest limit; result must be min(1000,2000,500) * 3 = 1500 sectors.
TEST(Raid0, MaxDiscardSectorsPropagatedThreeStripes) {
    auto device_a = CREATE_DISK((TestParams{.capacity = Gi, .max_discard_sectors = 1000u}));
    auto device_b = CREATE_DISK((TestParams{.capacity = Gi, .max_discard_sectors = 2000u}));
    auto device_c = CREATE_DISK((TestParams{.capacity = Gi, .max_discard_sectors = 500u}));

    auto raid_device =
        ublkpp::make_raid0_disk(boost::uuids::random_generator()(), 32 * Ki,
                                std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b, device_c});

    // min child (500) * 3 stripes = 1500 sectors
    EXPECT_EQ(raid_device->max_discard_sectors(), 500u * 3u);
}

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

    // safe_max = leg_count * usable_stripes_per_leg * stripe, where
    //   usable_stripes_per_leg = floor(leg_cap / stripe) - 1   (the -1 reserves the head stripe
    //   for our superblock). This is the largest size that keeps every per-leg offset within the
    //   backing device. The final max_sectors alignment in the ctor is 512 KiB here (capped by
    //   DEF_BUF_SIZE at raid0.cpp:97, regardless of leg max_io or count) and both the pre- and
    //   post-fix totals are 512 KiB-aligned, so that step is a no-op and the value is exact.
    uint64_t const safe_max = n * ((leg_cap / stripe) - 1) * stripe;
    // Pre-fix this returned n * (leg_cap - stripe) = 8,589,410,304, which is (64 KiB * 8) too large
    // and would route reads past the end of each leg.
    EXPECT_EQ(raid_device->capacity(), safe_max);
}

// Brief: A leg smaller than two stripes is rejected, never over-reported.
//
// The H3 floor runs before the H2 guard, so a sub-2-stripe leg is floored to zero usable stripes
// and H2 throws. This pins the ordering: the floor must not let construction underflow the unsigned
// dev_sectors subtraction or expose a degenerate (phantom) capacity for a too-small device.
TEST(Raid0, LegSmallerThanTwoStripesThrows) {
    constexpr uint64_t leg_cap = 100 * Ki; // smaller than a single 128 KiB stripe
    constexpr uint32_t stripe = 128 * Ki;

    std::vector< std::shared_ptr< ublk_disk > > disks;
    disks.push_back(CREATE_DISK(TestParams{.capacity = leg_cap}));
    disks.push_back(CREATE_DISK(TestParams{.capacity = leg_cap}));

    EXPECT_THROW(ublkpp::make_raid0_disk(boost::uuids::random_generator()(), stripe, std::move(disks)),
                 std::runtime_error);
}
