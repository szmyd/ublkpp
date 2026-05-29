#include "test_raid0_common.hpp"

// max_discard_sectors is propagated from child devices (min * stripe count)
TEST(Raid0, MaxDiscardSectorsPropagated) {
    constexpr uint32_t k_child_max_discard = 1000; // sectors
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi, .max_discard_sectors = k_child_max_discard});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi, .max_discard_sectors = k_child_max_discard * 2});

    auto raid_device = ublkpp::make_raid0_disk(boost::uuids::random_generator()(), 32 * Ki,
                                               std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b});

    // min child (1000) * 2 stripes = 2000 sectors
    EXPECT_EQ(raid_device->max_discard_sectors(), k_child_max_discard * 2u);
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
