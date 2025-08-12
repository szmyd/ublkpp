#include "test_raid1_common.hpp"

// Brief: Test that RAID1 array maintains a self-imposing limit to restrict the reserved size
//
TEST(Raid1, DevicesLargerThanAllowed) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = ublkpp::raid1::k_max_dev_size + ublkpp::Ti});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = ublkpp::raid1::k_max_dev_size * 2});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    EXPECT_EQ(raid_device.capacity(), ublkpp::raid1::k_max_dev_size);
    EXPECT_STREQ(raid_device.id().c_str(), "RAID1");

    // CanDiscard and DirectIO `true` be default.
    EXPECT_EQ(raid_device.can_discard(), true);
    EXPECT_EQ(raid_device.direct_io, true);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
