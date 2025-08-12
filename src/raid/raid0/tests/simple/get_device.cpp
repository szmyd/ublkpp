#include "test_raid0_common.hpp"

TEST(Raid0, GetDevice) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});
    EXPECT_TRUE(raid_device.get_device(1) == device_b);
    EXPECT_TRUE(raid_device.get_device(0) == device_a);
    EXPECT_TRUE(raid_device.get_device(2) == device_c);
    EXPECT_FALSE(raid_device.get_device(3));
}
