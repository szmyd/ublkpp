#include "test_raid0_common.hpp"

TEST(Raid0, GetDevice) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device =
        ublkpp::make_raid0_disk(boost::uuids::random_generator()(), 32 * Ki,
                                std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b, device_c});
    EXPECT_TRUE(ublkpp::raid0::get_device(*raid_device, 1) == device_b);
    EXPECT_TRUE(ublkpp::raid0::get_device(*raid_device, 0) == device_a);
    EXPECT_TRUE(ublkpp::raid0::get_device(*raid_device, 2) == device_c);
    EXPECT_FALSE(ublkpp::raid0::get_device(*raid_device, 3));
}
