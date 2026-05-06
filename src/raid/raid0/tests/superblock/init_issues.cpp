#include "test_raid0_common.hpp"

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
