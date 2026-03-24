#include "test_raid1_common.hpp"

// If we initialize with one new device and one defunct, take the working device as is
TEST(Raid1, DefunctDiskB) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::DefunctDisk >();
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Only update the SuperBlock, no writes to BITMAP
    EXPECT_TO_WRITE_SB(device_a);
}

TEST(Raid1, DefunctDiskA) {
    auto device_a = std::make_shared< ublkpp::DefunctDisk >();
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Only update the SuperBlock, no writes to BITMAP
    EXPECT_TO_WRITE_SB(device_b);
}

// We should throw if both devices are Defunct
TEST(Raid1, DefunctDisks) {
    auto device_a = std::make_shared< ublkpp::DefunctDisk >();
    auto device_b = std::make_shared< ublkpp::DefunctDisk >();
    EXPECT_THROW(ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}
