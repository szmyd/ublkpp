#include "test_raid1_common.hpp"

// Brief: Test that RAID1 array rejects devices larger than SuperBitmap can track
// The SuperBitmap can track at most 32,176 bitmap pages (4022 bytes * 8 bits/byte)
// With 32KiB chunks, each bitmap page covers 1 GiB, giving a max capacity of ~31.4 TiB
//
TEST(Raid1, DevicesLargerThanAllowed) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = UINT64_MAX});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = UINT64_MAX});

    // Should throw exception for devices exceeding SuperBitmap capacity
    EXPECT_THROW(
        {
            auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
        },
        std::runtime_error
    );
}
