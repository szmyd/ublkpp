#include "test_raid1_common.hpp"

// Brief: Test that RAID1 array maintains a self-imposing limit to restrict the reserved size
// The SuperBitmap can track at most 32,176 bitmap pages (4022 bytes * 8 bits/byte)
// With 32KiB chunks, each bitmap page covers 1 GiB, giving a max capacity of ~31.4 TiB
//
TEST(Raid1, DevicesLargerThanAllowed) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = UINT64_MAX});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = UINT64_MAX});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // SuperBitmap max: 32,176 pages * 1 GiB/page = 31.42 TiB
    // After subtracting reserved space for SuperBlock + bitmap pages, user capacity is slightly less
    constexpr auto expected_max_capacity = 31ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL;  // ~31 TiB
    EXPECT_GT(raid_device.capacity(), expected_max_capacity);  // Should be just over 31 TiB
    EXPECT_LT(raid_device.capacity(), 32ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL);  // Should be less than 32 TiB
    EXPECT_STREQ(raid_device.id().c_str(), "RAID1");

    // CanDiscard and DirectIO `true` be default.
    EXPECT_EQ(raid_device.can_discard(), true);
    EXPECT_EQ(raid_device.direct_io, true);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
