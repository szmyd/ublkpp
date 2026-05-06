#include "../test_raid1_common.hpp"

// Test: prepare collects FDs from both devices
TEST(Raid1, OpenForUring) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    // Set up expectations for prepare
    EXPECT_CALL(*device_a, prepare(_, 0))
        .Times(1)
        .WillOnce(::testing::Return(ublkpp::ublk_disk::prepare_result{.fds = {10, 11}}));

    EXPECT_CALL(*device_b, prepare(_, 2))
        .Times(1)
        .WillOnce(::testing::Return(ublkpp::ublk_disk::prepare_result{.fds = {12}}));

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto result = raid_device.prepare(nullptr, 0);

    // Should have 3 FDs total (2 from device_a, 1 from device_b)
    ASSERT_EQ(result.fds.size(), 3);

    auto it = result.fds.begin();
    EXPECT_EQ(*it++, 10);
    EXPECT_EQ(*it++, 11);
    EXPECT_EQ(*it++, 12);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: prepare with no FDs from devices
TEST(Raid1, OpenForUringEmpty) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    // Both devices return empty lists
    EXPECT_CALL(*device_a, prepare(_, 0)).Times(1).WillOnce(::testing::Return(ublkpp::ublk_disk::prepare_result{}));

    EXPECT_CALL(*device_b, prepare(_, 0)).Times(1).WillOnce(::testing::Return(ublkpp::ublk_disk::prepare_result{}));

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto result = raid_device.prepare(nullptr, 0);

    EXPECT_TRUE(result.fds.empty());

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: prepare offset calculation
TEST(Raid1, OpenForUringOffsetCalculation) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    // Device A returns 3 FDs, so device B should get offset of 103 (100 + 3)
    EXPECT_CALL(*device_a, prepare(_, 100))
        .Times(1)
        .WillOnce(::testing::Return(ublkpp::ublk_disk::prepare_result{.fds = {5, 6, 7}}));

    EXPECT_CALL(*device_b, prepare(_, 103))
        .Times(1)
        .WillOnce(::testing::Return(ublkpp::ublk_disk::prepare_result{.fds = {8, 9}}));

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto result = raid_device.prepare(nullptr, 100);

    ASSERT_EQ(result.fds.size(), 5);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
