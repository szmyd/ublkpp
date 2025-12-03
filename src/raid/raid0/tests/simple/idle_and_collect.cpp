#include "../test_raid0_common.hpp"

// Test: idle_transition propagates to all devices
TEST(Raid0, IdleTransitionEnter) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});

    EXPECT_CALL(*device_a, idle_transition(_, true)).Times(1);
    EXPECT_CALL(*device_b, idle_transition(_, true)).Times(1);
    EXPECT_CALL(*device_c, idle_transition(_, true)).Times(1);

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector<std::shared_ptr<UblkDisk>>{device_a, device_b, device_c});

    raid_device.idle_transition(nullptr, true);
}

// Test: idle_transition exit
TEST(Raid0, IdleTransitionExit) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    EXPECT_CALL(*device_a, idle_transition(_, false)).Times(1);
    EXPECT_CALL(*device_b, idle_transition(_, false)).Times(1);

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 64 * Ki,
                                         std::vector<std::shared_ptr<UblkDisk>>{device_a, device_b});

    raid_device.idle_transition(nullptr, false);
}

// Test: collect_async calls all non-ublk-iouring devices
TEST(Raid0, CollectAsync) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});

    // Set one device to use ublk_iouring (should NOT be called)
    device_b->uses_ublk_iouring = true;

    // Only devices A and C should be called
    EXPECT_CALL(*device_a, collect_async(_, _)).Times(1);
    EXPECT_CALL(*device_b, collect_async(_, _)).Times(0);
    EXPECT_CALL(*device_c, collect_async(_, _)).Times(1);

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector<std::shared_ptr<UblkDisk>>{device_a, device_b, device_c});

    std::list<ublkpp::async_result> results;
    raid_device.collect_async(nullptr, results);
}

// Test: collect_async with all devices using ublk_iouring
TEST(Raid0, CollectAsyncAllUblkIouring) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    // All devices use ublk_iouring - none should be called
    device_a->uses_ublk_iouring = true;
    device_b->uses_ublk_iouring = true;

    EXPECT_CALL(*device_a, collect_async(_, _)).Times(0);
    EXPECT_CALL(*device_b, collect_async(_, _)).Times(0);

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 64 * Ki,
                                         std::vector<std::shared_ptr<UblkDisk>>{device_a, device_b});

    std::list<ublkpp::async_result> results;
    raid_device.collect_async(nullptr, results);
}
