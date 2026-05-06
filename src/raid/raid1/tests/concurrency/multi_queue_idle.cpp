#include "test_raid1_common.hpp"

#include <atomic>
#include <barrier>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

// Regression tests for probe_tick concurrency correctness.
//
// probe_tick() is called by the tgt's queue loop whenever a probe timeout fires. Multiple queue
// threads can call probe_tick() concurrently if separate rings each fire a timeout. The disk
// must handle concurrent probe_tick() calls without data races or deadlocks.

// Test: probe_tick from N threads concurrently does not race or deadlock.
// This test is primarily a TSAN regression test — run with -o sanitize=thread to catch races.
TEST(Raid1Concurrency, ProbeTickConcurrent) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    constexpr int k_threads = 4;
    constexpr int k_iters = 50;

    // Allow any number of sync_iov calls in case a probe fires unexpectedly
    EXPECT_CALL(*device_a, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    std::barrier sync_point{k_threads + 1};
    std::vector< std::thread > threads;
    threads.reserve(k_threads);

    for (int i = 0; i < k_threads; ++i) {
        threads.emplace_back([&] {
            sync_point.arrive_and_wait();
            for (int j = 0; j < k_iters; ++j)
                raid_device.probe_tick(nullptr);
        });
    }

    sync_point.arrive_and_wait();
    for (auto& t : threads)
        t.join();

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: Rapid probe_tick calls don't crash or corrupt state.
TEST(Raid1Concurrency, ProbeTickRapid) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    EXPECT_CALL(*device_a, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    constexpr int k_iters = 200;
    for (int i = 0; i < k_iters; ++i)
        raid_device.probe_tick(nullptr);

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: swap_device() races safely against probe_tick() — the probe re-captures route state
// via __capture_route_state() so it never targets a device that was just swapped out.
// Run with -o sanitize=thread; TSAN catches a data race on the device pointer if the
// lock-free retry loop is broken.
TEST(Raid1Concurrency, SwapDeviceWhileProbing) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    EXPECT_CALL(*device_a, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    auto device_c = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});
    EXPECT_CALL(*device_c, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    std::atomic< bool > stop{false};
    std::barrier<> sync_point{2};

    auto probe_thread = std::thread([&] {
        sync_point.arrive_and_wait();
        while (!stop.load(std::memory_order_relaxed))
            raid_device.probe_tick(nullptr);
    });

    sync_point.arrive_and_wait();
    auto old_dev = raid_device.swap_device("DiskB", device_c);
    EXPECT_EQ(old_dev, device_b);

    stop.store(true, std::memory_order_relaxed);
    probe_thread.join();
}
