#include "test_raid1_common.hpp"

#include <atomic>
#include <barrier>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

// Regression tests for issue #193: multi-queue idle probe correctness.
//
// Problem: idle_transition() was not safe for nr_hw_queues > 1:
//   1. Concurrent calls raced on Raid1AvailProbeTask::_probe (unguarded jthread).
//   2. Any single queue going idle started the probe; any single queue exiting idle stopped it.
//
// Fix:
//   - _idle_probe_lock (mutex in Raid1DiskImpl) serializes all probe launch/stop calls.
//   - _idle_queue_count (atomic) gates probe start until all queues are idle; stops on first exit.
//   - _nr_hw_queues is set by counting open_for_uring() calls (one per queue thread, via init_queue).

// Test: With nr_hw_queues=2, sequential idle_transition calls follow correct counting semantics.
// Verifies: no crash, no deadlock, correct enter/exit sequencing.
TEST(Raid1Concurrency, MultiQueueIdleSequential) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Simulate 2 queue threads initializing (sets _nr_hw_queues = 2)
    ublksrv_queue queues[2]{};
    raid_device.open_for_uring(&queues[0], 0); // queue 0: enables resync, _nr_hw_queues = 1
    raid_device.open_for_uring(&queues[1], 0); // queue 1: _nr_hw_queues = 2

    // Queue 0 idle: count = 1 < 2, probe should NOT start yet
    raid_device.idle_transition(&queues[0], true);

    // Queue 1 idle: count = 2 == 2, probe starts
    raid_device.idle_transition(&queues[1], true);

    // Queue 0 exits idle: stops probes, count = 1
    raid_device.idle_transition(&queues[0], false);

    // Queue 0 goes idle again: count = 2 == 2, probe restarts
    raid_device.idle_transition(&queues[0], true);

    // Both queues exit idle: probes stopped
    raid_device.idle_transition(&queues[0], false);
    raid_device.idle_transition(&queues[1], false);

    // Expect clean unmount writes
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: Concurrent idle_transition calls from N threads do not cause data races or deadlocks.
// This test is primarily a TSAN regression test — run with -o sanitize=True to catch races.
TEST(Raid1Concurrency, MultiQueueIdleConcurrent) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    constexpr int k_queues = 4;
    constexpr int k_iters = 50;

    // Simulate k_queues queue threads (sets _nr_hw_queues = k_queues)
    ublksrv_queue queues[k_queues]{};
    for (int i = 0; i < k_queues; ++i)
        raid_device.open_for_uring(&queues[i], 0);

    // Allow any number of sync_iov calls in case the periodic probe fires unexpectedly
    EXPECT_CALL(*device_a, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    std::barrier sync_point{k_queues + 1};
    std::vector< std::thread > threads;
    threads.reserve(k_queues);

    for (int q = 0; q < k_queues; ++q) {
        threads.emplace_back([&, q] {
            sync_point.arrive_and_wait(); // All threads fire simultaneously
            for (int i = 0; i < k_iters; ++i) {
                raid_device.idle_transition(&queues[q], true);
                raid_device.idle_transition(&queues[q], false);
            }
        });
    }

    sync_point.arrive_and_wait(); // Release all threads
    for (auto& t : threads)
        t.join();

    // Ensure all queues are in non-idle state (counter = 0) before destruction
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: Rapid single-queue idle toggle. Verifies the counter does not underflow when a queue
// enters and exits idle rapidly (common under bursty workloads). No crash or deadlock expected.
TEST(Raid1Concurrency, MultiQueueIdleRapidToggle) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // Single queue with a real pointer so _pending_results.emplace is exercised
    ublksrv_queue q{};
    raid_device.open_for_uring(&q, 0);

    // Allow any number of sync_iov calls in case the periodic probe fires
    EXPECT_CALL(*device_a, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, sync_iov(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    constexpr int k_iters = 200;
    for (int i = 0; i < k_iters; ++i) {
        raid_device.idle_transition(&q, true);
        raid_device.idle_transition(&q, false);
    }

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test: swap_device() races safely against idle_transition() — the probe uses state re-captured
// under _idle_probe_lock so launch() never targets a device that was just swapped out.
// Run with -o sanitize=True; TSAN catches a data race on the device pointer if the locked
// re-capture is removed.
TEST(Raid1Concurrency, SwapDeviceWhileIdleTransitioning) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    // 2 queues with real pointers so _nr_hw_queues == 2 and _pending_results is populated
    ublksrv_queue queues[2]{};
    raid_device.open_for_uring(&queues[0], 0);
    raid_device.open_for_uring(&queues[1], 0);

    // Cover probe and swap I/O for device_a (stays throughout) and device_b (swapped out)
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

    // Idle thread: both queues enter idle (triggering probe launch) then exit, in a tight loop
    auto idle_thread = std::thread([&] {
        sync_point.arrive_and_wait();
        while (!stop.load(std::memory_order_relaxed)) {
            raid_device.idle_transition(&queues[0], true);
            raid_device.idle_transition(&queues[1], true);
            raid_device.idle_transition(&queues[0], false);
            raid_device.idle_transition(&queues[1], false);
        }
    });

    // Swap device_b → device_c while the idle thread is hammering idle_transition
    sync_point.arrive_and_wait();
    auto old_dev = raid_device.swap_device("DiskB", device_c);
    EXPECT_EQ(old_dev, device_b);

    stop.store(true, std::memory_order_relaxed);
    idle_thread.join();
}
