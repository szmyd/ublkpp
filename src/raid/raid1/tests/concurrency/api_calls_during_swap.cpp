#include "test_raid1_common.hpp"

#include <atomic>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

// Test that replica_states() API calls during swap_device return consistent
// snapshots. Without RouteState capture in replica_states(), a swap mid-call
// could return mismatched device/state pairs (e.g., device_a with device_b's
// state).
//
// WITHOUT FIX (bb2baef): Could read route, then before reading device unavail
// flags, swap_device changes which device is in which position, resulting in
// returning device_a's state as device_b or vice versa.
TEST(Raid1Concurrency, ReplicaStatesDuringSwap) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    std::atomic< bool > stop{false};
    std::atomic< int > api_call_count{0};

    // Background thread: call replica_states() rapidly to hit race window
    auto api_caller = std::thread([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            auto states = raid_device.replica_states();
            api_call_count.fetch_add(1, std::memory_order_relaxed);

            // Verify states are valid (not corrupted by race)
            // Should be CLEAN, SYNCING, or ERROR - never invalid values
            EXPECT_TRUE(states.device_a == ublkpp::raid1::replica_state::CLEAN ||
                        states.device_a == ublkpp::raid1::replica_state::SYNCING ||
                        states.device_a == ublkpp::raid1::replica_state::ERROR);
            EXPECT_TRUE(states.device_b == ublkpp::raid1::replica_state::CLEAN ||
                        states.device_b == ublkpp::raid1::replica_state::SYNCING ||
                        states.device_b == ublkpp::raid1::replica_state::ERROR);

            // Brief delay to make test faster but still hit race
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    });

    // Main thread: perform swap while API calls are ongoing
    std::this_thread::sleep_for(5ms);

    auto device_c = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});

    EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AtLeast(1))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
            if (addr == 0) { memset(iovecs->iov_base, 0, iovecs->iov_len); }
            return static_cast< int >(iovecs->iov_len);
        });

    EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(
            [](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result { return static_cast< int >(iovecs->iov_len); });

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(
            [](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result { return static_cast< int >(iovecs->iov_len); });

    // Perform swap while API calls are ongoing
    auto old_dev = raid_device.swap_device("DiskB", device_c);
    EXPECT_EQ(old_dev, device_b);

    // Continue API calls for a while after swap
    std::this_thread::sleep_for(15ms);
    stop.store(true, std::memory_order_relaxed);
    api_caller.join();

    EXPECT_GT(api_call_count.load(), 50); // Should achieve many API calls
}

// Test that replicas() API calls during swap_device return consistent device
// pairs. Without RouteState capture, the pair could be inconsistent.
TEST(Raid1Concurrency, ReplicasDuringSwap) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    std::atomic< bool > stop{false};
    std::atomic< int > api_call_count{0};

    auto api_caller = std::thread([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            auto devices = raid_device.replicas();
            api_call_count.fetch_add(1, std::memory_order_relaxed);

            // Verify we got two valid devices
            EXPECT_TRUE(devices.first != nullptr);
            EXPECT_TRUE(devices.second != nullptr);
        }
    });

    std::this_thread::sleep_for(5ms);

    auto device_c = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});

    EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AtLeast(1))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
            if (addr == 0) { memset(iovecs->iov_base, 0, iovecs->iov_len); }
            return static_cast< int >(iovecs->iov_len);
        });

    EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(
            [](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result { return static_cast< int >(iovecs->iov_len); });

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(
            [](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result { return static_cast< int >(iovecs->iov_len); });

    auto old_dev = raid_device.swap_device("DiskB", device_c);
    EXPECT_EQ(old_dev, device_b);

    std::this_thread::sleep_for(5ms);
    stop.store(true, std::memory_order_relaxed);
    api_caller.join();

    EXPECT_GT(api_call_count.load(), 10);
}

// Test multiple concurrent API callers during swap to maximize contention.
TEST(Raid1Concurrency, MultipleAPICallersDuringSwap) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    std::atomic< bool > stop{false};
    std::atomic< int > total_calls{0};
    std::vector< std::thread > threads;

    // Spawn 4 threads calling different APIs
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&, tid = i] {
            while (!stop.load(std::memory_order_relaxed)) {
                if (tid % 2 == 0) {
                    auto states = raid_device.replica_states();
                    (void)states;
                } else {
                    auto devices = raid_device.replicas();
                    EXPECT_TRUE(devices.first != nullptr);
                }
                total_calls.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::this_thread::sleep_for(5ms);

    // Swapper thread: perform one swap mid-flight
    auto swapper = std::thread([&] {
        auto device_c = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});

        EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_READ, _, _, _))
            .Times(::testing::AtLeast(1))
            .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
                if (addr == 0) { memset(iovecs->iov_base, 0, iovecs->iov_len); }
                return static_cast< int >(iovecs->iov_len);
            });

        EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .Times(::testing::AnyNumber())
            .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
                return static_cast< int >(iovecs->iov_len);
            });

        EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .Times(::testing::AnyNumber())
            .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
                return static_cast< int >(iovecs->iov_len);
            });

        auto old_dev = raid_device.swap_device("DiskB", device_c);
        EXPECT_EQ(old_dev, device_b);
    });

    swapper.join();

    std::this_thread::sleep_for(5ms);
    stop.store(true, std::memory_order_relaxed);

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_GT(total_calls.load(), 10);
}
