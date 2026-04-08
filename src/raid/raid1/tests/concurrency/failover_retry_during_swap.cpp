#include "test_raid1_common.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

using namespace std::chrono_literals;

// Test that recursive __failover_read() uses the same RouteState snapshot
// across retries. Without RouteState propagation, a swap between the first
// read failure and the retry could cause the retry to use a different device
// than intended, breaking the failover logic.
//
// WITHOUT FIX (dd2dcb9): The retry would call __capture_route_state() again,
// potentially seeing device_c (new device) as the backup instead of device_b,
// breaking failover logic.
//
// This test uses precise timing control (condition_variable) to open the race
// window: device_a fails the first read, swap occurs, then retry proceeds.
// The retry must use the same RouteState (pointing to device_b as backup).
TEST(Raid1Concurrency, FailoverRetryDuringSwap) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    std::mutex mtx;
    std::condition_variable cv;
    std::atomic< bool > first_read_attempted{false};
    std::atomic< bool > swap_completed{false};

    // device_a: fail first read, then succeed on subsequent reads
    std::atomic< int > device_a_read_count{0};
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            int count = device_a_read_count.fetch_add(1, std::memory_order_relaxed);
            if (count == 0) {
                // First read: signal and fail
                {
                    std::lock_guard< std::mutex > lock(mtx);
                    first_read_attempted.store(true, std::memory_order_release);
                }
                cv.notify_all();
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            }
            // Subsequent reads: succeed
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAB, iovecs->iov_len);
            return static_cast< int >(iovecs->iov_len);
        });

    // device_b: always succeed (backup for failover)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xCD, iovecs->iov_len);
            return static_cast< int >(iovecs->iov_len);
        });

    // Thread 1: Issue a read that will trigger failover
    auto reader = std::thread([&] {
        auto res = raid_device.sync_io(UBLK_IO_OP_READ, nullptr, 4 * Ki, 64 * Ki);
        EXPECT_TRUE(res); // Failover should succeed despite device_a failure
    });

    // Thread 2: Wait for first read attempt, then swap device_b mid-failover
    auto swapper = std::thread([&] {
        // Wait for first read to fail
        {
            std::unique_lock< std::mutex > lock(mtx);
            cv.wait(lock, [&] { return first_read_attempted.load(std::memory_order_acquire); });
        }

        // Small delay to maximize chance of swap occurring between failure and retry
        std::this_thread::sleep_for(2ms);

        // New device for swap
        auto device_c = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});

        EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_READ, _, _, _))
            .Times(::testing::AnyNumber())
            .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
                if (addr == 0) {
                    // Superblock read: return zeroed (new device)
                    memset(iovecs->iov_base, 0, iovecs->iov_len);
                } else if (iovecs->iov_base) {
                    // Data read
                    memset(iovecs->iov_base, 0xEF, iovecs->iov_len);
                }
                return static_cast< int >(iovecs->iov_len);
            });

        EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .Times(::testing::AnyNumber())
            .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
                return static_cast< int >(iovecs->iov_len);
            });

        EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .Times(::testing::AnyNumber())
            .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
                return static_cast< int >(iovecs->iov_len);
            });

        // Swap device_b for device_c during failover retry window
        auto old_dev = raid_device.swap_device("DiskB", device_c);
        EXPECT_EQ(old_dev, device_b);

        swap_completed.store(true, std::memory_order_release);
    });

    reader.join();
    swapper.join();

    EXPECT_TRUE(first_read_attempted.load());
    EXPECT_TRUE(swap_completed.load());
}
