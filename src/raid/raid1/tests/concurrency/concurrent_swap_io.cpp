#include "test_raid1_common.hpp"

#include <atomic>
#include <thread>

using namespace std::chrono_literals;

// Verify that concurrent sync reads and swap_device do not crash, produce
// use-after-free errors, or route I/O to wrong devices.  The double-read
// retry in __capture_route_state() and the use of shared_ptrs in RouteState
// are what make this safe.
TEST(Raid1, ConcurrentSwapAndSyncRead) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Both devices serve reads for the duration of the test
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAB, iovecs->iov_len);
            return static_cast< int >(iovecs->iov_len);
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xCD, iovecs->iov_len);
            return static_cast< int >(iovecs->iov_len);
        });

    // Background thread: issue sync reads continuously while the swap runs
    std::atomic< bool > stop{false};
    std::atomic< int >  read_count{0};
    auto reader = std::thread([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            auto res = raid_device.sync_io(UBLK_IO_OP_READ, nullptr, 4 * Ki, 64 * Ki);
            EXPECT_TRUE(res);
            read_count.fetch_add(1, std::memory_order_relaxed);
        }
    });

    // Give the reader a moment to start before the swap races with it
    std::this_thread::sleep_for(5ms);

    // New device: returns a zeroed superblock so it is treated as a new_device
    // (triggers full bitmap dirty + resync on the new device after swap)
    auto device_c = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});

    EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 0, iovecs->iov_len);
            return static_cast< int >(iovecs->iov_len);
        });

    // device_a receives the superblock write (staying device during swap)
    // device_c receives bitmap page writes and its own superblock write
    // Use AnyNumber so the destructor's unmount_clean writes are also covered
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

    // Swap device_b for device_c while the reader thread is active
    auto old_dev = raid_device.swap_device("DiskB", device_c);
    EXPECT_EQ(old_dev, device_b);

    // Let reads continue briefly after the swap to exercise the updated routing
    std::this_thread::sleep_for(5ms);
    stop.store(true, std::memory_order_relaxed);
    reader.join();

    EXPECT_GT(read_count.load(), 0);

    // After swap, device_a is the clean active device
    auto const states = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, states.device_a);
}
