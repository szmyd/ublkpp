#include "test_raid1_common.hpp"

#include <atomic>
#include <thread>

using namespace std::chrono_literals;

// Test that writes during swap_device do not crash or produce use-after-free
// errors. The __replicate() path captures RouteState, which holds shared_ptrs
// to devices. This test validates that the capture prevents use-after-free
// when a device is swapped mid-replication.
//
// WITHOUT FIX (ff46032): Would crash or use freed memory when swap_device
// replaces a device pointer while __replicate is using it. The shared_ptr
// in RouteState keeps the old device alive during the operation.
TEST(Raid1Concurrency, WriteDuringSwap) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Both devices handle sync writes
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            return static_cast< int >(iovecs->iov_len);
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            return static_cast< int >(iovecs->iov_len);
        });

    // Background thread: issue writes continuously
    std::atomic< bool > stop{false};
    std::atomic< int > write_count{0};
    auto writer = std::thread([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            // Issue write via sync_io (triggers __replicate with RouteState capture)
            auto res = raid_device.sync_io(UBLK_IO_OP_WRITE, nullptr, 4 * Ki, 64 * Ki);
            if (res) { write_count.fetch_add(1, std::memory_order_relaxed); }

            // Small delay to widen the race window
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    // Give the writer time to start and build up some operations
    std::this_thread::sleep_for(10ms);

    // New device for swap (swap while writes are in-flight)
    auto device_c = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});

    // device_c is new (zeroed superblock)
    EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 0, iovecs->iov_len);
            return static_cast< int >(iovecs->iov_len);
        });

    EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            return static_cast< int >(iovecs->iov_len);
        });

    // Swap device_b for device_c while writes are in-flight
    auto old_dev = raid_device.swap_device("DiskB", device_c);
    EXPECT_EQ(old_dev, device_b);

    // Continue writes for a while after swap to ensure overlap
    std::this_thread::sleep_for(20ms);
    stop.store(true, std::memory_order_relaxed);
    writer.join();

    EXPECT_GT(write_count.load(), 10); // Should achieve substantial writes
}

// Test that reads during swap_device do not crash. The __failover_read
// path captures RouteState with device shared_ptrs, ensuring safety even
// if the device is swapped mid-operation.
//
// WITHOUT FIX (ff46032): Would crash or use freed memory when swap_device
// happens during read operation.
TEST(Raid1Concurrency, ReadDuringSwap) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Both devices serve reads
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

    // Background thread: issue reads continuously
    std::atomic< bool > stop{false};
    std::atomic< int > read_count{0};
    auto reader = std::thread([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            // Issue read via sync_io
            auto res = raid_device.sync_io(UBLK_IO_OP_READ, nullptr, 4 * Ki, 64 * Ki);
            if (res) { read_count.fetch_add(1, std::memory_order_relaxed); }

            // Small delay to widen race window
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    std::this_thread::sleep_for(10ms);

    // New device for swap
    auto device_c = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskC"});

    EXPECT_CALL(*device_c, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
            if (addr == 0) {
                memset(iovecs->iov_base, 0, iovecs->iov_len);
            } else if (iovecs->iov_base) {
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

    // Swap device_b for device_c while reads are in-flight
    auto old_dev = raid_device.swap_device("DiskB", device_c);
    EXPECT_EQ(old_dev, device_b);

    std::this_thread::sleep_for(20ms);
    stop.store(true, std::memory_order_relaxed);
    reader.join();

    EXPECT_GT(read_count.load(), 10);
}
