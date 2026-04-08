#include "test_raid1_common.hpp"

#include <atomic>
#include <barrier>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

// Test that multiple concurrent readers can safely access the RAID device
// during swap_device. All readers capture independent RouteState snapshots.
//
// WITHOUT FIX (ff46032): Multiple concurrent readers could crash or see
// inconsistent device pointers when swap_device happens concurrently.
TEST(Raid1Concurrency, MultipleReadersDuringSwap) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Both devices serve reads
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, uint64_t) -> io_result {
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAB, iovecs->iov_len);
            return static_cast< int >(iovecs->iov_len);
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, uint64_t) -> io_result {
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xCD, iovecs->iov_len);
            return static_cast< int >(iovecs->iov_len);
        });

    constexpr int num_readers = 4;
    std::atomic< bool > stop{false};
    std::vector< std::atomic< int > > read_counts(num_readers);
    std::vector< std::thread > readers;

    // Barrier to synchronize thread start (all threads + main)
    std::barrier sync_point(num_readers + 1);

    // Spawn reader threads
    for (int i = 0; i < num_readers; ++i) {
        readers.emplace_back([&, reader_id = i] {
            // Wait for all threads to be ready
            sync_point.arrive_and_wait();

            while (!stop.load(std::memory_order_relaxed)) {
                auto res = raid_device.sync_io(UBLK_IO_OP_READ, nullptr, 4 * Ki, 64 * Ki);
                if (res) { read_counts[reader_id].fetch_add(1, std::memory_order_relaxed); }

                // Small delay to allow swap to happen mid-operation
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        });
    }

    // Wait for all reader threads to be ready, then release them
    sync_point.arrive_and_wait();

    // Let readers run for a bit
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

    // Swap device_b for device_c while all readers are active
    auto old_dev = raid_device.swap_device("DiskB", device_c);
    EXPECT_EQ(old_dev, device_b);

    // Continue reads after swap
    std::this_thread::sleep_for(20ms);
    stop.store(true, std::memory_order_relaxed);

    for (auto& t : readers) {
        t.join();
    }

    // Verify each reader performed reads
    for (int i = 0; i < num_readers; ++i) {
        EXPECT_GT(read_counts[i].load(), 5) << "Reader " << i << " performed insufficient reads";
    }
}
