#include "test_raid1_common.hpp"

#include <atomic>
#include <barrier>
#include <boost/uuid/string_generator.hpp>
#include <thread>

#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_impl.hpp"
#include "raid/raid1/raid1_resync_task.hpp"
#include "raid/raid1/raid1_superblock.hpp"

using namespace std::chrono_literals;
using namespace ublkpp::raid1;

// Verify that concurrent enqueue_write/dequeue_write pairs do not race with resync:
// resync must never write the data region for [write_lba, write_lba + io_size) while a
// write is tracked in-flight for that range.
//
// With per-region tracking, resync skips exactly the conflicting chunks via Phase 1 and
// Phase 2 overlap checks; unrelated chunks in the same dirty run may still be copied.
//
// Detection: IO threads bracket writes_in_flight around enqueue/dequeue.  The device_b
// write mock converts the raw disk address back to a logical offset (by subtracting
// _offset = Bitmap::page_size()) and flags a violation if that offset falls inside the
// write range and writes_in_flight > 0.
//
// TSAN is the authoritative signal for data races on the RegionTracker itself.
TEST(Raid1Concurrency, EnqueueDequeueRace) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAB, iovecs->iov_len);
            std::this_thread::sleep_for(1ms);
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });

    // IO threads write to this LBA range; resync must not copy it while writes are in-flight.
    // Each thread uses its own LBA range [tid * io_size, (tid+1) * io_size) so that no two
    // threads ever have concurrent in-flight writes to the same LBA — which would violate
    // block-device ordering and trigger spurious CAS failures in RegionTracker::untrack().
    constexpr uint32_t io_size = 512 * Ki;
    constexpr int k_n_threads = 4;

    // Per-thread in-flight counters; indexed by tid = logical_off / io_size.
    std::array< std::atomic< int >, k_n_threads > thread_in_flight{};
    for (auto& c : thread_in_flight)
        c.store(0, std::memory_order_relaxed);

    std::atomic< bool > overlap_detected{false};
    std::atomic< bool > resync_started{false};

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t raw_addr) -> ublkpp::io_result {
            resync_started.store(true, std::memory_order_release);
            // Convert raw disk address to logical offset (subtract the reserved bitmap area).
            // Determine which thread "owns" this LBA and flag a violation if that thread has
            // an in-flight write while resync copies it.
            auto const logical_off = static_cast< uint64_t >(raw_addr) - k_page_size;
            auto const tid = static_cast< int >(logical_off / io_size);
            if (tid < k_n_threads && thread_in_flight[tid].load(std::memory_order_acquire) > 0)
                overlap_detected.store(true, std::memory_order_release);
            std::this_thread::sleep_for(1ms);
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());
    bitmap->dirty_region(0, Gi);

    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    while (!resync_started.load(std::memory_order_acquire))
        std::this_thread::yield(); // Wait until resync has begun copying to device_b

    constexpr int k_iterations = 200;
    std::vector< std::thread > io_threads;
    io_threads.reserve(k_n_threads);

    for (int i = 0; i < k_n_threads; ++i) {
        io_threads.emplace_back([&, tid = i] {
            auto const lba = static_cast< uint64_t >(tid) * io_size;
            for (int j = 0; j < k_iterations; ++j) {
                task.enqueue_write(lba, io_size);
                // Only count as "in flight" after enqueue returns — by that point the
                // write is registered in the tracker and resync will skip this range.
                thread_in_flight[tid].fetch_add(1, std::memory_order_release);

                std::this_thread::yield();

                // Decrement before dequeue so the mock can't see a spurious > 0 after
                // the range is unregistered.
                thread_in_flight[tid].fetch_sub(1, std::memory_order_release);
                task.dequeue_write(lba, io_size);
            }
        });
    }

    for (auto& t : io_threads)
        t.join();
    task.stop();

    EXPECT_FALSE(overlap_detected.load()) << "Resync wrote to a region while a write was in-flight for that range";
}
