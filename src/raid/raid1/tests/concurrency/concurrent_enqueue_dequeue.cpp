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

// Phase 1 + Phase 2 concurrency stress test. TSAN is the authoritative correctness signal.
// Phase 2 unit test: see Phase2CompletedWriteDetected in write_resync_no_pause.cpp.
//
// Exercises concurrent enqueue_write/dequeue_write and resync under TSAN to catch data races
// in RegionTracker::track/untrack/overlaps and the shadow log protocol. Resync skips exactly
// the conflicting chunks via Phase 1 and Phase 2; unrelated chunks copy concurrently.
//
// Each IO thread owns a distinct LBA range so no two threads ever have concurrent in-flight
// writes to the same LBA — block-device ordering forbids that in production.
TEST(Raid1Concurrency, EnqueueDequeueRace) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    constexpr uint32_t io_size = 512 * Ki;
    constexpr int k_n_threads = 4;

    std::atomic< bool > resync_started{false};

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            resync_started.store(true, std::memory_order_release);
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAB, iovecs->iov_len);
            std::this_thread::sleep_for(1ms);
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
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
    task.launch(test_uuid, mirror_a, mirror_b, [] { return true; });

    while (!resync_started.load(std::memory_order_acquire))
        std::this_thread::yield();

    constexpr int k_iterations = 200;
    std::vector< std::thread > io_threads;
    io_threads.reserve(k_n_threads);

    for (int i = 0; i < k_n_threads; ++i) {
        io_threads.emplace_back([&, tid = i] {
            auto const lba = static_cast< uint64_t >(tid) * io_size;
            for (int j = 0; j < k_iterations; ++j) {
                task.enqueue_write(lba, io_size);
                std::this_thread::yield();
                task.dequeue_write(lba, io_size);
            }
        });
    }

    for (auto& t : io_threads)
        t.join();
    task.stop();

    // Correctness is verified by TSAN detecting data races in RegionTracker.
    SUCCEED();
}
