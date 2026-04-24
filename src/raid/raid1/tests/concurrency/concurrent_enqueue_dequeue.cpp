#include "test_raid1_common.hpp"

#include <atomic>
#include <barrier>
#include <boost/uuid/string_generator.hpp>
#include <thread>

#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_impl.hpp"
#include "raid/raid1/raid1_resync_task.hpp"

using namespace std::chrono_literals;
using namespace ublkpp::raid1;

// Regression test for: dequeue_write() + __resume() race where the resync could run
// concurrently with an in-flight write.
//
// Root cause: the old dequeue_write() was two steps:
//   1. fetch_sub → counter: 1→0
//   2. (window) concurrent enqueue_write(): counter 0→1, __pause() finds state==PAUSE
//      and early-exits — trusting the existing pause
//   3. __resume() fires: CAS PAUSE→ACTIVE
//   Result: resync runs with a write still conceptually in flight.
//
// Fix: dec_xchng_status_ifz() atomically packs the decrement and PAUSE→ACTIVE into a
// single CAS. If a concurrent enqueue increments before that CAS, the counter is >0
// and no state transition occurs. If the CAS fires first, state is already ACTIVE when
// the new enqueue runs, so __pause() correctly waits for SLEEPING rather than
// early-exiting on the stale PAUSE.
//
// Test strategy: launch resync with slow copies (1ms delay) so it runs long enough
// for the race window to be reached. From N I/O threads, rapidly fire enqueue_write /
// dequeue_write pairs. The mock write callback checks whether any test-tracked write
// is in flight — a positive means resync ran while a write owned the pause.
//
// Ordering contract:
//   enqueue_write()         — pause is established; resync may not run past this point
//   writes_in_flight.fetch_add  — record that we are in-flight
//   [write runs]
//   writes_in_flight.fetch_sub  — mark flight complete
//   dequeue_write()         — release the pause
//
// This bracket guarantees: if the mock write fires while writes_in_flight > 0, the
// invariant was violated. The ordering avoids false positives (no inflation before
// enqueue returns) and detects the bug (inflation happens before __resume() fires).
//
// TSAN on nublox2_dev is the authoritative correctness signal for this race.
TEST(Raid1Concurrency, EnqueueDequeueRace) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAB, iovecs->iov_len);
            std::this_thread::sleep_for(1ms);
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });

    std::atomic< int > writes_in_flight{0};
    std::atomic< bool > overlap_detected{false};

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            if (writes_in_flight.load(std::memory_order_acquire) > 0) {
                overlap_detected.store(true, std::memory_order_release);
            }
            std::this_thread::sleep_for(1ms);
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
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

    constexpr uint32_t io_size = 512 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    std::this_thread::sleep_for(10ms); // Allow resync to start copying

    constexpr int k_n_threads = 4;
    constexpr int k_iterations = 200;
    std::vector< std::thread > io_threads;
    io_threads.reserve(k_n_threads);

    for (int i = 0; i < k_n_threads; ++i) {
        io_threads.emplace_back([&] {
            for (int j = 0; j < k_iterations; ++j) {
                task.enqueue_write();
                // Only count as "in flight" after enqueue returns — by that point
                // __pause() has completed and the resync is guaranteed to be PAUSED.
                // This prevents false positives from the __pause() spin window.
                writes_in_flight.fetch_add(1, std::memory_order_release);

                std::this_thread::yield();

                // Decrement before dequeue: resync cannot restart until dequeue fires,
                // so the mock can't see a spurious writes_in_flight > 0.
                writes_in_flight.fetch_sub(1, std::memory_order_release);
                task.dequeue_write();
            }
        });
    }

    for (auto& t : io_threads)
        t.join();
    task.stop();

    EXPECT_FALSE(overlap_detected.load()) << "Resync ran while writes were in flight — "
                                             "pause/resume race condition detected";
}
