#include "test_raid1_common.hpp"

#include <atomic>
#include <boost/uuid/string_generator.hpp>
#include <thread>

#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_impl.hpp"
#include "raid/raid1/raid1_resync_task.hpp"

using namespace std::chrono_literals;
using namespace ublkpp::raid1;

// Regression test for: Raid1ResyncTask::launch() crash on joinable thread reassignment.
//
// Root cause: stop() is only called during swap or shutdown — never between resyncs. So
// after any completed resync, _resync_task remains joinable (join()/detach() not called).
// The next launch() call will assign to a joinable std::thread, which calls
// std::terminate() -> abort() -> SIGABRT.
//
// The fix joins _resync_task in launch() before assigning the new thread.
//
// Test strategy: use Raid1ResyncTask directly with an empty bitmap so the resync thread
// completes immediately. After the first launch() completes, _resync_task is joinable but
// state is IDLE. The second launch() exercises the assignment — without the fix this
// crashes; with the fix it joins first and succeeds.
TEST(Raid1Concurrency, ResyncRelaunchAfterComplete) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    EXPECT_CALL(*device_a, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);

    // Empty bitmap (no dirty pages) so the resync thread exits immediately.
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());

    constexpr uint32_t io_size = 4 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    // Track when the resync complete callback fires (called just before state→IDLE CAS).
    std::atomic< bool > first_complete{false};
    task.launch(test_uuid, mirror_a, mirror_b, [&first_complete] {
        first_complete.store(true, std::memory_order_release);
        return true;
    });

    // Wait for the complete callback then sleep briefly to let the IDLE CAS happen.
    // _resync_task is joinable for the entire period from launch() until stop() — which
    // is never called between resyncs in production.
    auto const deadline = std::chrono::steady_clock::now() + 2s;
    while (!first_complete.load(std::memory_order_acquire)) {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "Resync did not complete";
        std::this_thread::sleep_for(1ms);
    }
    // Brief sleep so the IDLE CAS in _start() can execute before we call launch() again.
    std::this_thread::sleep_for(5ms);

    // Second launch(): state is IDLE, _resync_task is joinable (never joined).
    // Without the fix: std::thread::operator= on joinable thread → std::terminate() → crash.
    // With the fix: joins first, then starts new thread cleanly.
    task.launch(test_uuid, mirror_a, mirror_b, [] { return true; });

    task.stop();
}

// Regression test for GH #205.
//
// Root cause: stop() CAS's IDLE→STOPPING when the resync thread has already finished
// (state=IDLE, thread still joinable). State gets stuck at STOPPING with no running thread
// to advance it back. The next launch() call spins forever in __transition_to.
//
// This sequence occurs in swap_device():
//   toggle_resync(false)  [stop hits IDLE+joinable → CAS IDLE→STOPPING]
//   __swap_device()       [sets route≠EITHER, dirties bitmap]
//   toggle_resync(true)   [launch() loops forever on STOPPING]
//
// Without the fix: launch() spins forever and the test fails after 2s.
// With the fix: stop() returns SUCCESS from IDLE+joinable (no CAS), state stays IDLE,
// and the second launch() starts the new resync normally.
//
// task is a shared_ptr so the detached stuck thread (in the failing case) holds a reference
// and never accesses freed memory, avoiding UB after the test body returns.
TEST(Raid1Concurrency, StopAndRelaunchAfterComplete) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    EXPECT_CALL(*device_a, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());

    constexpr uint32_t io_size = 4 * Ki;
    auto task = std::make_shared< Raid1ResyncTask >(bitmap, Bitmap::page_size(), io_size, io_size);

    // First launch: empty bitmap → resync completes immediately.
    std::atomic< bool > first_complete{false};
    task->launch(test_uuid, mirror_a, mirror_b, [&first_complete] {
        first_complete.store(true, std::memory_order_release);
        return true;
    });

    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!first_complete.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    ASSERT_TRUE(first_complete.load(std::memory_order_acquire)) << "First resync did not complete";

    // Let the IDLE CAS in _start() execute before stop() runs, landing in the
    // IDLE+joinable window that triggers the bug.
    std::this_thread::sleep_for(5ms);

    // Mirrors swap_device(): stop the old resync, then relaunch for the new device.
    task->stop();

    // Run the second launch() in a thread so we can detect if it hangs.
    // Without the fix: stop() left state=STOPPING and launch() spins forever.
    auto launched = std::make_shared< std::atomic< bool > >(false);
    auto second_complete = std::make_shared< std::atomic< bool > >(false);
    auto t = std::thread([task, launched, second_complete, mirror_a, mirror_b]() mutable {
        task->launch(test_uuid, mirror_a, mirror_b, [second_complete] {
            second_complete->store(true, std::memory_order_release);
            return true;
        });
        launched->store(true, std::memory_order_release);
    });

    deadline = std::chrono::steady_clock::now() + 2s;
    while (!launched->load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);

    if (!launched->load(std::memory_order_acquire)) {
        // launch() is stuck. Detach: shared_ptr keeps task alive so the spinning thread
        // never touches freed memory. The test process cleans up on exit.
        t.detach();
        FAIL() << "launch() hung after stop()+relaunch — GH #205 IDLE→STOPPING race in stop()";
        return;
    }
    t.join();

    deadline = std::chrono::steady_clock::now() + 2s;
    while (!second_complete->load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    EXPECT_TRUE(second_complete->load(std::memory_order_acquire)) << "Second resync did not call complete()";

    task->stop();
}

// Regression test for H4: complete() returning false must cause _start() to re-run __run().
//
// Simulates the concurrent-dirty_region race deterministically: the complete callback
// re-dirties one page before returning false on the first call, then returns true on the
// second. The test asserts the bitmap drains to zero — proving the loop re-entered __run()
// and synced the re-dirtied region rather than exiting with dirty bits orphaned.
TEST(Raid1Concurrency, BecomeCleanReturnsFalseLoopsAndDrains) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    EXPECT_CALL(*device_a, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);

    auto superbitmap_buf = make_test_superbitmap();
    // chunk_size == io_size so clean_region(addr, sz) receives a chunk-aligned len.
    constexpr uint32_t chunk_size = 4 * Ki;
    constexpr uint32_t io_size = chunk_size;
    auto bitmap = std::make_shared< Bitmap >(Gi, chunk_size, 4 * Ki, superbitmap_buf.get());
    bitmap->dirty_region(0, chunk_size); // one dirty chunk so __run() has work to do

    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    std::atomic< int > call_count{0};
    task.launch(test_uuid, mirror_a, mirror_b, [&bitmap, &call_count]() -> bool {
        if (call_count.fetch_add(1, std::memory_order_relaxed) == 0) {
            // First call: re-dirty to simulate a concurrent backup-fail dirty_region().
            // Returning false tells _start() to loop __run() and drain the new dirty bit.
            bitmap->dirty_region(0, chunk_size);
            return false;
        }
        return true; // second call: clean
    });

    auto const deadline = std::chrono::steady_clock::now() + 5s;
    while (call_count.load(std::memory_order_acquire) < 2 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);

    task.stop();
    EXPECT_EQ(2, call_count.load(std::memory_order_relaxed)) << "complete() must be called twice";
    EXPECT_EQ(0UL, bitmap->dirty_pages()) << "bitmap must be empty after both calls complete";
}
