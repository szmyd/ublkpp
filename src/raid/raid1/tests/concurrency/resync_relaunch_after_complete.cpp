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
// SDSTOR-21864
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

    // All sync I/O succeeds (bitmap page reads/writes during resync)
    EXPECT_CALL(*device_a, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> ublkpp::io_result {
            return static_cast< int >(iovecs->iov_len);
        });
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> ublkpp::io_result {
            return static_cast< int >(iovecs->iov_len);
        });

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
    task.launch(test_uuid, mirror_a, mirror_b,
                [&first_complete] { first_complete.store(true, std::memory_order_release); });

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
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    task.stop();
}
