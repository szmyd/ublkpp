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

// Regression test for: concurrent launch() calls racing on _resync_task assignment → std::terminate().
//
// Root cause: launch() used CAS(IDLE→IDLE) which provides no mutual exclusion. Two callers
// (e.g. __become_degraded on the I/O thread and swap_device on the control thread) could both
// pass the CAS simultaneously and race on the `_resync_task = std::thread{...}` assignment.
// Assigning to a joinable std::thread calls std::terminate().
//
// The fix: _launch_lock mutex serializes concurrent launch() (and stop()) callers so only one
// thread wins the CAS and assigns _resync_task at a time.
//
// Test strategy: use std::barrier to fire two launch() calls simultaneously from two threads.
// An empty bitmap means resync threads complete immediately so the test doesn't block. Both
// threads join, then stop() is called. No crash = test passes.
TEST(Raid1Concurrency, ConcurrentLaunch) {
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

    // Empty bitmap (no dirty pages) so the resync threads exit immediately.
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());

    constexpr uint32_t io_size = 4 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    std::barrier sync_point{3};
    auto t1 = std::thread([&] {
        sync_point.arrive_and_wait();
        task.launch(test_uuid, mirror_a, mirror_b, [] {});
    });
    auto t2 = std::thread([&] {
        sync_point.arrive_and_wait();
        task.launch(test_uuid, mirror_a, mirror_b, [] {});
    });
    sync_point.arrive_and_wait();
    t1.join();
    t2.join();
    task.stop();
}
