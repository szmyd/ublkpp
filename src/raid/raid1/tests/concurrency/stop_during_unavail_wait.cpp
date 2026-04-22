#include "test_raid1_common.hpp"

#include <atomic>
#include <boost/uuid/string_generator.hpp>
#include <thread>

#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_impl.hpp"
#include "raid/raid1/raid1_resync_task.hpp"

using namespace std::chrono_literals;
using namespace ublkpp::raid1;

// Regression test for: Raid1ResyncTask::stop() hangs when task is spinning in unavail wait loop.
//
// Root cause: stop() checked state == ACTIVE before CASing to STOPPING. When the resync thread
// was blocked in the unavail wait loop, state was IDLE (set at entry to _start() before the
// unavail check), so stop() returned SUCCESS without CASing. join() then blocked forever because
// the thread never saw STOPPING.
//
// The fix: stop() CASes IDLE→STOPPING (in addition to ACTIVE→STOPPING) so the unavail-wait
// branch of _start() can detect STOPPING and exit.
//
// Test strategy: mark mirror_b unavail and mock its sync_iov to always return EIO so the probe
// read fails and the thread stays in the unavail loop. Call stop() from a separate thread and
// assert it completes within 15s (worst case: one full avail_delay sleep of 5s plus margin).
TEST(Raid1Concurrency, StopDuringUnavailWait) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    EXPECT_CALL(*device_a, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    // First call: superblock read during MirrorDevice construction — must succeed.
    // Subsequent calls: probe reads during unavail loop — always fail to keep mirror_b unavail.
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> ublkpp::io_result {
            if (iovecs->iov_base) memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return static_cast< int >(iovecs->iov_len);
        })
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) -> ublkpp::io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);
    mirror_b->unavail.test_and_set(std::memory_order_release);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());

    constexpr uint32_t io_size = 4 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    std::this_thread::sleep_for(50ms);

    std::atomic< bool > stop_complete{false};
    auto stop_thread = std::thread([&] {
        task.stop();
        stop_complete.store(true, std::memory_order_release);
    });

    auto const deadline = std::chrono::steady_clock::now() + 15s;
    while (!stop_complete.load(std::memory_order_acquire)) {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline)
            << "stop() hung — resync task did not detect STOPPING during unavail wait";
        std::this_thread::sleep_for(10ms);
    }
    stop_thread.join();
}
