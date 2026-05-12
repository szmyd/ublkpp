#include "test_raid1_common.hpp"

#include <array>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <thread>
#include <vector>

extern "C" {
#include <fcntl.h>
#include <unistd.h>
}

#include <boost/uuid/string_generator.hpp>

#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_impl.hpp"
#include "raid/raid1/raid1_resync_task.hpp"
#include "ublkpp/drivers.hpp"

using namespace std::chrono_literals;
using namespace ublkpp::raid1;

// File size must be a multiple of max_sectors (1024 sectors = 512 KiB) so FSDisk dev_sectors > 0.
static constexpr size_t k_test_file_size = 512 * Ki;
// Data starts at 2 * k_page_size: first page is the superblock, second is bitmap page 0.
static constexpr uint32_t k_data_offset = 2 * k_page_size;
static constexpr uint32_t k_chunk_size = 32 * Ki;

namespace {

// Creates a temporary file pre-populated with a valid superblock at offset 0. Caller owns cleanup.
std::pair< std::string, int > make_resync_test_file(bool is_device_b = false) {
    char path[] = "/tmp/resync_iouring_XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) return {"", -1};
    if (ftruncate(fd, static_cast< off_t >(k_test_file_size)) != 0) {
        close(fd);
        return {"", -1};
    }
    auto sb = normal_superblock;
    if (is_device_b) sb.fields.device_b = 1;
    if (pwrite(fd, &sb, k_page_size, 0) != static_cast< ssize_t >(k_page_size)) {
        close(fd);
        return {"", -1};
    }
    return {std::string(path), fd};
}

bool wait_for_bitmap_clean(std::shared_ptr< Bitmap > const& bitmap, std::chrono::milliseconds timeout = 5000ms) {
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (0 == bitmap->dirty_pages()) return true;
        std::this_thread::sleep_for(1ms);
    }
    return false;
}

} // namespace

// Verify that the io_uring linked-SQE path copies data correctly when both mirror disks
// expose real backing fds (via FSDisk::backend_fd()). This is the fundamental Phase 2 check:
// data written to the clean mirror must appear byte-for-byte on the dirty mirror after resync.
TEST(AsyncResyncIoUring, BasicCopy) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0) << "Failed to create clean temp file";
    ASSERT_GE(dirty_raw_fd, 0) << "Failed to create dirty temp file";

    // Pre-fill the data region of the clean mirror with a recognisable byte pattern.
    constexpr uint8_t k_pattern = 0xCA;
    std::vector< uint8_t > source(k_chunk_size, k_pattern);
    ASSERT_EQ(static_cast< ssize_t >(k_chunk_size), pwrite(clean_raw_fd, source.data(), k_chunk_size, k_data_offset));
    close(clean_raw_fd);
    close(dirty_raw_fd);

    auto disk_clean = ublkpp::make_fs_disk(clean_path);
    auto disk_dirty = ublkpp::make_fs_disk(dirty_path);

    // io_uring path requires both disks to expose valid fds.
    ASSERT_GE(disk_clean->backend_fd(), 0) << "FSDisk must expose backing fd for io_uring resync";
    ASSERT_GE(disk_dirty->backend_fd(), 0);

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, disk_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, disk_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    bitmap->dirty_region(0, k_chunk_size);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap)) << "Resync must complete; bitmap must become clean";
    task.stop();

    // Verify the dirty mirror received a byte-for-byte copy.
    std::vector< uint8_t > actual(k_chunk_size, 0);
    int verify_fd = open(dirty_path.c_str(), O_RDONLY);
    ASSERT_GE(verify_fd, 0);
    ASSERT_EQ(static_cast< ssize_t >(k_chunk_size), pread(verify_fd, actual.data(), k_chunk_size, k_data_offset));
    close(verify_fd);
    EXPECT_EQ(source, actual) << "io_uring resync must produce byte-identical data on dirty mirror";

    std::filesystem::remove(clean_path);
    std::filesystem::remove(dirty_path);
}

// Verify that stop() returns promptly while the resync task is actively copying via io_uring.
// The 500 µs io_uring submit_and_wait_timeout ensures the resync loop wakes within that window
// after STOPPING is set. Bound conservatively at 1000 ms to tolerate CI scheduling jitter.
TEST(AsyncResyncIoUring, StopResponsive) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    close(clean_raw_fd);
    close(dirty_raw_fd);

    auto disk_clean = ublkpp::make_fs_disk(clean_path);
    auto disk_dirty = ublkpp::make_fs_disk(dirty_path);

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, disk_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, disk_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    // Dirty all chunks so resync has work to do when stop() fires.
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    bitmap->dirty_region(0, k_test_file_size - k_data_offset);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Give resync one sweep to start before calling stop().
    while (task.yield_count() < 1)
        std::this_thread::sleep_for(1ms);

    auto const before = std::chrono::steady_clock::now();
    task.stop();
    auto const elapsed = std::chrono::steady_clock::now() - before;

    EXPECT_LT(elapsed, 1000ms) << "stop() must return promptly; io_uring submit_and_wait_timeout provides the wakeup";

    std::filesystem::remove(clean_path);
    std::filesystem::remove(dirty_path);
}

// Verify that per-region conflict tracking (Phase 1) works correctly with the io_uring copy path.
// With two dirty chunks [0, chunk) and [chunk, 2*chunk), and chunk 0 held by an in-flight write:
//   - chunk 0 must NOT be copied while held (Phase 1 skip)
//   - chunk 1 MUST be copied in the same pass (independent region)
//   - After releasing the hold, chunk 0 must eventually be copied and the bitmap cleared.
TEST(AsyncResyncIoUring, ConflictIntegration) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);

    // Fill both chunks of the clean mirror with distinct patterns.
    constexpr uint8_t k_pat0 = 0xAA;
    constexpr uint8_t k_pat1 = 0xBB;
    std::vector< uint8_t > chunk0_data(k_chunk_size, k_pat0);
    std::vector< uint8_t > chunk1_data(k_chunk_size, k_pat1);
    ASSERT_EQ(static_cast< ssize_t >(k_chunk_size),
              pwrite(clean_raw_fd, chunk0_data.data(), k_chunk_size, k_data_offset));
    ASSERT_EQ(static_cast< ssize_t >(k_chunk_size),
              pwrite(clean_raw_fd, chunk1_data.data(), k_chunk_size, k_data_offset + k_chunk_size));
    close(clean_raw_fd);
    close(dirty_raw_fd);

    auto disk_clean = ublkpp::make_fs_disk(clean_path);
    auto disk_dirty = ublkpp::make_fs_disk(dirty_path);

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, disk_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, disk_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    // Dirty both chunks.
    bitmap->dirty_region(0, 2 * k_chunk_size);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};

    // RAII guard: releases the write hold if the test exits early via ASSERT, preventing
    // the task destructor from blocking forever in join() with chunk 0 still held.
    ResyncWriteGuard write_guard{task, 0, k_chunk_size};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Wait until resync has completed at least two sweeps (chunk 1 must have been copied by then).
    auto const deadline = std::chrono::steady_clock::now() + 5000ms;
    while (task.yield_count() < 2 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    ASSERT_GE(task.yield_count(), 2U) << "Resync must have swept at least twice within the timeout";

    // chunk 1 must be clean; chunk 0 still dirty.
    EXPECT_GT(bitmap->dirty_pages(), 0U) << "Bitmap must remain dirty while chunk 0 write is held";

    // Verify chunk 1 was copied correctly.
    {
        std::vector< uint8_t > actual1(k_chunk_size, 0);
        int verify_fd = open(dirty_path.c_str(), O_RDONLY);
        ASSERT_GE(verify_fd, 0);
        ASSERT_EQ(static_cast< ssize_t >(k_chunk_size),
                  pread(verify_fd, actual1.data(), k_chunk_size, k_data_offset + k_chunk_size));
        close(verify_fd);
        EXPECT_EQ(chunk1_data, actual1) << "chunk 1 must be copied while chunk 0 is held";
    }

    // Release chunk 0 — resync can now copy it and the bitmap clears.
    write_guard.release();
    EXPECT_TRUE(wait_for_bitmap_clean(bitmap)) << "Bitmap must become clean after releasing chunk 0 write";

    // Verify chunk 0 was also copied correctly.
    {
        std::vector< uint8_t > actual0(k_chunk_size, 0);
        int verify_fd = open(dirty_path.c_str(), O_RDONLY);
        ASSERT_GE(verify_fd, 0);
        ASSERT_EQ(static_cast< ssize_t >(k_chunk_size), pread(verify_fd, actual0.data(), k_chunk_size, k_data_offset));
        close(verify_fd);
        EXPECT_EQ(chunk0_data, actual0) << "chunk 0 must be correctly copied after the write is released";
    }

    task.stop();
    std::filesystem::remove(clean_path);
    std::filesystem::remove(dirty_path);
}

// Verify the sync_iov fallback is used when backend_fd() returns -1 (TestDisk default).
// Guards the __run() dispatch branch: if either mirror returns -1 from backend_fd(), the
// task must use sync_iov rather than io_uring and still complete the resync correctly.
// If the dispatch condition is broken (io_uring attempted with fd=-1), io_uring returns
// -EBADF, __copy_region_async returns error, unavail is set, and wait_for_bitmap_clean times out.
TEST(AsyncResyncIoUring, FallbackWhenNoFd) {
    auto device_clean = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskClean"});
    auto device_dirty =
        std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskDirty", .is_slot_b = true});

    std::atomic< int > sync_reads{0};
    EXPECT_CALL(*device_clean, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&sync_reads](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            sync_reads.fetch_add(1, std::memory_order_relaxed);
            if (iovecs->iov_base) memset(iovecs->iov_base, 0, iovecs->iov_len);
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    // The resync task flushes cleared bitmap pages to the clean mirror (clean_region() →
    // sync_iov WRITE at the bitmap page address, which is < k_data_offset). Data-region
    // writes must never go to the clean mirror; assert the offset is within metadata.
    EXPECT_CALL(*device_clean, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            EXPECT_LT(static_cast< uint64_t >(addr), static_cast< uint64_t >(k_data_offset))
                << "Only bitmap-page writes allowed to clean mirror; data-region write is a bug";
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_dirty, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, device_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, device_dirty);
    // Exclude the superblock READ issued by the MirrorDevice constructors.
    sync_reads.store(0, std::memory_order_relaxed);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    bitmap->dirty_region(0, k_chunk_size);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap))
        << "Sync fallback (backend_fd==-1) must complete resync and clean the bitmap";
    task.stop();

    EXPECT_GE(sync_reads.load(), 1)
        << "sync_iov READ must have been called — confirms fallback path was taken, not io_uring";
}

// Verify that all dirty chunks are copied correctly when several are dirty simultaneously.
// BasicCopy tests one chunk; this test uses four distinct byte patterns to catch address
// arithmetic errors (wrong _offset or chunk stride) and buffer-reuse bugs across multiple
// sequential io_uring READ_FIXED → WRITE_FIXED pairs using the same registered buffer.
TEST(AsyncResyncIoUring, MultipleChunkCopy) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);

    constexpr size_t k_num_chunks = 4;
    constexpr std::array< uint8_t, k_num_chunks > k_patterns{0x11, 0x22, 0x33, 0x44};
    for (size_t i = 0; i < k_num_chunks; ++i) {
        std::vector< uint8_t > buf(k_chunk_size, k_patterns[i]);
        ASSERT_EQ(static_cast< ssize_t >(k_chunk_size),
                  pwrite(clean_raw_fd, buf.data(), k_chunk_size, k_data_offset + i * k_chunk_size));
    }
    close(clean_raw_fd);
    close(dirty_raw_fd);

    auto disk_clean = ublkpp::make_fs_disk(clean_path);
    auto disk_dirty = ublkpp::make_fs_disk(dirty_path);

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, disk_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, disk_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    bitmap->dirty_region(0, k_num_chunks * k_chunk_size);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap)) << "All dirty chunks must be resynced";
    task.stop();

    // Verify each chunk has the correct distinct pattern on the dirty mirror.
    int verify_fd = open(dirty_path.c_str(), O_RDONLY);
    ASSERT_GE(verify_fd, 0);
    for (size_t i = 0; i < k_num_chunks; ++i) {
        std::vector< uint8_t > expected(k_chunk_size, k_patterns[i]);
        std::vector< uint8_t > actual(k_chunk_size, 0);
        ASSERT_EQ(static_cast< ssize_t >(k_chunk_size),
                  pread(verify_fd, actual.data(), k_chunk_size, k_data_offset + i * k_chunk_size));
        EXPECT_EQ(expected, actual) << "Chunk " << i << " has wrong pattern after resync";
    }
    close(verify_fd);

    std::filesystem::remove(clean_path);
    std::filesystem::remove(dirty_path);
}

// Verify that the complete() callback fires exactly once after the io_uring resync path
// copies all dirty chunks. All other tests pass [] {} as the callback; this test is the
// only one that asserts the callback is actually invoked via the real-file io_uring path.
TEST(AsyncResyncIoUring, CompleteCallbackFires) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    close(clean_raw_fd);
    close(dirty_raw_fd);

    auto disk_clean = ublkpp::make_fs_disk(clean_path);
    auto disk_dirty = ublkpp::make_fs_disk(dirty_path);

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, disk_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, disk_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    bitmap->dirty_region(0, k_chunk_size);

    std::atomic< int > callback_count{0};
    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    task.launch(test_uuid, mirror_clean, mirror_dirty,
                [&callback_count] { callback_count.fetch_add(1, std::memory_order_release); });

    // Wait for the callback directly — it fires in _start() right after __run() returns,
    // which is after the last clean_region() call that zeroed all dirty bitmap pages.
    auto const deadline = std::chrono::steady_clock::now() + 5000ms;
    while (callback_count.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);

    EXPECT_EQ(1, callback_count.load(std::memory_order_acquire))
        << "complete() must fire exactly once after a successful io_uring resync";
    EXPECT_EQ(0U, bitmap->dirty_pages()) << "Bitmap must be clean when complete() fires";

    task.stop();
    std::filesystem::remove(clean_path);
    std::filesystem::remove(dirty_path);
}

// Verify the io_uring ring lifecycle across a stop+relaunch cycle. The ring is created per
// launch and destroyed on stop; this test confirms the second launch creates a fresh ring,
// resync completes, and data on the dirty mirror is correct after the interrupted first run.
TEST(AsyncResyncIoUring, StopAndRelaunch) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);

    constexpr uint8_t k_pattern = 0xCD;
    std::vector< uint8_t > source(k_chunk_size, k_pattern);
    ASSERT_EQ(static_cast< ssize_t >(k_chunk_size), pwrite(clean_raw_fd, source.data(), k_chunk_size, k_data_offset));
    close(clean_raw_fd);
    close(dirty_raw_fd);

    auto disk_clean = ublkpp::make_fs_disk(clean_path);
    auto disk_dirty = ublkpp::make_fs_disk(dirty_path);

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, disk_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, disk_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};

    // First launch: dirty one chunk and stop after the first sweep; destroys the ring.
    bitmap->dirty_region(0, k_chunk_size);
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});
    while (task.yield_count() < 1)
        std::this_thread::sleep_for(1ms);
    task.stop();

    // Second launch: re-dirty (stop may have partially cleaned) and run to completion.
    // A fresh ring must be initialised; the second resync must produce correct data.
    bitmap->dirty_region(0, k_chunk_size);
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});
    EXPECT_TRUE(wait_for_bitmap_clean(bitmap)) << "Second resync must complete after stop+relaunch";
    task.stop();

    std::vector< uint8_t > actual(k_chunk_size, 0);
    int verify_fd = open(dirty_path.c_str(), O_RDONLY);
    ASSERT_GE(verify_fd, 0);
    ASSERT_EQ(static_cast< ssize_t >(k_chunk_size), pread(verify_fd, actual.data(), k_chunk_size, k_data_offset));
    close(verify_fd);
    EXPECT_EQ(source, actual) << "Data must be correct after stop+relaunch resync cycle";

    std::filesystem::remove(clean_path);
    std::filesystem::remove(dirty_path);
}

// Verify stop() returns promptly when the dirty mirror becomes unavail DURING an active resync
// sweep — i.e. the __run() unavail sleep loop, not _start()'s pre-ACTIVE unavail check.
//
// The existing StopDuringUnavailWait test (stop_during_unavail_wait.cpp) covers _start()'s
// unavail loop (mirror unavail before launch). This test covers the distinct __run() path:
//   1. Mirror starts available  → task becomes ACTIVE and enters __run().
//   2. First write to dirty fails → dirty_mirror->unavail set inside __run().
//   3. Task enters the unavail sleep loop (checks STOPPING every resync_delay ticks).
//   4. stop() must complete within one avail_delay window + margin.
TEST(AsyncResyncIoUring, StopDuringRunUnavail) {
    auto device_clean = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskClean"});
    auto device_dirty =
        std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskDirty", .is_slot_b = true});

    EXPECT_CALL(*device_clean, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    // First call: superblock read during MirrorDevice construction — must succeed.
    // All subsequent calls (copy writes + probe reads): fail to keep the mirror unavail.
    EXPECT_CALL(*device_dirty, sync_iov(::testing::_, _, _, _))
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> ublkpp::io_result {
            if (iovecs->iov_base) memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return static_cast< int >(iovecs->iov_len);
        })
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) -> ublkpp::io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, device_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, device_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    bitmap->dirty_region(0, k_test_file_size - k_data_offset);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Wait until at least one sweep completes — by then the write has failed and the task
    // is spinning in the __run() unavail loop checking STOPPING every resync_delay ticks.
    while (task.yield_count() < 1)
        std::this_thread::sleep_for(1ms);

    auto const before = std::chrono::steady_clock::now();
    task.stop();
    auto const elapsed = std::chrono::steady_clock::now() - before;

    // The __run() unavail loop checks STOPPING every resync_delay (default 300 µs).
    // stop() must complete well within avail_delay (default 5 s). Use 10 s for CI margin.
    EXPECT_LT(elapsed, 10000ms) << "stop() must complete promptly from within the __run() unavail loop";
}
