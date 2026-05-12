#include "test_raid1_common.hpp"

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

    // Hold chunk 0 in-flight. Resync must skip it but still copy chunk 1.
    task.enqueue_write(0, k_chunk_size);
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Wait until resync has completed at least two sweeps (chunk 1 must have been copied by then).
    auto const deadline = std::chrono::steady_clock::now() + 5000ms;
    while (task.yield_count() < 2 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    ASSERT_GE(task.yield_count(), 2U) << "Resync must have swept at least twice within the timeout";

    // chunk 1 must be clean; chunk 0 still dirty.
    EXPECT_GT(bitmap->dirty_pages(), 0U) << "Bitmap must remain dirty while chunk 0 write is held";

    // Verify chunk 1 was copied correctly.
    std::vector< uint8_t > actual1(k_chunk_size, 0);
    int verify_fd = open(dirty_path.c_str(), O_RDONLY);
    ASSERT_GE(verify_fd, 0);
    pread(verify_fd, actual1.data(), k_chunk_size, k_data_offset + k_chunk_size);
    close(verify_fd);
    EXPECT_EQ(chunk1_data, actual1) << "chunk 1 must be copied while chunk 0 is held";

    // Release chunk 0 — resync can now copy it and the bitmap clears.
    task.dequeue_write(0, k_chunk_size);
    EXPECT_TRUE(wait_for_bitmap_clean(bitmap)) << "Bitmap must become clean after releasing chunk 0 write";

    // Verify chunk 0 was also copied correctly.
    std::vector< uint8_t > actual0(k_chunk_size, 0);
    verify_fd = open(dirty_path.c_str(), O_RDONLY);
    ASSERT_GE(verify_fd, 0);
    pread(verify_fd, actual0.data(), k_chunk_size, k_data_offset);
    close(verify_fd);
    EXPECT_EQ(chunk0_data, actual0) << "chunk 0 must be correctly copied after the write is released";

    task.stop();
    std::filesystem::remove(clean_path);
    std::filesystem::remove(dirty_path);
}
