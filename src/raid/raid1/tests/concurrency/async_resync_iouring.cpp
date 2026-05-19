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

// RAII guard that removes a file path on destruction. Prevents temp file leaks when a test
// exits early via ASSERT_* (which throws, skipping any cleanup at the end of the test body).
struct ScopedTempFile {
    explicit ScopedTempFile(std::string p) noexcept : path(std::move(p)) {}
    ~ScopedTempFile() {
        if (!path.empty()) std::filesystem::remove(path);
    }
    ScopedTempFile(ScopedTempFile const&) = delete;
    ScopedTempFile& operator=(ScopedTempFile const&) = delete;
    std::string path;
};

// Test-only disk wrapper that delegates reads to an inner disk but fails all writes with
// -ENOSPC. Used to inject write failures without relying on fd manipulation.
class FailWritesDisk : public ublkpp::ublk_disk {
    std::shared_ptr< ublkpp::ublk_disk > _inner;

public:
    explicit FailWritesDisk(std::shared_ptr< ublkpp::ublk_disk > inner) : _inner(std::move(inner)) {}

    std::string id() const noexcept override { return _inner->id() + "[fail-writes]"; }

    ublkpp::disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                       uint32_t nr_vecs, uint64_t addr) override {
        if (ublksrv_get_op(data->iod) == UBLK_IO_OP_WRITE) co_return -ENOSPC;
        co_return co_await _inner->async_iov(q, data, iovecs, nr_vecs, addr);
    }

    ublkpp::io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept override {
        if (op == UBLK_IO_OP_WRITE) return std::unexpected(std::make_error_condition(std::errc::no_space_on_device));
        return _inner->sync_iov(op, iovecs, nr_vecs, addr);
    }

    prepare_result prepare(ublkpp::ublk_rings const* r, int start) override { return _inner->prepare(r, start); }
};

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

} // namespace

// Verify the async resync path copies data correctly when both mirror disks are backed by
// real files. Phase 3 uses async_iov() on the per-RAID1-pair resync ring (k_resync_slots
// concurrent slots); data written to the clean mirror must appear byte-for-byte on the dirty
// mirror after resync completes.
TEST(AsyncResyncIoUring, BasicCopy) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0) << "Failed to create clean temp file";
    ASSERT_GE(dirty_raw_fd, 0) << "Failed to create dirty temp file";
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};

    // Pre-fill the data region of the clean mirror with a recognisable byte pattern.
    constexpr uint8_t k_pattern = 0xCA;
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
    bitmap->dirty_region(0, k_chunk_size);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    TickDriver task_driver{task};
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
}

// Verify that stop() returns promptly while the resync task is actively copying via io_uring.
// stop() only CASes ACTIVE→STOPPING (non-blocking); TickDriver drives the drain asynchronously.
// Bound conservatively at 1000 ms to tolerate CI scheduling jitter.
TEST(AsyncResyncIoUring, StopResponsive) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};
    close(clean_raw_fd);
    close(dirty_raw_fd);

    auto disk_clean = ublkpp::make_fs_disk(clean_path);
    auto disk_dirty = ublkpp::make_fs_disk(dirty_path);

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, disk_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, disk_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    // Dirty only complete chunks to stay within file capacity (Bitmap rounds up to chunk boundaries).
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    auto const k_dirty_chunks = (k_test_file_size - k_data_offset) / k_chunk_size;
    bitmap->dirty_region(0, k_dirty_chunks * k_chunk_size);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    TickDriver task_driver{task};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Wait for at least one complete sweep — ensures tick() has run at least once before
    // stop() is called.
    while (task.yield_count() < 1)
        std::this_thread::sleep_for(1ms);

    auto const before = std::chrono::steady_clock::now();
    task.stop();
    auto const elapsed = std::chrono::steady_clock::now() - before;

    EXPECT_LT(elapsed, 1000ms) << "stop() must return promptly (non-blocking CAS ACTIVE→STOPPING)";
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
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};

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
    TickDriver task_driver{task};

    // RAII guard: releases the write hold if the test exits early via ASSERT, preventing
    // TickDriver from blocking in join() with chunk 0 still held.
    ResyncWriteGuard write_guard{task, 0, k_chunk_size};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Wait until chunk 1's specific region is clean. dirty_pages() counts whole bitmap pages
    // (each covers 1 GiB), so it stays at 1 for our sub-GiB test file until ALL chunks are
    // clean — the wrong signal. is_dirty() checks the exact LBA range.
    auto const deadline = std::chrono::steady_clock::now() + 5000ms;
    while (bitmap->is_dirty(k_chunk_size, k_chunk_size) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    ASSERT_FALSE(bitmap->is_dirty(k_chunk_size, k_chunk_size)) << "chunk 1 must be clean within timeout";
    ASSERT_TRUE(bitmap->is_dirty(0, k_chunk_size)) << "chunk 0 must still be dirty while write is held";

    // Verify chunk 1 was copied correctly.
    {
        std::vector< uint8_t > actual1(k_chunk_size, 0);
        int verify_fd = open(dirty_path.c_str(), O_RDONLY);
        ASSERT_GE(verify_fd, 0);
        // Drop page cache so a subsequent buffered pread sees O_DIRECT-written data on
        // non-overlayfs systems where FSDisk enables O_DIRECT. No-op hint on overlayfs.
        posix_fadvise(verify_fd, k_data_offset + k_chunk_size, k_chunk_size, POSIX_FADV_DONTNEED);
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
        posix_fadvise(verify_fd, k_data_offset, k_chunk_size, POSIX_FADV_DONTNEED);
        ASSERT_EQ(static_cast< ssize_t >(k_chunk_size), pread(verify_fd, actual0.data(), k_chunk_size, k_data_offset));
        close(verify_fd);
        EXPECT_EQ(chunk0_data, actual0) << "chunk 0 must be correctly copied after the write is released";
    }

    task.stop();
}

// Verify that all dirty chunks are copied correctly when several are dirty simultaneously.
// BasicCopy tests one chunk; this test uses four distinct byte patterns to catch address
// arithmetic errors (wrong _offset or chunk stride) and buffer-reuse bugs across multiple
// concurrent async_iov READ→WRITE slot pairs.
TEST(AsyncResyncIoUring, MultipleChunkCopy) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};

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
    TickDriver task_driver{task};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap)) << "All dirty chunks must be resynced";
    // Assert that at least 2 slots were in-flight concurrently, confirming async pipelining.
    EXPECT_GE(task.peak_in_flight(), 2u) << "Expected >= 2 concurrent in-flight slots (async pipelining)";
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
}

// Verify that the complete() callback fires exactly once after the io_uring resync path
// copies all dirty chunks. All other tests pass [] {} as the callback; this test is the
// only one that asserts the callback is actually invoked via the real-file io_uring path.
TEST(AsyncResyncIoUring, CompleteCallbackFires) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};
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
    TickDriver task_driver{task};
    task.launch(test_uuid, mirror_clean, mirror_dirty,
                [&callback_count] { callback_count.fetch_add(1, std::memory_order_release); });

    // Wait for the callback — it fires in __finish_session() after the bitmap is fully clean.
    auto const deadline = std::chrono::steady_clock::now() + 5000ms;
    while (callback_count.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);

    EXPECT_EQ(1, callback_count.load(std::memory_order_acquire))
        << "complete() must fire exactly once after a successful io_uring resync";
    EXPECT_EQ(0U, bitmap->dirty_pages()) << "Bitmap must be clean when complete() fires";

    task.stop();
}

// Verify the io_uring ring lifecycle across a stop+relaunch cycle. The ring is per-RAID1-pair
// (persistent across launches); this test confirms that after stop+relaunch the ring is
// reused cleanly, resync completes, and data on the dirty mirror is correct.
TEST(AsyncResyncIoUring, StopAndRelaunch) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};

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
    TickDriver task_driver{task};

    // First launch: dirty one chunk and stop after the first sweep.
    bitmap->dirty_region(0, k_chunk_size);
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});
    while (task.yield_count() < 1)
        std::this_thread::sleep_for(1ms);
    task.stop();

    // stop() is non-blocking (CAS ACTIVE→STOPPING); wait for TickDriver to finish the
    // drain before re-launching, otherwise launch() sees STOPPING and becomes a no-op.
    while (!task.is_idle())
        std::this_thread::sleep_for(1ms);

    // Second launch: re-dirty (stop may have partially cleaned) and run to completion.
    // The per-RAID1-pair ring is reused; the second resync must produce correct data.
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
}

// Verify that when all dirty chunks are Phase-1-conflicting, the resync loop skips them
// without marking them clean. The bitmap stays dirty until the conflict is released.
//
// Setup: one dirty chunk at LBA 0, held by an in-flight write (Phase-1 conflict). Every
// tick() iteration Phase 1 skips the chunk → no tasks submitted → __yield() increments
// yield_count. After releasing the write guard, the chunk is copied and the bitmap clears.
TEST(AsyncResyncIoUring, SleepWhenAllChunksConflict) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};
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

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    TickDriver task_driver{task};
    ResyncWriteGuard write_guard{task, 0, k_chunk_size}; // hold chunk 0: every tick Phase-1-skips it

    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Wait for yield_count >= 3 confirming tick() ran multiple sweeps while the chunk was held.
    // Allow 5 s for CI jitter.
    auto const deadline = std::chrono::steady_clock::now() + 5000ms;
    while (task.yield_count() < 3 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    ASSERT_GE(task.yield_count(), 3U) << "Resync must yield at least 3 times while all chunks are Phase-1 conflicting";

    // Release the conflict and run to completion.
    write_guard.release();
    EXPECT_TRUE(wait_for_bitmap_clean(bitmap)) << "Bitmap must become clean after conflict released";
    task.stop();

    EXPECT_EQ(0U, bitmap->dirty_pages()) << "Bitmap must be clean after the conflict is released";
}

// Verify that a READ failure on the clean mirror marks it unavailable and leaves the bitmap
// dirty. Truncating the clean mirror to k_data_offset bytes causes preadv at that offset to
// return 0 (short read); the resync loop must detect this, mark clean_mirror->unavail, and NOT
// mark the region clean.
TEST(AsyncResyncIoUring, ReadFailureMarksMirrorUnavail) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};
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

    // Truncate clean mirror to exactly k_data_offset bytes — preadv at k_data_offset returns 0
    // (past EOF), triggering the short-read failure path in __resync_slot_coro().
    {
        int trunc_fd = open(clean_path.c_str(), O_RDWR);
        ASSERT_GE(trunc_fd, 0) << "Cannot open clean mirror for truncation";
        ASSERT_EQ(0, ftruncate(trunc_fd, static_cast< off_t >(k_data_offset)));
        close(trunc_fd);
    }

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    TickDriver task_driver{task};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Wait for clean_mirror->unavail to be set (the short-read failure path sets it).
    auto const deadline = std::chrono::steady_clock::now() + 5000ms;
    while (!mirror_clean->unavail.test(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);

    EXPECT_TRUE(mirror_clean->unavail.test(std::memory_order_acquire))
        << "Short read must mark clean_mirror unavailable";
    EXPECT_GT(bitmap->dirty_pages(), 0U) << "Bitmap must stay dirty after a read failure";

    task.stop();
}

// Verify that a WRITE failure on the dirty mirror marks it unavailable and leaves the bitmap
// dirty. FailWritesDisk wraps the dirty mirror so all writes return -ENOSPC;
// the resync loop must detect this, mark dirty_mirror->unavail, and NOT clean the region.
TEST(AsyncResyncIoUring, WriteFailureMarksDirtyMirrorUnavail) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};

    // Write a recognisable pattern to the data region of the clean mirror.
    std::vector< uint8_t > src(k_chunk_size, 0xAB);
    ASSERT_EQ(static_cast< ssize_t >(k_chunk_size), pwrite(clean_raw_fd, src.data(), k_chunk_size, k_data_offset));
    close(clean_raw_fd);
    close(dirty_raw_fd);

    auto disk_clean = ublkpp::make_fs_disk(clean_path);
    // Wrap the dirty-mirror disk in FailWritesDisk: writes return -ENOSPC, reads delegate to
    // the underlying FSDisk. The resync READ from clean_mirror succeeds; WRITE to dirty fails.
    auto disk_dirty = std::make_shared< FailWritesDisk >(ublkpp::make_fs_disk(dirty_path));

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, disk_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, disk_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    bitmap->dirty_region(0, k_chunk_size);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    TickDriver task_driver{task};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Wait for dirty_mirror->unavail to be set (the ENOSPC write-failure path sets it).
    auto const deadline = std::chrono::steady_clock::now() + 5000ms;
    while (!mirror_dirty->unavail.test(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);

    EXPECT_TRUE(mirror_dirty->unavail.test(std::memory_order_acquire))
        << "ENOSPC write must mark dirty_mirror unavailable";
    EXPECT_GT(bitmap->dirty_pages(), 0U) << "Bitmap must stay dirty after a write failure";

    task.stop();
}

// Verify that stop() does not deadlock or hang when called immediately after launch(), before
// tick() has completed any sweep. This exercises the STOPPING drain path
// (has_in_flight() loop + 30 s watchdog) that StopResponsive does not reach because it waits
// for yield_count >= 1 (all SQEs from that sweep already completed). Bound at 2 s to tolerate
// worst-case io_uring drain latency under CI scheduling pressure.
TEST(AsyncResyncIoUring, StopMidFlight) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};
    close(clean_raw_fd);
    close(dirty_raw_fd);

    auto disk_clean = ublkpp::make_fs_disk(clean_path);
    auto disk_dirty = ublkpp::make_fs_disk(dirty_path);

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, disk_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, disk_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    // Dirty all available chunks to maximise the chance that SQEs are in flight when stop() fires.
    auto const k_dirty_chunks = (k_test_file_size - k_data_offset) / k_chunk_size;
    bitmap->dirty_region(0, k_dirty_chunks * k_chunk_size);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    TickDriver task_driver{task};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Spin until the TickDriver has started at least one in-flight slot so that stop()
    // genuinely exercises the mid-flight STOPPING drain path, not just an empty-slot drain.
    auto const in_flight_deadline = std::chrono::steady_clock::now() + 5000ms;
    while (task.peak_in_flight() == 0 && std::chrono::steady_clock::now() < in_flight_deadline)
        std::this_thread::sleep_for(1ms);
    ASSERT_GT(task.peak_in_flight(), 0u) << "Expected at least one in-flight slot before stop()";

    // stop() CASes ACTIVE→STOPPING and returns immediately; TickDriver drives the drain.
    auto const before = std::chrono::steady_clock::now();
    task.stop();
    auto const elapsed = std::chrono::steady_clock::now() - before;

    EXPECT_LT(elapsed, 2000ms) << "stop() must return promptly even when called mid-sweep";
}

// Verify that a write conflict registered AFTER chunk 1 is copied still blocks chunk 0 via
// Phase 1 (not Phase 2): at the time resync attempts chunk 0, the new write guard is already
// registered so overlaps() returns true in the Phase-1 check. Genuine Phase-2 testing (write
// arrives and fully completes during the async READ window) is covered by
// Phase2ConflictDetectedAfterCopy and Phase2CompletedWriteDetected in write_resync_no_pause.cpp;
// reproducing that race reliably on a real io_uring path would require blocking SQE callbacks,
// which are not available in the fs-backed FsDisk path.
TEST(AsyncResyncIoUring, Phase1StillBlocksAfterConflictArrives) {
    auto [clean_path, clean_raw_fd] = make_resync_test_file(false);
    auto [dirty_path, dirty_raw_fd] = make_resync_test_file(true);
    ASSERT_GE(clean_raw_fd, 0);
    ASSERT_GE(dirty_raw_fd, 0);
    ScopedTempFile clean_guard{clean_path};
    ScopedTempFile dirty_guard{dirty_path};

    constexpr uint8_t k_pat0 = 0xC0;
    constexpr uint8_t k_pat1 = 0xC1;
    std::vector< uint8_t > chunk0(k_chunk_size, k_pat0);
    std::vector< uint8_t > chunk1(k_chunk_size, k_pat1);
    ASSERT_EQ(static_cast< ssize_t >(k_chunk_size), pwrite(clean_raw_fd, chunk0.data(), k_chunk_size, k_data_offset));
    ASSERT_EQ(static_cast< ssize_t >(k_chunk_size),
              pwrite(clean_raw_fd, chunk1.data(), k_chunk_size, k_data_offset + k_chunk_size));
    close(clean_raw_fd);
    close(dirty_raw_fd);

    auto disk_clean = ublkpp::make_fs_disk(clean_path);
    auto disk_dirty = ublkpp::make_fs_disk(dirty_path);

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_clean = std::make_shared< MirrorDevice >(uuid, disk_clean);
    auto mirror_dirty = std::make_shared< MirrorDevice >(uuid, disk_dirty);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(k_test_file_size, k_chunk_size, k_page_size, superbitmap_buf.get());
    bitmap->dirty_region(0, 2 * k_chunk_size);

    Raid1ResyncTask task{bitmap, k_data_offset, k_chunk_size, k_chunk_size};
    TickDriver task_driver{task};

    // Phase-1 guard on chunk 0: resync skips it and copies chunk 1 first.
    ResyncWriteGuard phase1_guard{task, 0, k_chunk_size};
    task.launch(test_uuid, mirror_clean, mirror_dirty, [] {});

    // Wait for chunk 1 to be copied (bitmap no longer dirty for chunk 1).
    auto const deadline = std::chrono::steady_clock::now() + 5000ms;
    while (bitmap->is_dirty(k_chunk_size, k_chunk_size) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    ASSERT_FALSE(bitmap->is_dirty(k_chunk_size, k_chunk_size)) << "chunk 1 must be copied while chunk 0 is held";

    // Register a new write conflict on chunk 0, then release the Phase-1 guard. The resync
    // thread will see Phase 1 clear but Phase 2 conflict (the new guard), keep chunk 0 dirty,
    // and retry on the next sweep.
    ResyncWriteGuard phase2_guard{task, 0, k_chunk_size};
    phase1_guard.release();

    // Confirm chunk 0 stays dirty while the Phase-2 guard is held.
    std::this_thread::sleep_for(5ms); // give resync a few ticks to attempt chunk 0
    EXPECT_TRUE(bitmap->is_dirty(0, k_chunk_size)) << "chunk 0 must stay dirty while Phase-2 conflict is held";

    // Release the Phase-2 guard — resync can now copy chunk 0 and the bitmap clears.
    phase2_guard.release();
    EXPECT_TRUE(wait_for_bitmap_clean(bitmap)) << "Bitmap must become clean after Phase-2 conflict released";
    task.stop();

    // Verify chunk 0 was correctly copied.
    std::vector< uint8_t > actual0(k_chunk_size, 0);
    int verify_fd = open(dirty_path.c_str(), O_RDONLY);
    ASSERT_GE(verify_fd, 0);
    posix_fadvise(verify_fd, k_data_offset, k_chunk_size, POSIX_FADV_DONTNEED);
    ASSERT_EQ(static_cast< ssize_t >(k_chunk_size), pread(verify_fd, actual0.data(), k_chunk_size, k_data_offset));
    close(verify_fd);
    EXPECT_EQ(chunk0, actual0) << "chunk 0 must be correctly copied after Phase-2 conflict released";
}
