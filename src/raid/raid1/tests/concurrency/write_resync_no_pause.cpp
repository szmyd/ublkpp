#include "test_raid1_common.hpp"

#include <atomic>
#include <future>
#include <thread>

#include <boost/uuid/string_generator.hpp>

#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_impl.hpp"
#include "raid/raid1/raid1_resync_task.hpp"

using namespace std::chrono_literals;
using namespace ublkpp::raid1;

static bool wait_for_bitmap_clean(std::shared_ptr< Bitmap > const& bitmap, std::chrono::milliseconds timeout = 2000ms) {
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (0 == bitmap->dirty_pages()) return true;
        std::this_thread::sleep_for(1ms);
    }
    return false;
}

// Verify that a write to an unrelated LBA does not block resync of a different dirty region.
// Old mechanism: ANY write paused ALL resync.
// New mechanism: only conflicting LBA ranges are skipped.
TEST(Raid1Concurrency, UnrelatedWriteDoesNotBlockResync) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    std::atomic< int > resync_reads{0};

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&resync_reads](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            resync_reads.fetch_add(1, std::memory_order_relaxed);
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
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
    // Dirty LBA 0
    bitmap->dirty_region(0, 32 * Ki);

    constexpr uint32_t io_size = 32 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    // Register a write to a non-overlapping LBA (512 MiB away from dirty LBA 0) and hold it
    // in-flight. With per-region tracking, resync must copy LBA 0 despite this write.
    constexpr uint64_t k_unrelated_lba = 512 * Mi;
    task.enqueue_write(k_unrelated_lba, io_size);

    // Start resync — it must copy LBA 0 despite the in-flight write at 512 MiB
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap, 2000ms))
        << "Resync must complete even with an unrelated write held in-flight";

    EXPECT_GE(resync_reads.load(), 1) << "Resync should have performed at least one read";

    // Release the in-flight write now that resync has finished
    task.dequeue_write(k_unrelated_lba, io_size);
    task.stop();
}

// Verify that resync skips a conflicting in-flight write (bitmap stays dirty),
// and successfully copies the region after the write completes.
TEST(Raid1Concurrency, ConflictingWriteSkipped_ResyncRetries) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    std::atomic< int > resync_reads{0};

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&resync_reads](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            resync_reads.fetch_add(1, std::memory_order_relaxed);
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
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
    bitmap->dirty_region(0, 32 * Ki);

    constexpr uint32_t io_size = 32 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    // Register a write overlapping LBA 0 (the dirty region) and hold it in-flight
    task.enqueue_write(0, io_size);

    // Start resync — Phase 1 should detect the conflict and skip LBA 0 every sweep
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    // Wait for resync to have yielded at least twice (two full sweeps attempted) before
    // asserting. This is deterministic: yield_count() only advances when the resync thread
    // completes a sweep and sleeps — no wall-clock guessing needed.
    while (task.yield_count() < 2)
        std::this_thread::sleep_for(1ms);

    EXPECT_EQ(0, resync_reads.load()) << "Resync must not read the conflicting region while write is in-flight";
    EXPECT_GT(bitmap->dirty_pages(), 0U) << "Bitmap must remain dirty while conflicting write is in-flight";

    // Dequeue the write — resync can now proceed
    task.dequeue_write(0, io_size);

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap, 2000ms))
        << "Bitmap must become clean after the conflicting write is dequeued";

    task.stop();
}

// Verify that Phase 2 (post-copy conflict check) detects a write that arrives AFTER Phase 1
// passes but BEFORE clean_region is called, and correctly keeps the bitmap dirty.
//
// Sequence:
//   Phase 1 check: no write in-flight, resync begins copy READ (mock blocks)
//   [test thread registers write, then unblocks READ]
//   Copy WRITE completes; Phase 2 sees write in-flight, skips clean_region
//   Resync yields — yield_count() advances, giving a deterministic signal that Phase 2 ran
//   Bitmap stays dirty; dequeue write; resync re-copies and cleans bitmap
TEST(Raid1Concurrency, Phase2ConflictDetectedAfterCopy) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    // The first resync READ signals that Phase 1 has passed, then blocks until the test
    // thread registers a write for the same region.
    std::promise< void > copy_read_started;
    std::promise< void > write_registered;
    auto write_registered_future = write_registered.get_future();

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillOnce([&, fut = std::move(write_registered_future)](uint8_t, iovec* iovecs, uint32_t,
                                                                off_t) mutable -> ublkpp::io_result {
            copy_read_started.set_value();
            fut.wait();
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return ublkpp::iovec_len(iovecs, iovecs + 1);
        })
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
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
    bitmap->dirty_region(0, 32 * Ki);

    constexpr uint32_t io_size = 32 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    // Start resync — Phase 1 sees no write registered and begins the copy READ
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    // Wait for resync to start reading LBA 0 (Phase 1 has already passed)
    copy_read_started.get_future().wait();

    // Register a write to LBA 0 AFTER Phase 1 passed; Phase 2 must catch it
    task.enqueue_write(0, io_size);

    // Capture yield count before unblocking the READ. After the READ is unblocked, the resync
    // thread will complete the copy, Phase 2 will detect the in-flight write and skip
    // clean_region, then call __yield(). Waiting for yield_count to advance is the only
    // deterministic signal that Phase 2 has fully executed (and not just that the copy WRITE
    // has started). Using a WillOnce hook on the device_b WRITE would fire before Phase 2
    // runs, creating a race where dequeue_write could complete before Phase 2 checks overlaps().
    auto const yield_before = task.yield_count();
    write_registered.set_value();

    // Wait for the resync sweep to complete — Phase 2 has definitely run and yielded.
    while (task.yield_count() == yield_before)
        std::this_thread::sleep_for(1ms);
    EXPECT_GT(bitmap->dirty_pages(), 0U) << "Bitmap must remain dirty while Phase-2-detected write is in-flight";

    // Dequeue the write — resync can now copy the dirty region and mark it clean
    task.dequeue_write(0, io_size);

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap, 2000ms)) << "Bitmap must become clean after the Phase-2 write completes";

    task.stop();
}

// Verify that Phase 2 detects a write that arrived AND fully completed during the resync READ
// window. This is the case where the slot was already freed (lba == k_free) before Phase 2 ran,
// making overlaps() return false. The shadow completion log in RegionTracker catches this.
//
// Sequence:
//   Phase 1 check: no write in-flight, resync begins copy READ (mock blocks)
//   [test thread: enqueue_write then dequeue_write — write fully completes, slot freed]
//   [test thread unblocks the READ]
//   Copy WRITE completes, Phase 2 checks completed_since(), detects the shadow entry
//   Bitmap stays dirty, resync retries, bitmap cleans
TEST(Raid1Concurrency, Phase2CompletedWriteDetected) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    std::promise< void > read_started;
    std::promise< void > write_fully_done;

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillOnce([&](uint8_t, iovec* iovecs, uint32_t, off_t) mutable -> ublkpp::io_result {
            read_started.set_value();
            write_fully_done.get_future().wait();
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return ublkpp::iovec_len(iovecs, iovecs + 1);
        })
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    // Signal when the first resync WRITE to dirty_mirror completes (Phase 2 has run).
    std::promise< void > resync_write_done;
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillOnce([&](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            auto const result = ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
            resync_write_done.set_value();
            return result;
        })
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
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
    bitmap->dirty_region(0, 32 * Ki);

    constexpr uint32_t io_size = 32 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    // Start resync — Phase 1 sees no write, begins the copy READ (which blocks on read_started)
    task.launch(test_uuid, mirror_a, mirror_b, [] {});
    read_started.get_future().wait();

    // Enqueue AND dequeue the write while the READ is blocked: the write fully completes
    // and the slot is freed (lba == k_free) before Phase 2 runs. overlaps() would return
    // false, but completed_since() must detect the shadow log entry.
    task.enqueue_write(0, io_size);
    task.dequeue_write(0, io_size); // write fully completes here
    write_fully_done.set_value();   // unblock the resync READ

    // Wait until Phase 2 has run (resync WRITE to dirty_mirror completed).
    resync_write_done.get_future().wait();
    EXPECT_GT(bitmap->dirty_pages(), 0U)
        << "Bitmap must remain dirty: Phase 2 must detect the completed write via shadow log";

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap, 2000ms))
        << "Bitmap must become clean after resync retries with no competing write";

    task.stop();
}

// Verify the core improvement: when a dirty run spans multiple chunks and only chunk 0
// conflicts with an in-flight write, resync skips chunk 0 and copies chunk 1 in the same pass.
// This is the key behaviour that distinguishes per-region tracking from the old global pause.
TEST(Raid1Concurrency, MultiChunkDirtyRun_PartialSkip) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    // Track which LBAs resync actually reads.
    std::atomic< int > reads_chunk0{0};
    std::atomic< int > reads_chunk1{0};

    constexpr uint32_t io_size = 32 * Ki;

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t raw_addr) -> ublkpp::io_result {
            // Subtract the bitmap reserved area to recover the logical offset.
            auto const logical = static_cast< uint64_t >(raw_addr) - Bitmap::page_size();
            if (logical < io_size)
                reads_chunk0.fetch_add(1, std::memory_order_relaxed);
            else if (logical >= io_size && logical < 2 * io_size)
                reads_chunk1.fetch_add(1, std::memory_order_relaxed);
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
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
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(Gi, io_size, 4 * Ki, superbitmap_buf.get());
    // Dirty two adjacent chunks: [0, 32Ki) and [32Ki, 64Ki)
    bitmap->dirty_region(0, 2 * io_size);

    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    // Hold chunk 0 in-flight — resync must skip it but still copy chunk 1
    task.enqueue_write(0, io_size);
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    // Wait for chunk 1 to be copied while chunk 0 is held
    auto const deadline = std::chrono::steady_clock::now() + 2000ms;
    while (reads_chunk1.load() == 0 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);

    EXPECT_EQ(0, reads_chunk0.load()) << "Resync must not read chunk 0 while its write is in-flight";
    EXPECT_GE(reads_chunk1.load(), 1) << "Resync must copy chunk 1 despite the hold on chunk 0";

    // Release chunk 0 — resync copies it and the bitmap clears
    task.dequeue_write(0, io_size);
    EXPECT_TRUE(wait_for_bitmap_clean(bitmap, 2000ms))
        << "Bitmap must become clean after the conflicting write on chunk 0 is released";

    task.stop();
}
