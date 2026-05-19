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

// Helper: returns the iov length as a successful io_result.
static auto submit_iov_ok() {
    return [](ublksrv_queue const*, ublk_io_data const*, iovec* iovecs, uint32_t, uint64_t) -> ublkpp::io_result {
        if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
        return static_cast< ssize_t >(iovecs->iov_len);
    };
}

// Verify that a write to an unrelated LBA does not block resync of a different dirty region.
// Old mechanism: ANY write paused ALL resync.
// New mechanism: only conflicting LBA ranges are skipped.
TEST(Raid1Concurrency, UnrelatedWriteDoesNotBlockResync) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    // Async path: data READs go through submit_iov on the clean mirror (device_a).
    std::atomic< int > resync_reads{0};
    EXPECT_CALL(*device_a, submit_iov(_, _, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&resync_reads](ublksrv_queue const*, ublk_io_data const*, iovec* iovecs, uint32_t,
                                        uint64_t) -> ublkpp::io_result {
            resync_reads.fetch_add(1, std::memory_order_relaxed);
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return static_cast< ssize_t >(iovecs->iov_len);
        });
    // Bitmap-page writes from clean_region() still go through sync_iov WRITE on the clean mirror.
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    // device_b (dirty mirror): superblock read on construction via sync_iov, data WRITEs via submit_iov.
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, submit_iov(_, _, _, _, _)).Times(::testing::AnyNumber()).WillRepeatedly(submit_iov_ok());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);
    resync_reads.store(0, std::memory_order_relaxed);

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

    EXPECT_GE(resync_reads.load(), 1) << "Resync must have performed at least one async read";

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
    EXPECT_CALL(*device_a, submit_iov(_, _, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&resync_reads](ublksrv_queue const*, ublk_io_data const*, iovec* iovecs, uint32_t,
                                        uint64_t) -> ublkpp::io_result {
            resync_reads.fetch_add(1, std::memory_order_relaxed);
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return static_cast< ssize_t >(iovecs->iov_len);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, submit_iov(_, _, _, _, _)).Times(::testing::AnyNumber()).WillRepeatedly(submit_iov_ok());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);
    resync_reads.store(0, std::memory_order_relaxed);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());
    bitmap->dirty_region(0, 32 * Ki);

    constexpr uint32_t io_size = 32 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    // Register a write overlapping LBA 0 (the dirty region) and hold it in-flight
    task.enqueue_write(0, io_size);

    // Start resync — Phase 1 should detect the conflict and skip LBA 0 every sweep
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    // Wait for resync to have yielded at least twice (two full sweeps attempted).
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
// With the async path, Phase 2 runs in the drain loop after async_iov().start() returns.
// The injection window: block submit_iov (READ) → resync thread is stuck inside start() →
// test thread registers write → unblock → Phase 2 sees overlaps() = true → skips WRITE.
TEST(Raid1Concurrency, Phase2ConflictDetectedAfterCopy) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    std::promise< void > copy_read_started;
    std::promise< void > write_registered;
    auto write_registered_future = write_registered.get_future();

    // Block the first data READ (via submit_iov) so we can register a write in the window
    // between Phase 1 (already passed) and Phase 2 (runs after submit_iov returns).
    EXPECT_CALL(*device_a, submit_iov(_, _, _, _, _))
        .Times(::testing::AnyNumber())
        .WillOnce([&, fut = std::move(write_registered_future)](ublksrv_queue const*, ublk_io_data const*,
                                                                iovec* iovecs, uint32_t,
                                                                uint64_t) mutable -> ublkpp::io_result {
            copy_read_started.set_value();
            fut.wait(); // hold until test thread registers the write
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return static_cast< ssize_t >(iovecs->iov_len);
        })
        .WillRepeatedly(
            [](ublksrv_queue const*, ublk_io_data const*, iovec* iovecs, uint32_t, uint64_t) -> ublkpp::io_result {
                if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
                return static_cast< ssize_t >(iovecs->iov_len);
            });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, submit_iov(_, _, _, _, _)).Times(::testing::AnyNumber()).WillRepeatedly(submit_iov_ok());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());
    bitmap->dirty_region(0, 32 * Ki);

    constexpr uint32_t io_size = 32 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    // Start resync — Phase 1 sees no write registered, starts async READ which blocks
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    // Wait until we are inside submit_iov (Phase 1 has passed, READ is executing)
    copy_read_started.get_future().wait();

    // Register the write NOW — Phase 1 already passed, Phase 2 will detect via overlaps().
    task.enqueue_write(0, io_size);

    auto const yield_before = task.yield_count();
    write_registered.set_value(); // unblock READ

    // Wait for the sweep to complete — Phase 2 ran, skipped WRITE, yielded
    while (task.yield_count() == yield_before)
        std::this_thread::sleep_for(1ms);
    EXPECT_GT(bitmap->dirty_pages(), 0U) << "Bitmap must remain dirty while Phase-2-detected write is in-flight";

    // Dequeue the write — resync can now copy the dirty region and mark it clean
    task.dequeue_write(0, io_size);

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap, 2000ms)) << "Bitmap must become clean after the Phase-2 write completes";

    task.stop();
}

// Verify that Phase 2 detects a write that arrived AND fully completed during the resync READ
// window. The shadow completion log in RegionTracker catches completed_since() cases.
TEST(Raid1Concurrency, Phase2CompletedWriteDetected) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    std::promise< void > read_started;
    std::promise< void > write_fully_done;
    auto write_fully_done_future = write_fully_done.get_future();

    EXPECT_CALL(*device_a, submit_iov(_, _, _, _, _))
        .Times(::testing::AnyNumber())
        .WillOnce([&, fut = std::move(write_fully_done_future)](ublksrv_queue const*, ublk_io_data const*,
                                                                iovec* iovecs, uint32_t,
                                                                uint64_t) mutable -> ublkpp::io_result {
            read_started.set_value();
            fut.wait(); // wait until test thread completes the write
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return static_cast< ssize_t >(iovecs->iov_len);
        })
        .WillRepeatedly(
            [](ublksrv_queue const*, ublk_io_data const*, iovec* iovecs, uint32_t, uint64_t) -> ublkpp::io_result {
                if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
                return static_cast< ssize_t >(iovecs->iov_len);
            });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, submit_iov(_, _, _, _, _)).Times(::testing::AnyNumber()).WillRepeatedly(submit_iov_ok());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());
    bitmap->dirty_region(0, 32 * Ki);

    constexpr uint32_t io_size = 32 * Ki;
    Raid1ResyncTask task{bitmap, Bitmap::page_size(), io_size, io_size};

    task.launch(test_uuid, mirror_a, mirror_b, [] {});
    read_started.get_future().wait();

    // Enqueue AND dequeue the write while the READ is blocked: the write fully completes
    // and the slot is freed before Phase 2 runs. completed_since() must detect the shadow entry.
    task.enqueue_write(0, io_size);
    task.dequeue_write(0, io_size); // write fully completes, slot freed, shadow entry written

    EXPECT_GT(bitmap->dirty_pages(), 0U) << "Bitmap must remain dirty while READ is blocked (Phase 2 has not run yet)";

    write_fully_done.set_value(); // unblock READ; Phase 2 runs and detects completed_since()

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap, 2000ms))
        << "Bitmap must become clean after resync retries with no competing write";

    task.stop();
}

// Verify that the skip_from hint carries across outer-loop iterations.
TEST(Raid1Concurrency, SkipFromHintAllowsCopyingBeyondConflict) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    constexpr uint32_t io_size = 32 * Ki;
    auto const k_offset = Bitmap::page_size();

    std::atomic< int > reads_region_a{0};
    std::atomic< int > reads_region_b{0};

    EXPECT_CALL(*device_a, submit_iov(_, _, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&](ublksrv_queue const*, ublk_io_data const*, iovec* iovecs, uint32_t,
                            uint64_t addr) -> ublkpp::io_result {
            auto const logical = addr - k_offset;
            if (logical < io_size)
                reads_region_a.fetch_add(1, std::memory_order_relaxed);
            else if (logical >= 2 * io_size && logical < 3 * io_size)
                reads_region_b.fetch_add(1, std::memory_order_relaxed);
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xBB, iovecs->iov_len);
            return static_cast< ssize_t >(iovecs->iov_len);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, submit_iov(_, _, _, _, _)).Times(::testing::AnyNumber()).WillRepeatedly(submit_iov_ok());

    auto uuid = boost::uuids::string_generator()(test_uuid);
    auto mirror_a = std::make_shared< MirrorDevice >(uuid, device_a);
    auto mirror_b = std::make_shared< MirrorDevice >(uuid, device_b);

    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = std::make_shared< Bitmap >(Gi, io_size, 4 * Ki, superbitmap_buf.get());
    // Two separate dirty regions with a clean gap between them.
    bitmap->dirty_region(0, io_size);           // Region A: one chunk at LBA 0
    bitmap->dirty_region(2 * io_size, io_size); // Region B: one chunk at LBA 64Ki (gap at 32Ki..64Ki)

    Raid1ResyncTask task{bitmap, k_offset, io_size, io_size};

    // Hold region A in-flight: Phase 1 will skip it every sweep and set skip_from = io_size.
    task.enqueue_write(0, io_size);
    task.launch(test_uuid, mirror_a, mirror_b, [] {});

    // Wait for region B to be read while region A is still held.
    auto const deadline = std::chrono::steady_clock::now() + 5000ms;
    while (reads_region_b.load() == 0 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);

    EXPECT_EQ(0, reads_region_a.load()) << "Region A must not be read while its write is in-flight (Phase 1 conflict)";
    EXPECT_GE(reads_region_b.load(), 1) << "Region B must be copied via skip_from hint despite A being held";

    // Release A: resync can now copy it and the bitmap clears.
    task.dequeue_write(0, io_size);
    EXPECT_TRUE(wait_for_bitmap_clean(bitmap, 5000ms)) << "Bitmap must become clean after releasing region A";

    task.stop();
}

// Verify the core improvement: when a dirty run spans multiple chunks and only chunk 0
// conflicts with an in-flight write, resync skips chunk 0 and copies chunk 1 in the same pass.
TEST(Raid1Concurrency, MultiChunkDirtyRun_PartialSkip) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    std::atomic< int > reads_chunk0{0};
    std::atomic< int > reads_chunk1{0};

    constexpr uint32_t io_size = 32 * Ki;
    auto const k_offset = Bitmap::page_size();

    EXPECT_CALL(*device_a, submit_iov(_, _, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&](ublksrv_queue const*, ublk_io_data const*, iovec* iovecs, uint32_t,
                            uint64_t addr) -> ublkpp::io_result {
            auto const logical = addr - k_offset;
            if (logical < io_size)
                reads_chunk0.fetch_add(1, std::memory_order_relaxed);
            else if (logical >= io_size && logical < 2 * io_size)
                reads_chunk1.fetch_add(1, std::memory_order_relaxed);
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return static_cast< ssize_t >(iovecs->iov_len);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(::testing::_, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly(sync_iov_zero_on_read());
    EXPECT_CALL(*device_b, submit_iov(_, _, _, _, _)).Times(::testing::AnyNumber()).WillRepeatedly(submit_iov_ok());

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
