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

    // Give resync several sweep cycles to attempt copying — it must skip each time
    std::this_thread::sleep_for(300ms);

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
//   Phase 1 check: no write in-flight → resync proceeds, begins copy READ
//   [test thread registers a write while READ is in progress]
//   Copy WRITE completes → Phase 2 detects the in-flight write → skip clean_region
//   → bitmap stays dirty until the write completes → resync re-copies → clean
TEST(Raid1Concurrency, Phase2ConflictDetectedAfterCopy) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskA"});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi, .id = "DiskB", .is_slot_b = true});

    // The first resync READ signals that Phase 1 has passed, then blocks until the test
    // thread registers a write for the same region
    std::promise< void > copy_read_started;
    std::promise< void > write_registered;

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillOnce([&](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            copy_read_started.set_value();
            write_registered.get_future().wait();
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
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

    // Unblock the resync read — Phase 2 will detect the in-flight write and skip clean_region
    write_registered.set_value();

    // Bitmap must remain dirty while the write is still in-flight
    std::this_thread::sleep_for(300ms);
    EXPECT_GT(bitmap->dirty_pages(), 0U) << "Bitmap must remain dirty while Phase-2-detected write is in-flight";

    // Dequeue the write — resync can now copy the dirty region and mark it clean
    task.dequeue_write(0, io_size);

    EXPECT_TRUE(wait_for_bitmap_clean(bitmap, 2000ms)) << "Bitmap must become clean after the Phase-2 write completes";

    task.stop();
}
