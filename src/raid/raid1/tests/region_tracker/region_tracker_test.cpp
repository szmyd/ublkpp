#include <atomic>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "lib/common.hpp"
#include "raid/raid1/region_tracker.hpp"

using ublkpp::Ki;
using ublkpp::raid1::RegionTracker;

// Unit tests use chunk_size=1 (byte granularity) so lba/len values need not be
// chunk-aligned. Production always uses the superblock chunk_size (default 32 KiB).
static constexpr uint32_t k_chunk = 1;

TEST(RegionTracker, EmptyNoOverlap) {
    RegionTracker tracker(16, k_chunk);
    EXPECT_FALSE(tracker.overlaps(0, 512 * Ki));
    EXPECT_FALSE(tracker.overlaps(100, 512));
    EXPECT_TRUE(tracker.all_free());
}

TEST(RegionTracker, RegisterAndOverlapExact) {
    RegionTracker tracker(16, k_chunk);
    tracker.track(100, 512);
    EXPECT_TRUE(tracker.overlaps(100, 512));
    EXPECT_FALSE(tracker.all_free());
}

TEST(RegionTracker, OverlapPartialLeft) {
    RegionTracker tracker(16, k_chunk);
    tracker.track(200, 512);                 // [200, 712)
    EXPECT_TRUE(tracker.overlaps(100, 200)); // [100, 300) overlaps at [200, 300)
}

TEST(RegionTracker, OverlapPartialRight) {
    RegionTracker tracker(16, k_chunk);
    tracker.track(100, 512);                 // [100, 612)
    EXPECT_TRUE(tracker.overlaps(500, 200)); // [500, 700) overlaps at [500, 612)
}

TEST(RegionTracker, OverlapContained) {
    RegionTracker tracker(16, k_chunk);
    tracker.track(100, 512);                 // [100, 612)
    EXPECT_TRUE(tracker.overlaps(200, 100)); // [200, 300) fully inside registered range
}

TEST(RegionTracker, AdjacentNoOverlap) {
    RegionTracker tracker(16, k_chunk);
    tracker.track(100, 512);                  // [100, 612)
    EXPECT_FALSE(tracker.overlaps(612, 100)); // [612, 712) — adjacent, not overlapping
    EXPECT_FALSE(tracker.overlaps(0, 100));   // [0, 100) — adjacent on the other side
}

TEST(RegionTracker, UnregisterClearsSlot) {
    RegionTracker tracker(16, k_chunk);
    tracker.track(100, 512);
    EXPECT_TRUE(tracker.overlaps(100, 512));
    tracker.untrack(100, 512);
    EXPECT_FALSE(tracker.overlaps(100, 512));
    EXPECT_TRUE(tracker.all_free());
}

TEST(RegionTracker, DuplicateLba_TwoSlots) {
    // The INVARIANT (block-device ordering) guarantees each write creates exactly one guard,
    // so two concurrent registrations for the same (lba, len) cannot occur in production.
    // This test exercises the slot-scan matching logic under that hypothetical scenario.
    RegionTracker tracker(16, k_chunk);
    tracker.track(100, 512);
    tracker.track(100, 512);
    EXPECT_TRUE(tracker.overlaps(100, 512));

    tracker.untrack(100, 512);               // clears one slot
    EXPECT_TRUE(tracker.overlaps(100, 512)); // second slot still occupied

    tracker.untrack(100, 512); // clears second slot
    EXPECT_FALSE(tracker.overlaps(100, 512));
    EXPECT_TRUE(tracker.all_free());
}

TEST(RegionTracker, MultipleDistinctRanges) {
    RegionTracker tracker(16, k_chunk);
    tracker.track(0, 512);
    tracker.track(1024, 512);
    tracker.track(4096, 512);

    EXPECT_TRUE(tracker.overlaps(0, 512));
    EXPECT_TRUE(tracker.overlaps(1024, 512));
    EXPECT_TRUE(tracker.overlaps(4096, 512));
    EXPECT_FALSE(tracker.overlaps(512, 512));   // gap between first and second
    EXPECT_FALSE(tracker.overlaps(1536, 2560)); // gap between second and third

    tracker.untrack(1024, 512);
    EXPECT_FALSE(tracker.overlaps(1024, 512));
    EXPECT_TRUE(tracker.overlaps(0, 512));
    EXPECT_TRUE(tracker.overlaps(4096, 512));
}

TEST(RegionTracker, FillAndDrain) {
    constexpr uint32_t k_slots = 8;
    RegionTracker tracker(k_slots, k_chunk);

    for (uint32_t i = 0; i < k_slots; ++i)
        tracker.track(static_cast< uint64_t >(i) * 1024, 512);

    EXPECT_FALSE(tracker.all_free());

    // Unregister one, verify a new registration can take its place
    tracker.untrack(0, 512);
    tracker.track(k_slots * 1024, 512);
    EXPECT_TRUE(tracker.overlaps(k_slots * 1024, 512));

    // Drain remaining
    for (uint32_t i = 1; i < k_slots; ++i)
        tracker.untrack(static_cast< uint64_t >(i) * 1024, 512);
    tracker.untrack(k_slots * 1024, 512);
    EXPECT_TRUE(tracker.all_free());
}

// TSAN target: concurrent track/untrack on distinct LBA ranges while overlaps() on a
// reader thread spans all of them, so TSAN sees the reader racing with every slot field.
// Each writer thread owns its own LBA to avoid the duplicate-LBA untrack() race that
// produces phantom slots (lba=X, len=0) and causes slot exhaustion.
TEST(RegionTracker, ConcurrentRegisterUnregister) {
    constexpr uint32_t k_threads = 8;
    constexpr uint32_t k_iters = 200;
    constexpr uint32_t k_len = 512;
    // One slot per thread; sized to 2× so no exhaustion when all hold simultaneously.
    RegionTracker tracker(k_threads * 2, k_chunk);

    std::atomic< bool > stop{false};
    // Reader spans the full LBA range so overlaps() races with every writer's slot.
    std::thread reader([&] {
        constexpr uint64_t k_span = static_cast< uint64_t >(k_threads) * 1024 * 1024 + k_len;
        while (!stop.load(std::memory_order_relaxed))
            (void)tracker.overlaps(0, k_span);
    });

    std::vector< std::thread > writers;
    writers.reserve(k_threads);
    for (uint32_t t = 0; t < k_threads; ++t) {
        writers.emplace_back([&tracker, t] {
            auto const lba = static_cast< uint64_t >(t) * 1024 * 1024;
            for (uint32_t i = 0; i < k_iters; ++i) {
                tracker.track(lba, k_len);
                tracker.untrack(lba, k_len);
            }
        });
    }

    for (auto& th : writers)
        th.join();
    stop.store(true, std::memory_order_relaxed);
    reader.join();

    EXPECT_TRUE(tracker.all_free());
}

// --- completed_since() unit tests ---

// A write tracked then untracked before snapshot_gen is invisible to completed_since.
// Only completions after gen_before are detected.
TEST(RegionTracker, CompletedSince_BeforeSnapshotNotDetected) {
    RegionTracker tracker(16, k_chunk);
    tracker.track(0, 512);
    tracker.untrack(0, 512); // completed before snapshot

    auto const gen = tracker.snapshot_gen();
    EXPECT_FALSE(tracker.completed_since(0, 512, gen)) << "completion before snapshot must not be detected";
}

// Basic: track, snapshot, untrack → completed_since detects the overlapping completion.
TEST(RegionTracker, CompletedSince_OverlappingWriteDetected) {
    RegionTracker tracker(16, k_chunk);
    auto const gen = tracker.snapshot_gen();

    tracker.track(0, 512);
    tracker.untrack(0, 512); // completes after gen

    EXPECT_TRUE(tracker.completed_since(0, 512, gen)) << "overlapping completion after snapshot must be detected";
}

// A write to a non-overlapping range must not trigger completed_since.
TEST(RegionTracker, CompletedSince_NonOverlappingWriteNotDetected) {
    RegionTracker tracker(16, k_chunk);
    auto const gen = tracker.snapshot_gen();

    tracker.track(1024 * Ki, 512); // [1MiB, 1MiB+512)
    tracker.untrack(1024 * Ki, 512);

    // Query [0, 512) — no overlap with [1MiB, 1MiB+512)
    EXPECT_FALSE(tracker.completed_since(0, 512, gen)) << "completion in a non-overlapping range must not be detected";
}

// Partial overlap: write at [256, 768), query [0, 512) → overlap at [256, 512)
TEST(RegionTracker, CompletedSince_PartialOverlapDetected) {
    RegionTracker tracker(16, k_chunk);
    auto const gen = tracker.snapshot_gen();

    tracker.track(256, 512); // [256, 768)
    tracker.untrack(256, 512);

    EXPECT_TRUE(tracker.completed_since(0, 512, gen)) << "partial overlap must be detected";
    EXPECT_FALSE(tracker.completed_since(768, 512, gen)) << "adjacent range must not be detected";
}

// Multiple writes: only the one overlapping the query range triggers completed_since.
TEST(RegionTracker, CompletedSince_MultipleWrites_OnlyOverlappingDetected) {
    RegionTracker tracker(16, k_chunk);
    auto const gen = tracker.snapshot_gen();

    tracker.track(0, 512);
    tracker.untrack(0, 512);
    tracker.track(4096, 512);
    tracker.untrack(4096, 512);

    EXPECT_TRUE(tracker.completed_since(0, 512, gen));
    EXPECT_TRUE(tracker.completed_since(4096, 512, gen));
    EXPECT_FALSE(tracker.completed_since(512, 3584, gen)) << "gap between the two writes must not be detected";
}

// Shadow overflow: more completions than the ring can hold → returns true conservatively.
// The shadow ring is sized to 4×max_slots. Use a 1-slot tracker (shadow size=4) and force 5
// completions before calling completed_since.
TEST(RegionTracker, CompletedSince_ShadowOverflow_ReturnsTrueConservatively) {
    // slot_count=1 → shadow size=4. Five completions overflow the ring by one.
    RegionTracker tracker(1, k_chunk);
    auto const gen = tracker.snapshot_gen(); // capture before any completions

    // Each track/untrack uses the single main slot sequentially (no concurrency here).
    // Use a fixed non-overlapping lba so that without overflow the result would be false.
    constexpr uint64_t k_write_lba = 1 * Ki * Ki; // 1 MiB — far from our query range
    constexpr uint32_t k_len = 512;
    for (int i = 0; i < 5; ++i) {
        tracker.track(k_write_lba, k_len);
        tracker.untrack(k_write_lba, k_len);
    }

    // Query a range that doesn't overlap any of the writes. With no overflow we'd get false;
    // with overflow (head - gen > shadow_size = 4) we must get true conservatively.
    EXPECT_TRUE(tracker.completed_since(0, 512, gen)) << "shadow overflow must return true conservatively";
}

// TSAN target for the multi-producer shadow fix: concurrent untrack() callers must each
// write to a distinct shadow slot. Without the fix (relaxed load + fetch_add), two threads
// can compute the same shadow index, overwrite each other's entry, and completed_since()
// misses one completion (false negative). With the fix (fetch_add(acq_rel) first), each
// caller atomically claims a distinct slot before writing.
//
// Correctness invariant verified: for each tracked (lba, len) pair, after untrack() returns,
// completed_since() with a gen_before captured before untrack() must return true for that
// range. Any failure is a false negative — the very bug this test targets.
TEST(RegionTracker, ConcurrentUntrack_CompletedSince_NoFalseNegatives) {
    constexpr uint32_t k_threads = 8;
    constexpr uint32_t k_iters = 500;
    constexpr uint32_t k_len = 512;
    // Each thread gets a distinct LBA range so main slots don't collide.
    // slot_count = 2×k_threads so all threads can be in-flight simultaneously.
    RegionTracker tracker(k_threads * 2, k_chunk);

    std::atomic< bool > any_false_negative{false};

    std::vector< std::thread > threads;
    threads.reserve(k_threads);
    for (uint32_t t = 0; t < k_threads; ++t) {
        threads.emplace_back([&, t] {
            auto const lba = static_cast< uint64_t >(t) * 1024 * 1024; // distinct per thread
            for (uint32_t i = 0; i < k_iters; ++i) {
                auto const gen = tracker.snapshot_gen();
                tracker.track(lba, k_len);
                tracker.untrack(lba, k_len);
                // After untrack() returns, the shadow entry must be published. completed_since()
                // must return true for the range that just completed.
                if (!tracker.completed_since(lba, k_len, gen))
                    any_false_negative.store(true, std::memory_order_relaxed);
            }
        });
    }

    for (auto& th : threads)
        th.join();

    EXPECT_FALSE(any_false_negative.load())
        << "completed_since() returned false after untrack() completed — shadow entry was lost "
           "(multi-producer race: two threads wrote to the same shadow slot)";
    EXPECT_TRUE(tracker.all_free());
}
