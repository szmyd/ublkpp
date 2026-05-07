#include <atomic>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "lib/common.hpp"
#include "raid/raid1/region_tracker.hpp"

using ublkpp::Ki;
using ublkpp::raid1::RegionTracker;

TEST(RegionTracker, EmptyNoOverlap) {
    RegionTracker tracker(16);
    EXPECT_FALSE(tracker.overlaps(0, 512 * Ki));
    EXPECT_FALSE(tracker.overlaps(100, 512));
    EXPECT_TRUE(tracker.all_free());
}

TEST(RegionTracker, RegisterAndOverlapExact) {
    RegionTracker tracker(16);
    tracker.track(100, 512);
    EXPECT_TRUE(tracker.overlaps(100, 512));
    EXPECT_FALSE(tracker.all_free());
}

TEST(RegionTracker, OverlapPartialLeft) {
    RegionTracker tracker(16);
    tracker.track(200, 512);                 // [200, 712)
    EXPECT_TRUE(tracker.overlaps(100, 200)); // [100, 300) overlaps at [200, 300)
}

TEST(RegionTracker, OverlapPartialRight) {
    RegionTracker tracker(16);
    tracker.track(100, 512);                 // [100, 612)
    EXPECT_TRUE(tracker.overlaps(500, 200)); // [500, 700) overlaps at [500, 612)
}

TEST(RegionTracker, OverlapContained) {
    RegionTracker tracker(16);
    tracker.track(100, 512);                 // [100, 612)
    EXPECT_TRUE(tracker.overlaps(200, 100)); // [200, 300) fully inside registered range
}

TEST(RegionTracker, AdjacentNoOverlap) {
    RegionTracker tracker(16);
    tracker.track(100, 512);                  // [100, 612)
    EXPECT_FALSE(tracker.overlaps(612, 100)); // [612, 712) — adjacent, not overlapping
    EXPECT_FALSE(tracker.overlaps(0, 100));   // [0, 100) — adjacent on the other side
}

TEST(RegionTracker, UnregisterClearsSlot) {
    RegionTracker tracker(16);
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
    RegionTracker tracker(16);
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
    RegionTracker tracker(16);
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
    RegionTracker tracker(k_slots);

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
    RegionTracker tracker(k_threads * 2);

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
