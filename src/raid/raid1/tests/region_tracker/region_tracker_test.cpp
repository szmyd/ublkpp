#include <atomic>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "raid/raid1/region_tracker.hpp"
#include "ublkpp/lib/common.hpp"

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
    tracker.register_write(100, 512);
    EXPECT_TRUE(tracker.overlaps(100, 512));
    EXPECT_FALSE(tracker.all_free());
}

TEST(RegionTracker, OverlapPartialLeft) {
    RegionTracker tracker(16);
    tracker.register_write(200, 512);        // [200, 712)
    EXPECT_TRUE(tracker.overlaps(100, 200)); // [100, 300) overlaps at [200, 300)
}

TEST(RegionTracker, OverlapPartialRight) {
    RegionTracker tracker(16);
    tracker.register_write(100, 512);        // [100, 612)
    EXPECT_TRUE(tracker.overlaps(500, 200)); // [500, 700) overlaps at [500, 612)
}

TEST(RegionTracker, OverlapContained) {
    RegionTracker tracker(16);
    tracker.register_write(100, 512);        // [100, 612)
    EXPECT_TRUE(tracker.overlaps(200, 100)); // [200, 300) fully inside registered range
}

TEST(RegionTracker, AdjacentNoOverlap) {
    RegionTracker tracker(16);
    tracker.register_write(100, 512);         // [100, 612)
    EXPECT_FALSE(tracker.overlaps(612, 100)); // [612, 712) — adjacent, not overlapping
    EXPECT_FALSE(tracker.overlaps(0, 100));   // [0, 100) — adjacent on the other side
}

TEST(RegionTracker, UnregisterClearsSlot) {
    RegionTracker tracker(16);
    tracker.register_write(100, 512);
    EXPECT_TRUE(tracker.overlaps(100, 512));
    tracker.unregister_write(100, 512);
    EXPECT_FALSE(tracker.overlaps(100, 512));
    EXPECT_TRUE(tracker.all_free());
}

TEST(RegionTracker, DuplicateLba_TwoSlots) {
    // Primary and replica legs of the same write register independently
    RegionTracker tracker(16);
    tracker.register_write(100, 512);
    tracker.register_write(100, 512); // same range, separate slot
    EXPECT_TRUE(tracker.overlaps(100, 512));

    tracker.unregister_write(100, 512);      // clears one slot
    EXPECT_TRUE(tracker.overlaps(100, 512)); // second slot still occupied

    tracker.unregister_write(100, 512); // clears second slot
    EXPECT_FALSE(tracker.overlaps(100, 512));
    EXPECT_TRUE(tracker.all_free());
}

TEST(RegionTracker, MultipleDistinctRanges) {
    RegionTracker tracker(16);
    tracker.register_write(0, 512);
    tracker.register_write(1024, 512);
    tracker.register_write(4096, 512);

    EXPECT_TRUE(tracker.overlaps(0, 512));
    EXPECT_TRUE(tracker.overlaps(1024, 512));
    EXPECT_TRUE(tracker.overlaps(4096, 512));
    EXPECT_FALSE(tracker.overlaps(512, 512));   // gap between first and second
    EXPECT_FALSE(tracker.overlaps(1536, 2560)); // gap between second and third

    tracker.unregister_write(1024, 512);
    EXPECT_FALSE(tracker.overlaps(1024, 512));
    EXPECT_TRUE(tracker.overlaps(0, 512));
    EXPECT_TRUE(tracker.overlaps(4096, 512));
}

TEST(RegionTracker, FillAndDrain) {
    constexpr uint32_t k_slots = 8;
    RegionTracker tracker(k_slots);

    for (uint32_t i = 0; i < k_slots; ++i)
        tracker.register_write(static_cast< uint64_t >(i) * 1024, 512);

    EXPECT_FALSE(tracker.all_free());

    // Unregister one, verify a new register can take its place
    tracker.unregister_write(0, 512);
    tracker.register_write(k_slots * 1024, 512);
    EXPECT_TRUE(tracker.overlaps(k_slots * 1024, 512));

    // Drain remaining
    for (uint32_t i = 1; i < k_slots; ++i)
        tracker.unregister_write(static_cast< uint64_t >(i) * 1024, 512);
    tracker.unregister_write(k_slots * 1024, 512);
    EXPECT_TRUE(tracker.all_free());
}

TEST(RegionTracker, ConcurrentRegisterUnregister) {
    constexpr uint32_t k_threads = 8;
    constexpr uint32_t k_iters = 200;
    // Slot count sized to 2× threads so no exhaustion occurs
    RegionTracker tracker(k_threads * 2);

    std::vector< std::thread > threads;
    threads.reserve(k_threads);

    for (uint32_t t = 0; t < k_threads; ++t) {
        threads.emplace_back([&tracker, t] {
            for (uint32_t i = 0; i < k_iters; ++i) {
                // Each thread operates on its own non-overlapping LBA range
                auto const lba = static_cast< uint64_t >(t) * 1024 * 1024;
                tracker.register_write(lba, 512);
                tracker.unregister_write(lba, 512);
            }
        });
    }

    for (auto& th : threads)
        th.join();

    EXPECT_TRUE(tracker.all_free());
}
