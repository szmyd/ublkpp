#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/tests/test_raid1_common.hpp"

using ublkpp::Gi;
using ublkpp::Ki;

// When all bits on a page are cleared, clean_region must clear the superbitmap bit and
// return the shared clean page (non-null), signalling to the caller that the page slot
// is now empty.
TEST(Raid1CleanRegion, FullCleanClearsSuperbitmap) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());

    bitmap.dirty_region(0, 32 * Ki);
    EXPECT_EQ(1UL, bitmap.dirty_pages());

    auto [page_ptr, page_off, sz] = bitmap.clean_region(0, 32 * Ki);

    // Superbitmap bit must be cleared — dirty_pages() and next_dirty() use it to scan
    EXPECT_EQ(0UL, bitmap.dirty_pages());
    auto [next_off, next_len] = bitmap.next_dirty();
    EXPECT_EQ(0U, next_len);

    // Non-null page pointer signals the page became fully clean (caller swaps to clean_page)
    EXPECT_NE(nullptr, page_ptr);
}

// When only part of a page is cleaned, the remaining dirty bits keep the superbitmap bit set.
TEST(Raid1CleanRegion, PartialCleanPreservesSuperbitmap) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());

    // Dirty two consecutive chunks in the same bitmap page
    bitmap.dirty_region(0, 64 * Ki);
    EXPECT_EQ(1UL, bitmap.dirty_pages());

    // Clean only the first chunk — the second remains dirty
    auto [page_ptr, page_off, sz] = bitmap.clean_region(0, 32 * Ki);

    // Superbitmap bit must stay set
    EXPECT_EQ(1UL, bitmap.dirty_pages());
    auto [next_off, next_len] = bitmap.next_dirty();
    EXPECT_GT(next_len, 0U);

    // Null pointer: page is not empty, caller keeps the dirty page in the slot
    EXPECT_EQ(nullptr, page_ptr);
}

// dirty_region followed by clean_region followed by dirty_region again must leave the
// superbitmap in the correct state at each step.  This exercises the invariant that
// clear_bit in clean_region does not permanently suppress a subsequent set_bit from
// dirty_region.
TEST(Raid1CleanRegion, DirtyCleanDirtyCyclePreservesInvariant) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());

    // Cycle 1
    bitmap.dirty_region(0, 32 * Ki);
    EXPECT_EQ(1UL, bitmap.dirty_pages());

    bitmap.clean_region(0, 32 * Ki);
    EXPECT_EQ(0UL, bitmap.dirty_pages());

    // Cycle 2 — superbitmap must be re-set correctly
    bitmap.dirty_region(0, 32 * Ki);
    EXPECT_EQ(1UL, bitmap.dirty_pages());

    // And clean again
    bitmap.clean_region(0, 32 * Ki);
    EXPECT_EQ(0UL, bitmap.dirty_pages());
}

// Two threads calling clean_region on different chunks of the same page concurrently must not
// underflow _dirty_chunks_est to UINT64_MAX. The old load+fetch_sub pair was not jointly atomic:
// both threads could read the same counter value and both subtract, wrapping to UINT64_MAX.
TEST(Raid1CleanRegion, ConcurrentCleanDoesNotUnderflowDirtyEst) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());

    // Dirty two adjacent chunks so _dirty_chunks_est == 2.
    bitmap.dirty_region(0, 32 * Ki);
    bitmap.dirty_region(32 * Ki, 32 * Ki);
    EXPECT_EQ(2UL * 32 * Ki, bitmap.dirty_data_est());

    // Clean both chunks from separate threads simultaneously.
    std::thread t1([&] { bitmap.clean_region(0, 32 * Ki); });
    std::thread t2([&] { bitmap.clean_region(32 * Ki, 32 * Ki); });
    t1.join();
    t2.join();

    // Counter must be 0, not UINT64_MAX.
    EXPECT_EQ(0UL, bitmap.dirty_data_est());
}

// Cleaning an already-clean page (no page allocated) is a safe no-op.
TEST(Raid1CleanRegion, CleanAlreadyCleanPageIsNoOp) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());

    // Never dirtied — clean_region should not crash and superbitmap stays clear
    auto [page_ptr, page_off, sz] = bitmap.clean_region(0, 32 * Ki);
    EXPECT_EQ(0UL, bitmap.dirty_pages());
    EXPECT_EQ(nullptr, page_ptr);
}
