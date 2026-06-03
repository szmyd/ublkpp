#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/tests/test_raid1_common.hpp"

using ublkpp::Ki;

// Ensure that all required pages are initialized
TEST(Raid1, DoesNotCrossPage) {
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());
    bitmap.dirty_region(ublkpp::Gi - (4 * Ki), 12 * Ki);
    EXPECT_TRUE(bitmap.is_dirty(ublkpp::Gi + (4 * Ki), 4 * Ki));
}

TEST(Raid1, IsDirtyNextPage) {
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());
    bitmap.dirty_region(ublkpp::Gi + (4 * Ki), 12 * Ki);
    EXPECT_TRUE(bitmap.is_dirty(ublkpp::Gi - (4 * Ki), 8 * Ki));
}

// After a full clean cycle (superbitmap cleared) followed by dirty_region, is_dirty must
// return true based on page bits — not be suppressed by the now-cleared superbitmap.
// dirty_region re-sets the superbitmap, but the code path under test is the direct page-bit
// read that executes regardless of the superbitmap state.
TEST(Raid1, IsDirtyReflectsPageBitsAfterSuperbitmapCleared) {
    auto superbitmap_buf = make_test_superbitmap();
    // Two 32KiB chunks on the same bitmap page (page covers 1 GiB of data)
    auto bitmap = ublkpp::raid1::Bitmap(10 * ublkpp::Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());

    // Dirty both chunks → superbitmap SET
    bitmap.dirty_region(0, 32 * Ki);
    bitmap.dirty_region(32 * Ki, 32 * Ki);
    ASSERT_EQ(1UL, bitmap.dirty_pages());

    // Clean both → all bits zero → superbitmap CLEARED
    bitmap.clean_region(0, 32 * Ki);
    bitmap.clean_region(32 * Ki, 32 * Ki);
    ASSERT_EQ(0UL, bitmap.dirty_pages());
    EXPECT_FALSE(bitmap.is_dirty(0, 32 * Ki));

    // Re-dirty the first chunk (page is already allocated from the first cycle).
    // dirty_region will set the bit AND call set_bit on the superbitmap; is_dirty must
    // return true regardless — this exercises the direct page-bit check for an existing page.
    bitmap.dirty_region(0, 32 * Ki);
    EXPECT_TRUE(bitmap.is_dirty(0, 32 * Ki));
    EXPECT_FALSE(bitmap.is_dirty(32 * Ki, 32 * Ki)); // second chunk stays clean
}

// Some actual problematic Bitmap bugs encountered when testing BufferedI/O
TEST(Raid1, BufferedIoTest) {
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());
    bitmap.dirty_region(0xf7b000, 512 * Ki);
    bitmap.dirty_region(0xffb000, 512 * Ki);
    bitmap.dirty_region(0x22ac000, 512 * Ki);
    bitmap.dirty_region(0x232c000, 312 * Ki);
    bitmap.dirty_region(0x237a000, 512 * Ki);
    bitmap.dirty_region(0x23fa000, 512 * Ki);
    EXPECT_TRUE(bitmap.is_dirty(0x2448000, 44 * Ki));
}

// H6 regression: is_dirty must read page bits directly even when the superbitmap bit is
// cleared. The exact window that motivated the fix: superbitmap shows "clean" (bit cleared
// transiently by clean_region) while the page still has dirty bits set. A superbitmap-based
// fast-path would return false here; the page-bit read must return true.
//
// We simulate the transient superbitmap state by manually zeroing the superbitmap byte that
// covers page 0 — equivalent to what clean_region does between clear_bit and the double-check.
TEST(Raid1, IsDirtyPageBitsSetSuperbitmapClearedManually) {
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * ublkpp::Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());

    bitmap.dirty_region(0, 32 * Ki);
    ASSERT_EQ(1UL, bitmap.dirty_pages());

    // Simulate the transient clear_bit() that clean_region does before its double-check.
    // Page 0 maps to byte 0, bit 0 of the superbitmap.
    superbitmap_buf.get()[0] = 0;
    ASSERT_EQ(0UL, bitmap.dirty_pages()); // superbitmap now says clean...

    // ...but is_dirty reads page bits directly and must still return true.
    EXPECT_TRUE(bitmap.is_dirty(0, 32 * Ki));
}
