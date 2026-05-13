#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/tests/test_raid1_common.hpp"

using ublkpp::Gi;
using ublkpp::Ki;
using ublkpp::Mi;

// Test the iteration through dirty pages
TEST(Raid1, NextDirty) {
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(100 * ublkpp::Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());
    bitmap.dirty_region(0x4096, 512 * Ki);
    bitmap.dirty_region(0x23f1000, 16 * Ki);
    bitmap.dirty_region(0x23f8000, 64 * Ki);
    bitmap.dirty_region(ublkpp::Gi - (4 * Ki), 8 * Ki);
    bitmap.dirty_region(ublkpp::Gi, 4 * Ki);
    bitmap.dirty_region(5 * ublkpp::Gi, 4 * Ki);
    EXPECT_EQ(3, bitmap.dirty_pages());
    {
        auto [off, len] = bitmap.next_dirty();
        EXPECT_EQ(0, off);                      // Chunk aligned
        EXPECT_EQ((512 * Ki) + (32 * Ki), len); // Cross word dirty
        bitmap.clean_region(off, len);
    }
    {
        auto [off, len] = bitmap.next_dirty();
        EXPECT_EQ(0x23f0000, off); // Chunk aligned
        EXPECT_EQ(64 * Ki, len);   // Merged dirty
        bitmap.clean_region(off, len);
    }
    {
        auto [off, len] = bitmap.next_dirty();
        EXPECT_EQ(0x2400000, off); // Chunk aligned
        EXPECT_EQ(32 * Ki, len);   // Merged dirty
        bitmap.clean_region(off, len);
    }
    {
        auto [off, len] = bitmap.next_dirty();
        EXPECT_EQ(ublkpp::Gi - (32 * Ki), off);
        EXPECT_EQ(32 * Ki, len); // Split dirty
        bitmap.clean_region(off, len);
    }
    EXPECT_EQ(2, bitmap.dirty_pages());
    {
        auto [off, len] = bitmap.next_dirty();
        EXPECT_EQ(ublkpp::Gi, off);
        EXPECT_EQ(32 * Ki, len);
        bitmap.clean_region(off, len);
    }
    EXPECT_EQ(1, bitmap.dirty_pages());
    {
        auto [off, len] = bitmap.next_dirty();
        EXPECT_EQ(5 * ublkpp::Gi, off);
        EXPECT_EQ(32 * Ki, len);
        bitmap.clean_region(off, len);
    }
    EXPECT_EQ(0, bitmap.dirty_pages());
    {
        auto [off, len] = bitmap.next_dirty();
        EXPECT_EQ(0U, len);
    }
}

// next_dirty_after() with nothing dirty → returns {0, 0}
TEST(Raid1NextDirtyAfter, EmptyBitmapReturnsZero) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());
    auto [off, len] = bitmap.next_dirty_after(0);
    EXPECT_EQ(0U, len);
}

// cursor points exactly at the only dirty chunk → finds it
TEST(Raid1NextDirtyAfter, CursorAtDirtyChunkFindsIt) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());
    bitmap.dirty_region(512 * Ki, 32 * Ki);
    auto [off, len] = bitmap.next_dirty_after(512 * Ki);
    EXPECT_EQ(512 * Ki, off);
    EXPECT_EQ(32 * Ki, len);
}

// cursor is before the dirty chunk → still finds it
TEST(Raid1NextDirtyAfter, CursorBeforeDirtyChunkFindsIt) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());
    bitmap.dirty_region(256 * Ki, 32 * Ki);
    auto [off, len] = bitmap.next_dirty_after(0);
    EXPECT_EQ(256 * Ki, off);
    EXPECT_EQ(32 * Ki, len);
}

// cursor is past the only dirty chunk → returns {0, 0}
TEST(Raid1NextDirtyAfter, CursorPastAllDirtyReturnsZero) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());
    bitmap.dirty_region(0, 32 * Ki);
    auto [off, len] = bitmap.next_dirty_after(64 * Ki);
    EXPECT_EQ(0U, len);
}

// cursor lands mid-run → returns the remainder of the run (chunk-aligned at cursor)
TEST(Raid1NextDirtyAfter, CursorMidRunReturnsRemainder) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());
    // Dirty three consecutive chunks: 0, 32K, 64K
    bitmap.dirty_region(0, 96 * Ki);
    // Cursor at 32K: should return starting at 32K
    auto [off, len] = bitmap.next_dirty_after(32 * Ki);
    EXPECT_EQ(32 * Ki, off);
    EXPECT_GE(len, 32 * Ki); // at least the remaining two chunks
}

// two dirty runs on different pages; cursor skips past the first page entirely
TEST(Raid1NextDirtyAfter, CursorSkipsFirstPageFindsSecond) {
    auto sb = make_test_superbitmap();
    // 100 GiB device; each page covers page_width = data_size/num_pages bytes.
    // With 4KiB pages and 32KiB chunks: page_width = (4Ki/sizeof(word_t)) * 64 * 32Ki = 1 GiB per page.
    auto bitmap = ublkpp::raid1::Bitmap(100 * Gi, 32 * Ki, 4 * Ki, sb.get());
    bitmap.dirty_region(0, 32 * Ki);      // page 0
    bitmap.dirty_region(2 * Gi, 32 * Ki); // page 2
    // Cursor well past page 0 → should land on page 2 run
    auto [off, len] = bitmap.next_dirty_after(Gi);
    EXPECT_EQ(2 * Gi, off);
    EXPECT_EQ(32 * Ki, len);
}

// cursor lands exactly on the first chunk of a different page
TEST(Raid1NextDirtyAfter, CursorAtPageBoundaryFindsRun) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(100 * Gi, 32 * Ki, 4 * Ki, sb.get());
    bitmap.dirty_region(Gi, 32 * Ki); // first chunk of page 1
    auto [off, len] = bitmap.next_dirty_after(Gi);
    EXPECT_EQ(Gi, off);
    EXPECT_EQ(32 * Ki, len);
}

// cursor one chunk past a page boundary — dirty chunk is exactly at cursor
TEST(Raid1NextDirtyAfter, CursorOneChunkIntoDirtyPageFindsRemainder) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(100 * Gi, 32 * Ki, 4 * Ki, sb.get());
    bitmap.dirty_region(Gi, 64 * Ki); // two consecutive chunks at start of page 1
    auto [off, len] = bitmap.next_dirty_after(Gi + 32 * Ki);
    EXPECT_EQ(Gi + 32 * Ki, off);
    EXPECT_EQ(32 * Ki, len);
}

// dirty run straddles a word boundary; cursor points into the second word
TEST(Raid1NextDirtyAfter, CursorInSecondWordOfCrossWordRun) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(100 * Gi, 32 * Ki, 4 * Ki, sb.get());
    // 64 chunks per word × 32KiB = 2MiB per word. Dirty 3MiB (96 chunks) to cross word 0→1.
    bitmap.dirty_region(0, 3 * Mi);
    // Cursor at word boundary (2MiB): should find the remainder of the run in word 1
    auto [off, len] = bitmap.next_dirty_after(2 * Mi);
    EXPECT_EQ(2 * Mi, off);
    EXPECT_GE(len, 32 * Ki);
}

// cursor is one chunk before the end of the device; dirty chunk is at the very end
TEST(Raid1NextDirtyAfter, CursorNearEndOfDevice) {
    auto sb = make_test_superbitmap();
    constexpr uint64_t dev_size = 100 * Gi;
    auto bitmap = ublkpp::raid1::Bitmap(dev_size, 32 * Ki, 4 * Ki, sb.get());
    bitmap.dirty_region(dev_size - 32 * Ki, 32 * Ki);
    auto [off, len] = bitmap.next_dirty_after(dev_size - 32 * Ki);
    EXPECT_EQ(dev_size - 32 * Ki, off);
    EXPECT_EQ(32 * Ki, len);
}

// two adjacent dirty chunks; cursor between them picks up only the second
TEST(Raid1NextDirtyAfter, TwoAdjacentChunksCursorBetween) {
    auto sb = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(10 * Gi, 32 * Ki, 4 * Ki, sb.get());
    bitmap.dirty_region(0, 32 * Ki);
    bitmap.dirty_region(32 * Ki, 32 * Ki);
    // Cursor after first chunk — next_dirty_after should not return the first chunk
    auto [off, len] = bitmap.next_dirty_after(32 * Ki);
    EXPECT_GE(off, 32 * Ki);
    EXPECT_GE(len, 32 * Ki);
}
