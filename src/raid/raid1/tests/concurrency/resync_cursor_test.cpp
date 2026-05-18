#include <gtest/gtest.h>

#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/resync_cursor.hpp"
#include "raid/raid1/tests/test_raid1_common.hpp"

using ublkpp::Gi;
using ublkpp::Ki;
using ublkpp::Mi;
using ublkpp::raid1::Bitmap;
using ublkpp::raid1::ResyncCursor;

static constexpr uint32_t k_chunk = 32 * Ki;
static constexpr uint32_t k_page = 4 * Ki;

// Bitmap stores a raw pointer to its superbitmap buffer but does not own it.
// BitmapFixture keeps both alive together: the unique_ptr owns the buffer,
// the unique_ptr<Bitmap> owns the Bitmap (heap-allocated because Bitmap has
// std::atomic members and is non-movable).
struct BitmapFixture {
    std::unique_ptr< uint8_t[] > sb;
    std::unique_ptr< Bitmap > bitmap;

    explicit BitmapFixture(uint64_t data_size = 1 * Gi) :
            sb(make_test_superbitmap()), bitmap(std::make_unique< Bitmap >(data_size, k_chunk, k_page, sb.get())) {}

    Bitmap& operator*() noexcept { return *bitmap; }
    Bitmap* operator->() noexcept { return bitmap.get(); }
    // Implicit conversion so BitmapFixture can be passed directly to functions
    // taking Bitmap& (e.g. ResyncCursor ctor, cursor.skip(), cursor.advance()).
    operator Bitmap&() noexcept { return *bitmap; }
};

static BitmapFixture make_bitmap(uint64_t data_size = 1 * Gi) { return BitmapFixture(data_size); }

// No hint (hint == 0) → uses next_dirty() and finds the first dirty chunk.
TEST(ResyncCursorTest, ConstructorNoHint) {
    auto bm = make_bitmap();
    bm->dirty_region(64 * Ki, k_chunk);
    ResyncCursor cursor(bm, 0);
    EXPECT_EQ(64 * Ki, cursor.lba);
    EXPECT_GE(cursor.sz, k_chunk);
    EXPECT_FALSE(cursor.any_copy);
    EXPECT_EQ(0U, cursor.skip_from);
}

// Hint points past the first dirty run → next_dirty_after(hint) finds the second run.
TEST(ResyncCursorTest, ConstructorHintSkipsPriorRegion) {
    auto bm = make_bitmap();
    bm->dirty_region(0, k_chunk);        // run A at LBA 0
    bm->dirty_region(128 * Ki, k_chunk); // run B at LBA 128Ki
    // hint = 64Ki: next_dirty_after(64Ki) skips A and finds B
    ResyncCursor cursor(bm, 64 * Ki);
    EXPECT_GE(cursor.lba, 64 * Ki) << "Cursor must start at or after the hint";
    EXPECT_GE(cursor.sz, k_chunk);
}

// Hint falls past all dirty chunks → next_dirty_after returns nothing → fallback to next_dirty().
TEST(ResyncCursorTest, ConstructorHintFallbackToStart) {
    auto bm = make_bitmap();
    bm->dirty_region(0, k_chunk); // only one chunk at LBA 0
    // hint = 64Ki → next_dirty_after(64Ki) returns sz=0 → fallback finds LBA 0
    ResyncCursor cursor(bm, 64 * Ki);
    EXPECT_EQ(0U, cursor.lba);
    EXPECT_GE(cursor.sz, k_chunk);
}

// chunk_len() returns min(sz, max_sz).
TEST(ResyncCursorTest, ChunkLenClampsToMaxSize) {
    auto bm = make_bitmap();
    bm->dirty_region(0, 3 * k_chunk); // sz = 3 * 32Ki = 96Ki
    ResyncCursor cursor(bm, 0);
    EXPECT_EQ(k_chunk, cursor.chunk_len(k_chunk)) << "chunk_len must clamp to max_sz";
    EXPECT_EQ(2 * k_chunk, cursor.chunk_len(2 * k_chunk)) << "chunk_len must return sz when sz < max_sz is false";
    EXPECT_EQ(cursor.sz, cursor.chunk_len(cursor.sz + 1)) << "chunk_len returns sz when max_sz > sz";
}

// skip() with sz > 0 remaining → returns false, cursor moves forward.
TEST(ResyncCursorTest, SkipPartialRegionReturnsFalse) {
    auto bm = make_bitmap();
    bm->dirty_region(0, 2 * k_chunk); // two-chunk run
    ResyncCursor cursor(bm, 0);       // lba=0, sz=2*k_chunk
    bool broke = cursor.skip(k_chunk, bm);
    EXPECT_FALSE(broke);
    EXPECT_EQ(k_chunk, cursor.lba);
    EXPECT_EQ(k_chunk, cursor.sz);
    EXPECT_EQ(0U, cursor.skip_from); // skip_from not touched when sz > 0 after skip
}

// skip() exhausts the entire region with any_copy == false → sets skip_from, returns true.
TEST(ResyncCursorTest, SkipFullRegionNoAnyCopySetsSkipFrom) {
    auto bm = make_bitmap();
    bm->dirty_region(0, k_chunk); // exactly one chunk
    ResyncCursor cursor(bm, 0);   // lba=0, sz=k_chunk, any_copy=false
    bool broke = cursor.skip(k_chunk, bm);
    EXPECT_TRUE(broke) << "skip() must return true to break the inner loop";
    EXPECT_EQ(k_chunk, cursor.skip_from) << "skip_from must point to the end of the exhausted region";
}

// skip() exhausts the region with any_copy == true → calls next_dirty, returns false.
TEST(ResyncCursorTest, SkipFullRegionWithAnyCopyLoadsNext) {
    auto bm = make_bitmap();
    bm->dirty_region(0, 2 * k_chunk);    // run at [0, 64Ki)
    bm->dirty_region(128 * Ki, k_chunk); // separate run at [128Ki, 160Ki)
    ResyncCursor cursor(bm, 0);          // lba=0, sz=2*k_chunk

    // Simulate copying chunk 0 (advance sets any_copy=true; bitmap NOT cleaned here since
    // advance only calls next_dirty when sz reaches 0, which it doesn't yet).
    cursor.advance(k_chunk, bm); // lba=k_chunk, sz=k_chunk, any_copy=true

    // Clean [0, k_chunk) so next_dirty() inside skip() won't re-find it.
    bm->clean_region(0, k_chunk);

    // Now skip chunk 1 (Phase-1 conflict). any_copy=true → loads next dirty, returns false.
    bool broke = cursor.skip(k_chunk, bm);
    EXPECT_FALSE(broke) << "any_copy=true: skip() must return false and load the next dirty region";
    EXPECT_EQ(0U, cursor.skip_from) << "skip_from must not be set when any_copy=true";
    EXPECT_FALSE(cursor.any_copy) << "any_copy must be reset for the new region";
}

// skip_inflight() advances cursor but does NOT set skip_from or any_copy.
TEST(ResyncCursorTest, SkipInflightAdvancesCursor) {
    auto bm = make_bitmap();
    bm->dirty_region(0, 2 * k_chunk);
    ResyncCursor cursor(bm, 0); // lba=0, sz=2*k_chunk
    cursor.skip_inflight(k_chunk, bm);
    EXPECT_EQ(k_chunk, cursor.lba);
    EXPECT_EQ(k_chunk, cursor.sz);
    EXPECT_FALSE(cursor.any_copy); // unchanged
    EXPECT_EQ(0U, cursor.skip_from);
}

// skip_inflight() with sz == 0 after → loads next dirty region.
TEST(ResyncCursorTest, SkipInflightExhaustionLoadsNext) {
    auto bm = make_bitmap();
    bm->dirty_region(0, k_chunk);        // run A
    bm->dirty_region(128 * Ki, k_chunk); // run B
    ResyncCursor cursor(bm, 0);          // lba=0, sz=k_chunk

    // Clean A so next_dirty finds B, not A again.
    bm->clean_region(0, k_chunk);
    cursor.skip_inflight(k_chunk, bm);

    EXPECT_GE(cursor.lba, 128 * Ki) << "After exhausting A, cursor must advance to the next dirty run";
    EXPECT_GE(cursor.sz, k_chunk);
    EXPECT_FALSE(cursor.any_copy);
}

// advance() sets any_copy=true and moves the cursor forward.
TEST(ResyncCursorTest, AdvanceSetsAnyCopy) {
    auto bm = make_bitmap();
    bm->dirty_region(0, 2 * k_chunk);
    ResyncCursor cursor(bm, 0); // lba=0, sz=2*k_chunk, any_copy=false
    cursor.advance(k_chunk, bm);
    EXPECT_TRUE(cursor.any_copy);
    EXPECT_EQ(k_chunk, cursor.lba);
    EXPECT_EQ(k_chunk, cursor.sz);
}

// advance() with sz == 0 after → loads next dirty and resets any_copy for the new region.
TEST(ResyncCursorTest, AdvanceExhaustionLoadsNextAndResetsAnyCopy) {
    auto bm = make_bitmap();
    bm->dirty_region(0, k_chunk);        // run A (1 chunk)
    bm->dirty_region(128 * Ki, k_chunk); // run B
    ResyncCursor cursor(bm, 0);          // lba=0, sz=k_chunk

    // Clean A so next_dirty() inside advance() finds B, not A.
    bm->clean_region(0, k_chunk);
    cursor.advance(k_chunk, bm);

    EXPECT_FALSE(cursor.any_copy) << "any_copy must be reset when advance loads the next region";
    EXPECT_GE(cursor.lba, 128 * Ki);
    EXPECT_GE(cursor.sz, k_chunk);
}
