#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "raid/raid1/bitmap.hpp"

#define Ki 1024

TEST(Raid1, CalcBitmapRegions) {
    static uint32_t chunk_size = 32 * Ki;
    static uint32_t page_width = chunk_size * ublkpp::raid1::k_page_size *
        ublkpp::raid1::k_bits_in_byte;                              // How many user data bytes does a BITMAP page cover
    static uint32_t word_width = chunk_size * sizeof(uint64_t) * 8; // How many user data bytes does a BITMAP WORD cover

    using ublkpp::raid1::Bitmap;
    // Test simple first chunk dirty
    {
        auto [page_offset, word_offset, shift_offset, sz] = Bitmap::calc_bitmap_region(0, 4 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(63, shift_offset);
        EXPECT_EQ(4 * Ki, sz);
    }

    // Still in first chunk
    {
        auto [page_offset, word_offset, shift_offset, sz] = Bitmap::calc_bitmap_region(4 * Ki, chunk_size, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(63, shift_offset);
        EXPECT_EQ(chunk_size, sz);
    }

    // Second chunk (still in first word and first page)
    {
        auto [page_offset, word_offset, shift_offset, sz] = Bitmap::calc_bitmap_region(chunk_size, 16 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(62, shift_offset);
        EXPECT_EQ(16 * Ki, sz);
    }

    // Last bit (chunk) of the first word and page
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            Bitmap::calc_bitmap_region((chunk_size * 64) - 4 * Ki, 16 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(0, shift_offset);
        EXPECT_EQ(16 * Ki, sz);
    }

    // Middle bit (chunk) of the first word and page of differing chunk size
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            Bitmap::calc_bitmap_region((chunk_size * 64) - 4 * Ki, 16 * Ki, chunk_size * 2);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(32, shift_offset);
        EXPECT_EQ(16 * Ki, sz);
    }

    // Second word; first Chunk
    {
        auto [page_offset, word_offset, shift_offset, sz] = Bitmap::calc_bitmap_region(word_width, 16 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(1, word_offset);
        EXPECT_EQ(63, shift_offset);
        EXPECT_EQ(16 * Ki, sz);
    }

    // Second page, all in the first word
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            Bitmap::calc_bitmap_region(page_width, 128 * Ki, chunk_size);
        EXPECT_EQ(1, page_offset);
        EXPECT_EQ(0, word_offset);
        EXPECT_EQ(63, shift_offset);
        EXPECT_EQ(128 * Ki, sz);
    }

    // First page last word and bit, sz is truncated at page boundary
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            Bitmap::calc_bitmap_region(page_width - (chunk_size), 128 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(511, word_offset);
        EXPECT_EQ(0, shift_offset);
        EXPECT_EQ(chunk_size, sz);
    }

    // First page, last word and bit offset into chunk, truncated at page boundary
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            Bitmap::calc_bitmap_region(page_width - (4 * Ki), 12 * Ki, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(511, word_offset);
        EXPECT_EQ(0, shift_offset);
        EXPECT_EQ(4 * Ki, sz);
    }

    // First page, last word and bit offset into chunk, truncated at page boundary
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            Bitmap::calc_bitmap_region(page_width - (4 * Ki), 2 * chunk_size, chunk_size);
        EXPECT_EQ(0, page_offset);
        EXPECT_EQ(511, word_offset);
        EXPECT_EQ(0, shift_offset);
        EXPECT_EQ(4 * Ki, sz);
        {
            auto [pg_offset2, word_offset2, shift_offset2, sz2] =
                Bitmap::calc_bitmap_region(page_width - (4 * Ki) + (sz), (2 * chunk_size) - sz, chunk_size);
            EXPECT_EQ(1, pg_offset2);
            EXPECT_EQ(0, word_offset2);
            EXPECT_EQ(63, shift_offset2);
            EXPECT_EQ((2 * chunk_size) - sz, sz2);
        }
    }

    // Third page, middle of second word
    {
        auto [page_offset, word_offset, shift_offset, sz] =
            Bitmap::calc_bitmap_region((page_width * 2) + word_width + (3 * chunk_size), 5 * chunk_size, chunk_size);
        EXPECT_EQ(2, page_offset);
        EXPECT_EQ(1, word_offset);
        EXPECT_EQ(60, shift_offset);
        EXPECT_EQ(5 * chunk_size, sz);
    }
}
