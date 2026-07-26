#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "tests/test_disk.hpp"
#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_superblock.hpp"
#include "raid/raid1/tests/test_raid1_common.hpp"

#ifndef Ki
using ublkpp::Gi;
using ublkpp::Ki;
#endif

// A device whose data region is not a chunk multiple leaves a partial chunk at the tail. A
// full resync's final clean_region ends unaligned at exactly data_size; this must clear the
// tail bit rather than abort. Regression for the resync-completion SIGABRT observed when the
// sweep reached the tail (clean_region(addr, len) with len % chunk != 0, addr+len == data end).
TEST(Raid1, PartialTailChunkClean) {
    auto const chunk_size = 32 * Ki;
    auto const data_size = 1 * Gi + 300 * Ki; // 300Ki % 32Ki != 0 -> partial tail chunk
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(data_size, chunk_size, 4 * Ki, superbitmap_buf.get());

    // Dirty everything (dirty_region accepts arbitrary ranges), then clean in resync order:
    // aligned full chunks first, then the final partial region ending exactly at data_size.
    bitmap.dirty_region(0, data_size);
    EXPECT_TRUE(bitmap.is_dirty(0, data_size));

    // clean_region is a single page-span primitive; iterate on the consumed size as the
    // resync path does.
    auto const clean_range = [&bitmap](uint64_t addr, uint64_t len) {
        for (uint64_t off = 0; off < len;) {
            auto const [page, page_offset, sz] = bitmap.clean_region(addr + off, len - off);
            (void)page;
            (void)page_offset;
            off += sz;
        }
    };

    auto const tail_start = (data_size / chunk_size - 4) * chunk_size;
    clean_range(0, tail_start);

    auto const tail_len = static_cast< uint32_t >(data_size - tail_start);
    EXPECT_NE(0u, tail_len % chunk_size); // the interesting case: unaligned final region
    clean_range(tail_start, tail_len);

    EXPECT_FALSE(bitmap.is_dirty(0, data_size));
    EXPECT_EQ(0u, bitmap.dirty_pages());
}
