#include <gtest/gtest.h>
#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_superblock.hpp"

using namespace ublkpp;

TEST(BitmapMemoryTest, CalculateMaxMemoryFor2TB) {
    // 2 TB volume with 32 KiB chunks
    auto const volume_size = 2UL * Ti;
    auto const chunk_size = 32 * Ki;

    auto const memory = raid1::Bitmap::calculate_max_memory(volume_size, chunk_size);

    // Expected: 2 TB = 2048 GiB, each page tracks 1 GiB
    // So we need 2048 pages * 4 KiB = 8 MiB + SuperBitmap (4022 bytes)
    auto const expected = (2048UL * 4 * Ki) + raid1::k_superbitmap_size;

    EXPECT_EQ(memory, expected);
}

TEST(BitmapMemoryTest, CalculateMaxMemoryFor150TB) {
    // 150 TB volume with 32 KiB chunks
    auto const volume_size = 150UL * Ti;
    auto const chunk_size = 32 * Ki;

    auto const memory = raid1::Bitmap::calculate_max_memory(volume_size, chunk_size);

    // Expected: 150 TB = 153600 GiB, each page tracks 1 GiB
    // So we need 153600 pages * 4 KiB = 600 MiB + SuperBitmap
    auto const expected_pages = 153600UL;
    auto const expected = (expected_pages * 4 * Ki) + raid1::k_superbitmap_size;

    EXPECT_EQ(memory, expected);

    // Verify it's approximately 600 MiB
    auto const memory_mib = memory / (1024 * 1024);
    EXPECT_EQ(memory_mib, 600UL);
}

TEST(BitmapMemoryTest, CalculateMaxMemorySmallVolume) {
    // 10 GB volume
    auto const volume_size = 10UL * Gi;
    auto const chunk_size = 32 * Ki;

    auto const memory = raid1::Bitmap::calculate_max_memory(volume_size, chunk_size);

    // Expected: 10 GiB, each page tracks 1 GiB
    // So we need 10 pages * 4 KiB = 40 KiB + SuperBitmap
    auto const expected = (10UL * 4 * Ki) + raid1::k_superbitmap_size;

    EXPECT_EQ(memory, expected);
}

TEST(BitmapMemoryTest, RoundUpPartialPage) {
    // Test that partial pages are rounded up
    // 1.5 GiB should require 2 pages, not 1.5
    auto const volume_size = (1UL * Gi) + (512UL * Mi);  // 1.5 GiB
    auto const chunk_size = 32 * Ki;

    auto const memory = raid1::Bitmap::calculate_max_memory(volume_size, chunk_size);

    // Expected: 2 pages * 4 KiB + SuperBitmap (rounded up from 1.5)
    auto const expected = (2UL * 4 * Ki) + raid1::k_superbitmap_size;

    EXPECT_EQ(memory, expected);
}
