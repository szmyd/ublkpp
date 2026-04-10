#include <gtest/gtest.h>
#include "ublkpp/raid/raid1.hpp"

// Note: Tests assume chunk_size=32768 (32 KiB) from SISL options
// Each bitmap page covers 1 GiB of data (32KiB × 4096 × 8)
//
// Memory calculation:
//   superblock (4 KiB) + PageData_vector (num_pages × 24) + clean_page (4 KiB) + dirty_pages (num_pages × 4 KiB)

TEST(Raid1MemoryEstimation, SmallVolume) {
    uint64_t volume_size = 512ULL * 1024 * 1024;
    uint64_t memory = ublkpp::Raid1Disk::estimate_device_overhead(volume_size);

    // 1 page: 4096 + 24 + 4096 + 4096 = 12,312 bytes
    EXPECT_EQ(memory, 12312ULL);
}

TEST(Raid1MemoryEstimation, OneTiB) {
    uint64_t volume_size = 1ULL * 1024 * 1024 * 1024 * 1024;
    uint64_t memory = ublkpp::Raid1Disk::estimate_device_overhead(volume_size);

    // 1024 pages: 4096 + 24576 + 4096 + 4194304 = 4,227,072 bytes
    EXPECT_EQ(memory, 4227072ULL);
}

TEST(Raid1MemoryEstimation, TenTiB) {
    uint64_t volume_size = 10ULL * 1024 * 1024 * 1024 * 1024;
    uint64_t memory = ublkpp::Raid1Disk::estimate_device_overhead(volume_size);

    // 10240 pages: 4096 + 245760 + 4096 + 41943040 = 42,196,992 bytes
    EXPECT_EQ(memory, 42196992ULL);
}

TEST(Raid1MemoryEstimation, LinearScaling) {
    uint64_t vol_1tib = 1ULL * 1024 * 1024 * 1024 * 1024;
    uint64_t vol_10tib = 10ULL * 1024 * 1024 * 1024 * 1024;

    uint64_t mem_1tib = ublkpp::Raid1Disk::estimate_device_overhead(vol_1tib);
    uint64_t mem_10tib = ublkpp::Raid1Disk::estimate_device_overhead(vol_10tib);

    double ratio = static_cast< double >(mem_10tib) / mem_1tib;
    EXPECT_GT(ratio, 9.5);
    EXPECT_LT(ratio, 10.5);
}

TEST(Raid1MemoryEstimation, VeryLargeVolume) {
    uint64_t volume_size = 100ULL * 1024 * 1024 * 1024 * 1024;
    uint64_t memory = ublkpp::Raid1Disk::estimate_device_overhead(volume_size);

    // 102400 pages: 4096 + 2457600 + 4096 + 419430400 = 421,896,192 bytes
    EXPECT_EQ(memory, 421896192ULL);
}
