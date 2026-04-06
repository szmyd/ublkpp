#include <gtest/gtest.h>
#include "ublkpp/raid/raid1.hpp"

// Note: Tests assume chunk_size=32768 (32 KiB) from SISL options
// Each bitmap page covers 1 GiB of data (32KiB × 4096 × 8)

TEST(Raid1MemoryEstimation, OneTiB) {
    uint64_t volume_size = 1ULL * 1024 * 1024 * 1024 * 1024;
    uint64_t memory = ublkpp::Raid1Disk::estimate_device_overhead(volume_size);
    
    EXPECT_GT(memory, 4ULL * 1024 * 1024);
    EXPECT_LT(memory, 4.1 * 1024 * 1024);
}

TEST(Raid1MemoryEstimation, TenTiB) {
    uint64_t volume_size = 10ULL * 1024 * 1024 * 1024 * 1024;
    uint64_t memory = ublkpp::Raid1Disk::estimate_device_overhead(volume_size);
    
    EXPECT_GT(memory, 40ULL * 1024 * 1024);
    EXPECT_LT(memory, 41ULL * 1024 * 1024);
}

TEST(Raid1MemoryEstimation, SmallVolume) {
    uint64_t volume_size = 512ULL * 1024 * 1024;
    uint64_t memory = ublkpp::Raid1Disk::estimate_device_overhead(volume_size);
    
    EXPECT_GT(memory, 15ULL * 1024);
    EXPECT_LT(memory, 20ULL * 1024);
}

TEST(Raid1MemoryEstimation, LinearScaling) {
    uint64_t vol_1tib = 1ULL * 1024 * 1024 * 1024 * 1024;
    uint64_t vol_10tib = 10ULL * 1024 * 1024 * 1024 * 1024;
    
    uint64_t mem_1tib = ublkpp::Raid1Disk::estimate_device_overhead(vol_1tib);
    uint64_t mem_10tib = ublkpp::Raid1Disk::estimate_device_overhead(vol_10tib);
    
    double ratio = static_cast<double>(mem_10tib) / mem_1tib;
    EXPECT_GT(ratio, 9.5);
    EXPECT_LT(ratio, 10.5);
}

TEST(Raid1MemoryEstimation, VeryLargeVolume) {
    uint64_t volume_size = 100ULL * 1024 * 1024 * 1024 * 1024;
    uint64_t memory = ublkpp::Raid1Disk::estimate_device_overhead(volume_size);
    
    EXPECT_GT(memory, 400ULL * 1024 * 1024);
    EXPECT_LT(memory, 410ULL * 1024 * 1024);
}
