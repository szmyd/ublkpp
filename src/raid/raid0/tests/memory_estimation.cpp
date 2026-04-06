#include <gtest/gtest.h>
#include "ublkpp/raid/raid0.hpp"

// Test: 2-disk stripe overhead
TEST(Raid0MemoryEstimation, TwoDisks) {
    uint64_t memory = ublkpp::Raid0Disk::estimate_device_overhead(2);
    // Expected: 2 × 4 KiB SuperBlocks = 8 KiB
    EXPECT_EQ(memory, 8192ULL);
}

// Test: 4-disk stripe overhead
TEST(Raid0MemoryEstimation, FourDisks) {
    uint64_t memory = ublkpp::Raid0Disk::estimate_device_overhead(4);
    // Expected: 4 × 4 KiB SuperBlocks = 16 KiB
    EXPECT_EQ(memory, 16384ULL);
}

// Test: 8-disk stripe overhead
TEST(Raid0MemoryEstimation, EightDisks) {
    uint64_t memory = ublkpp::Raid0Disk::estimate_device_overhead(8);
    // Expected: 8 × 4 KiB SuperBlocks = 32 KiB
    EXPECT_EQ(memory, 32768ULL);
}

// Test: Single disk (edge case)
TEST(Raid0MemoryEstimation, SingleDisk) {
    uint64_t memory = ublkpp::Raid0Disk::estimate_device_overhead(1);
    // Expected: 1 × 4 KiB SuperBlock = 4 KiB
    EXPECT_EQ(memory, 4096ULL);
}

// Test: Disk count scales linearly
TEST(Raid0MemoryEstimation, LinearScaling) {
    uint64_t memory_2 = ublkpp::Raid0Disk::estimate_device_overhead(2);
    uint64_t memory_4 = ublkpp::Raid0Disk::estimate_device_overhead(4);
    uint64_t memory_8 = ublkpp::Raid0Disk::estimate_device_overhead(8);

    // Should scale linearly: 2x disks = 2x memory
    EXPECT_EQ(memory_4, memory_2 * 2);
    EXPECT_EQ(memory_8, memory_4 * 2);
}
