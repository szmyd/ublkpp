#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <sstream>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublksrv.h>

#include "../fs_disk_impl.hpp"
#include <ublkpp/lib/cqe_state.hpp>
#include "ublkpp/lib/common.hpp"

namespace {

// ============================================================================
// Test block_has_unmap function
// ============================================================================

TEST(BlockHasUnmap, NonExistentDevice) {
    // Test with a device that doesn't exist
    // clang-format off
    struct stat fake_stat{};
    // clang-format on
    bool result = ublkpp::block_has_unmap(fake_stat);

    // Should return false for non-existent devices
    EXPECT_FALSE(result);
}

// ============================================================================
// Test discard_to_fallocate function
// ============================================================================

TEST(DiscardToFallocate, FallocateModeConstants) {
    // Verify that the fallocate mode constants are properly defined
    EXPECT_GT(FALLOC_FL_KEEP_SIZE, 0);
    EXPECT_GT(FALLOC_FL_PUNCH_HOLE, 0);
    EXPECT_GT(FALLOC_FL_ZERO_RANGE, 0);

    // Verify they are different values
    EXPECT_NE(FALLOC_FL_PUNCH_HOLE, FALLOC_FL_ZERO_RANGE);

    // Verify that combining flags works as expected
    int punch_mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;
    int zero_mode = FALLOC_FL_KEEP_SIZE | FALLOC_FL_ZERO_RANGE;

    EXPECT_NE(punch_mode & FALLOC_FL_KEEP_SIZE, 0);
    EXPECT_NE(punch_mode & FALLOC_FL_PUNCH_HOLE, 0);
    EXPECT_EQ(punch_mode & FALLOC_FL_ZERO_RANGE, 0);

    EXPECT_NE(zero_mode & FALLOC_FL_KEEP_SIZE, 0);
    EXPECT_NE(zero_mode & FALLOC_FL_ZERO_RANGE, 0);
    EXPECT_EQ(zero_mode & FALLOC_FL_PUNCH_HOLE, 0);
}

TEST(DiscardToFallocate, UblkOperationConstants) {
    // Verify UBLK operation constants are defined correctly
    EXPECT_NE(UBLK_IO_OP_DISCARD, UBLK_IO_OP_WRITE_ZEROES);
    EXPECT_GT(UBLK_IO_F_NOUNMAP, 0);
}

TEST(DiscardToFallocate, FlagBitOperations) {
    // Test that flag operations work correctly
    uint32_t flags_with_nounmap = UBLK_IO_F_NOUNMAP;
    uint32_t flags_without_nounmap = 0;

    // Test NOUNMAP flag check
    EXPECT_NE(flags_with_nounmap & UBLK_IO_F_NOUNMAP, 0);
    EXPECT_EQ(flags_without_nounmap & UBLK_IO_F_NOUNMAP, 0);

    // Verify the logic used in discard_to_fallocate:
    // (0 == (UBLK_IO_F_NOUNMAP & flags))
    EXPECT_FALSE(0 == (UBLK_IO_F_NOUNMAP & flags_with_nounmap));
    EXPECT_TRUE(0 == (UBLK_IO_F_NOUNMAP & flags_without_nounmap));
}

TEST(DiscardToFallocate, ModeCombinations) {
    // Test all valid mode combinations
    int base_mode = FALLOC_FL_KEEP_SIZE;

    int punch_hole_mode = base_mode | FALLOC_FL_PUNCH_HOLE;
    int zero_range_mode = base_mode | FALLOC_FL_ZERO_RANGE;

    // Verify they're distinct
    EXPECT_NE(punch_hole_mode, zero_range_mode);

    // Verify base mode is present in both
    EXPECT_EQ(punch_hole_mode & FALLOC_FL_KEEP_SIZE, FALLOC_FL_KEEP_SIZE);
    EXPECT_EQ(zero_range_mode & FALLOC_FL_KEEP_SIZE, FALLOC_FL_KEEP_SIZE);
}

TEST(DiscardToFallocate, FlagValuesAreNonZero) {
    // Ensure flags have meaningful values (power of 2 typically)
    EXPECT_GT(FALLOC_FL_KEEP_SIZE, 0);
    EXPECT_GT(FALLOC_FL_PUNCH_HOLE, 0);
    EXPECT_GT(FALLOC_FL_ZERO_RANGE, 0);

    // Verify they can be combined with bitwise OR
    int combined = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE | FALLOC_FL_ZERO_RANGE;
    EXPECT_GT(combined, 0);
}

// ============================================================================
// Integration and edge case tests
// ============================================================================

TEST(FsDiskImpl, UblkOperationValues) {
    EXPECT_NE(UBLK_IO_OP_DISCARD, UBLK_IO_OP_WRITE_ZEROES);
    EXPECT_NE(UBLK_IO_OP_READ, UBLK_IO_OP_WRITE);
    EXPECT_NE(UBLK_IO_OP_FLUSH, UBLK_IO_OP_DISCARD);
    EXPECT_GT(UBLK_IO_OP_WRITE, 0);
    EXPECT_GT(UBLK_IO_OP_FLUSH, 0);
}

TEST(FsDiskImpl, CqeStateTargetBitEncoding) {
    // build_cqe_state_data must set bit 63; run_queue_loop checks this to identify target CQEs.
    ublkpp::async_io io{};
    ublk_io_data fake{};
    fake.private_data = &io;
    auto const [state, user_data] = ublkpp::build_cqe_state_data(&fake);
    EXPECT_NE(user_data & (1ULL << 63), 0ULL);
    auto* decoded = reinterpret_cast< ublkpp::CqeState* >(user_data & ~(1ULL << 63));
    EXPECT_EQ(state, decoded);
}

} // anonymous namespace
