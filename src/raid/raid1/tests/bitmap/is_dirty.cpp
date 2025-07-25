#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <isa-l/mem_routines.h>

#include "tests/test_disk.hpp"
#include "raid/raid1/bitmap.hpp"

using ::testing::_;

// Ensure that all required pages are initialized
TEST(Raid1, DoesNotCrossPage) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 2 * ublkpp::Gi});
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    {
        auto [pg, pg_off, sz] = bitmap.dirty_page(ublkpp::Gi - (4 * ublkpp::Ki), 12 * ublkpp::Ki);
        EXPECT_EQ(0U, pg_off);
        EXPECT_EQ(4 * ublkpp::Ki, sz);
    }
    EXPECT_FALSE(bitmap.is_dirty(ublkpp::Gi + (4 * ublkpp::Ki), 4 * ublkpp::Ki));
}

TEST(Raid1, IsDirtyNextPage) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 2 * ublkpp::Gi});
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    {
        auto [pg, pg_off, sz] = bitmap.dirty_page(ublkpp::Gi + (4 * ublkpp::Ki), 12 * ublkpp::Ki);
        EXPECT_EQ(1U, pg_off);
        EXPECT_EQ(12 * ublkpp::Ki, sz);
    }
    EXPECT_TRUE(bitmap.is_dirty(ublkpp::Gi - (4 * ublkpp::Ki), 8 * ublkpp::Ki));
}
