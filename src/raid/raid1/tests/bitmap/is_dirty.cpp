#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "raid/raid1/bitmap.hpp"

using ublkpp::Ki;

// Ensure that all required pages are initialized
TEST(Raid1, DoesNotCrossPage) {
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * Ki, 4 * Ki);
    EXPECT_EQ(4 * Ki, bitmap.dirty_page(ublkpp::Gi - (4 * Ki), 12 * Ki));
    EXPECT_FALSE(bitmap.is_dirty(ublkpp::Gi + (4 * Ki), 4 * Ki));
}

TEST(Raid1, IsDirtyNextPage) {
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * Ki, 4 * Ki);
    EXPECT_EQ(12 * Ki, bitmap.dirty_page(ublkpp::Gi + (4 * Ki), 12 * Ki));
    EXPECT_TRUE(bitmap.is_dirty(ublkpp::Gi - (4 * Ki), 8 * Ki));
}

// Some actual problematic Bitmap bugs encountered when testing BufferedI/O
TEST(Raid1, BufferedIoTest) {
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * Ki, 4 * Ki);
    EXPECT_EQ(512 * Ki, bitmap.dirty_page(0xf7b000, 512 * Ki));
    EXPECT_EQ(512 * Ki, bitmap.dirty_page(0xffb000, 512 * Ki));
    EXPECT_EQ(512 * Ki, bitmap.dirty_page(0x22ac000, 512 * Ki));
    EXPECT_EQ(312 * Ki, bitmap.dirty_page(0x232c000, 312 * Ki));
    EXPECT_EQ(512 * Ki, bitmap.dirty_page(0x237a000, 512 * Ki));
    EXPECT_EQ(512 * Ki, bitmap.dirty_page(0x23fa000, 512 * Ki));
    EXPECT_TRUE(bitmap.is_dirty(0x2448000, 44 * Ki));
}
