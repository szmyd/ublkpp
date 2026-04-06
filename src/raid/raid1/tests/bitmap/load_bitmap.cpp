#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "tests/test_disk.hpp"
#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/super_bitmap.hpp"
#include "raid/raid1/raid1_superblock.hpp"
#include "raid/raid1/tests/test_raid1_common.hpp"

using ::testing::_;

// Ensure that all required pages are read
TEST(Raid1, LoadBitmap) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 3 * ublkpp::Gi});
    auto superbitmap_buf = make_test_superbitmap();

    // Mark all 3 pages as dirty in the SuperBitmap (simulating what sync_to would do)
    auto sb = ublkpp::raid1::SuperBitmap(superbitmap_buf.get());
    sb.set_bit(0);
    sb.set_bit(1);
    sb.set_bit(2);

    auto bitmap = ublkpp::raid1::Bitmap(3 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki, superbitmap_buf.get());

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(3)
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::Bitmap::page_size(), ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::Bitmap::page_size());     // Expect write to bitmap!
            EXPECT_LE(addr, 3 * ublkpp::raid1::Bitmap::page_size()); // Expect write to bitmap!
            memset(iovecs->iov_base, 0xff, iovecs->iov_len);
            return ublkpp::raid1::Bitmap::page_size();
        });

    bitmap.load_from(*device);
}

// Simulate a crash-recovery scenario: superbitmap bit is set (on-disk superblock not updated after
// clean_region zeroed the page), but the bitmap page on disk is zero. load_from must clear the
// stale bit so the invariant holds and dirty_pages()/next_dirty() don't see a null-page slot.
TEST(Raid1, LoadBitmapStaleSuperBitmapBit) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 2 * ublkpp::Gi});
    auto superbitmap_buf = make_test_superbitmap();

    // Simulate on-disk superblock state after crash: bit 0 is dirty, bit 1 is clean
    auto sb = ublkpp::raid1::SuperBitmap(superbitmap_buf.get());
    sb.set_bit(0);
    sb.set_bit(1);

    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki, superbitmap_buf.get());

    // Page 0: zero on disk (was cleaned before crash — stale superbitmap bit)
    // Page 1: non-zero on disk (genuinely dirty)
    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> ublkpp::io_result {
            memset(iovecs->iov_base, 0x00, iovecs->iov_len); // stale: zero page
            return ublkpp::raid1::Bitmap::page_size();
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> ublkpp::io_result {
            memset(iovecs->iov_base, 0xff, iovecs->iov_len); // dirty page
            return ublkpp::raid1::Bitmap::page_size();
        });

    bitmap.load_from(*device);

    // Stale bit for page 0 must be cleared; only page 1 counts as dirty
    EXPECT_FALSE(sb.test_bit(0));
    EXPECT_TRUE(sb.test_bit(1));
    EXPECT_EQ(1UL, bitmap.dirty_pages());
}

// Ensure that all required pages are read
TEST(Raid1, LoadBitmapFailure) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 2 * ublkpp::Gi});
    auto superbitmap_buf = make_test_superbitmap();

    // Mark both pages as dirty in the SuperBitmap
    auto sb = ublkpp::raid1::SuperBitmap(superbitmap_buf.get());
    sb.set_bit(0);
    sb.set_bit(1);

    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki, superbitmap_buf.get());

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::Bitmap::page_size(), ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::Bitmap::page_size());     // Expect write to bitmap!
            EXPECT_LT(addr, 2 * ublkpp::raid1::Bitmap::page_size()); // Expect write to bitmap!
            memset(iovecs->iov_base, 0xff, iovecs->iov_len);
            return ublkpp::raid1::Bitmap::page_size();
        })
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) -> ublkpp::io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    EXPECT_THROW(bitmap.load_from(*device), std::runtime_error);
}
