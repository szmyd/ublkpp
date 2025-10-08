#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "tests/test_disk.hpp"
#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_superblock.hpp"

using ::testing::_;

// Ensure that all required pages are read
TEST(Raid1, LoadBitmap) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 3 * ublkpp::Gi});
    auto bitmap = ublkpp::raid1::Bitmap(3 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(3)
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::Bitmap::page_size(), ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::Bitmap::page_size()); // Expect write to bitmap!
            EXPECT_LT(addr, ublkpp::raid1::reserved_size);       // Expect write to bitmap!
            memset(iovecs->iov_base, 0xff, iovecs->iov_len);
            return ublkpp::raid1::Bitmap::page_size();
        });

    bitmap.load_from(*device);
}

// Ensure that all required pages are read
TEST(Raid1, LoadBitmapFailure) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 2 * ublkpp::Gi});
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::Bitmap::page_size(), ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::Bitmap::page_size()); // Expect write to bitmap!
            EXPECT_LT(addr, ublkpp::raid1::reserved_size);       // Expect write to bitmap!
            memset(iovecs->iov_base, 0xff, iovecs->iov_len);
            return ublkpp::raid1::Bitmap::page_size();
        })
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) -> ublkpp::io_result {
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        });

    EXPECT_THROW(bitmap.load_from(*device), std::runtime_error);
}
