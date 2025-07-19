#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <isa-l/mem_routines.h>

#include "tests/test_disk.hpp"
#include "raid/raid1/bitmap.hpp"

using ::testing::_;

// Ensure that all required pages are initialized
TEST(Raid1, InitBitmap) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 2 * ublkpp::Gi});
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);   // Expect write to bitmap!
            EXPECT_LT(addr, ublkpp::raid1::reserved_size); // Expect write to bitmap!
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size));
            return ublkpp::raid1::k_page_size;
        });

    bitmap.init_to(*device);
}

// Ensure that all required pages are initialized
TEST(Raid1, InitBitmapFailure) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 2 * ublkpp::Gi});
    auto bitmap = ublkpp::raid1::Bitmap(2 * ublkpp::Gi, 32 * ublkpp::Ki, 4 * ublkpp::Ki);

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) -> ublkpp::io_result {
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        });

    EXPECT_THROW(bitmap.init_to(*device), std::runtime_error);
}
