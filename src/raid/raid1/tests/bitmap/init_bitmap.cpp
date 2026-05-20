#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <isa-l/mem_routines.h>

#include "tests/test_disk.hpp"
#include "raid/raid1/bitmap.hpp"
#include "raid/raid1/raid1_superblock.hpp"
#include "raid/raid1/tests/test_raid1_common.hpp"

using ::testing::_;
#ifndef Ki
using ublkpp::Gi;
using ublkpp::Ki;
#endif

// Ensure that all required pages are initialized
TEST(Raid1, InitBitmap) {
    auto const capacity = 100 * Gi;
    auto const chunk_size = 32 * Ki;

    auto const device_params = TestParams{.capacity = capacity};
    auto const block_size = device_params.l_size;
    auto device = std::make_shared< ublkpp::TestDisk >(device_params);
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(capacity, chunk_size, block_size, superbitmap_buf.get());

    // init_to always writes k_superbitmap_bits pages regardless of disk capacity (fixed layout).
    auto const max_pages = device->max_tx() / ublkpp::raid1::Bitmap::page_size();
    auto const num_writes =
        ublkpp::raid1::k_superbitmap_bits / max_pages + ((0 == ublkpp::raid1::k_superbitmap_bits % max_pages) ? 0 : 1);

    auto total_written = 0UL;
    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(num_writes)
        .WillRepeatedly([max_tx = device->max_tx(), &total_written,
                         &max_pages](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            EXPECT_LE(nr_vecs, max_pages);
            total_written += ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
            EXPECT_LE(ublkpp::iovec_len(iovecs, iovecs + nr_vecs), max_tx);
            EXPECT_GE(addr, ublkpp::raid1::Bitmap::page_size()); // Expect write to bitmap!
            EXPECT_LE(addr, ublkpp::raid1::k_superbitmap_bits * ublkpp::raid1::Bitmap::page_size());
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::Bitmap::page_size()));
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    bitmap.init_to(device);
    EXPECT_EQ(ublkpp::raid1::k_superbitmap_bits * ublkpp::raid1::Bitmap::page_size(), total_written);
}

// After init_to, all bitmap pages must be pre-allocated in memory.
TEST(Raid1, InitBitmapPreallocatesAllPages) {
    auto const capacity = 100 * Gi;
    auto const chunk_size = 32 * Ki;

    auto const device_params = TestParams{.capacity = capacity};
    auto device = std::make_shared< ublkpp::TestDisk >(device_params);
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(capacity, chunk_size, device_params.l_size, superbitmap_buf.get());

    auto const page_width = chunk_size * ublkpp::raid1::Bitmap::page_size() * ublkpp::raid1::k_bits_in_byte;
    auto const num_pages = capacity / page_width + ((0 == capacity % page_width) ? 0 : 1);

    EXPECT_EQ(0UL, bitmap.pages_allocated());

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });

    bitmap.init_to(device);

    EXPECT_EQ(num_pages, bitmap.pages_allocated());
}

// Calling init_to twice (one per device leg) must not double-allocate pages.
TEST(Raid1, InitBitmapDoubleInitIdempotent) {
    auto const capacity = 2 * Gi;
    auto const chunk_size = 32 * Ki;

    auto const device_params = TestParams{.capacity = capacity};
    auto device_a = std::make_shared< ublkpp::TestDisk >(device_params);
    auto device_b = std::make_shared< ublkpp::TestDisk >(device_params);
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(capacity, chunk_size, device_params.l_size, superbitmap_buf.get());

    auto const page_width = chunk_size * ublkpp::raid1::Bitmap::page_size() * ublkpp::raid1::k_bits_in_byte;
    auto const num_pages = capacity / page_width + ((0 == capacity % page_width) ? 0 : 1);

    auto write_ok = [](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> ublkpp::io_result {
        return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
    };
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _)).WillRepeatedly(write_ok);
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _)).WillRepeatedly(write_ok);

    bitmap.init_to(device_a);
    EXPECT_EQ(num_pages, bitmap.pages_allocated());

    bitmap.init_to(device_b);
    // Second call must not change the allocation count — pages are reused.
    EXPECT_EQ(num_pages, bitmap.pages_allocated());
}

// Ensure that all required pages are initialized
TEST(Raid1, InitBitmapFailure) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 2 * Gi});
    auto superbitmap_buf = make_test_superbitmap();
    auto bitmap = ublkpp::raid1::Bitmap(2 * Gi, 32 * Ki, 4 * Ki, superbitmap_buf.get());

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) -> ublkpp::io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    EXPECT_THROW(bitmap.init_to(device), std::runtime_error);
}
