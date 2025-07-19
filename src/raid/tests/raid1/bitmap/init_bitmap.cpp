#include "test_raid1_common.hpp"

#include <isa-l/mem_routines.h>

#include "raid/bitmap.hpp"

// Ensure that all required pages are initialized
TEST(Raid1, InitBitmap) {
    auto device = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 2 * Gi});
    auto bitmap = ublkpp::raid1::Bitmap(2 * Gi, 32 * Ki, 4 * Ki);

    EXPECT_CALL(*device, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size); // Expect write to bitmap!
            EXPECT_LT(addr, reserved_size);              // Expect write to bitmap!
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size));
            return ublkpp::raid1::k_page_size;
        });

    bitmap.init_to(*device);
}

