#include "test_raid1_common.hpp"

using ::testing::_;

// Ensure that all required pages are read
TEST(Raid1, MismatchedBitmapUUIDs) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = ublkpp::Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = ublkpp::Gi});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr); // Expect read SuperBlock!
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto superblock = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            superblock->fields.bitmap.uuid[0] = 0xff; // Corrupt BITMAP uuid
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> ublkpp::io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr); // Expect read SuperBlock!
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });

    EXPECT_THROW(ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}
