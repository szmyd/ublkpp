#include "test_raid1_common.hpp"

// SB v2 is the minimum supported version. Any existing disk with version < 2 uses an
// incompatible on-disk layout (capacity-proportional reserved region vs the fixed
// k_superbitmap_bits layout of v2) and must be re-created.
TEST(Raid1, V1SuperblockRejected) {
    // Construct a valid SB with the right magic and UUID but version=1.
    auto v1_superblock = normal_superblock;
    v1_superblock.header.version = htobe16(1);

    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    // device_a presents a v1 SB — MirrorDevice throws before device_b is even touched.
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([&v1_superblock](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            memcpy(iovecs->iov_base, &v1_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });

    EXPECT_THROW(ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}

// A completely new array (both devices zero) must stamp SB_VERSION=2 on every write.
TEST(Raid1, NewArrayWritesV2Superblock) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    auto zero_read = [](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
        memset(iovecs->iov_base, 0, iovecs->iov_len);
        return ublkpp::raid1::k_page_size;
    };
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(1).WillOnce(zero_read);
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _)).Times(1).WillOnce(zero_read);

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(testing::AtLeast(1))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(testing::AtLeast(1))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });

    // Both SB writes at addr=0 must carry version=2.
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, 0))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            auto const* sb = static_cast< ublkpp::raid1::SuperBlock const* >(iovecs->iov_base);
            EXPECT_EQ(htobe16(2), sb->header.version);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, 0))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result {
            auto const* sb = static_cast< ublkpp::raid1::SuperBlock const* >(iovecs->iov_base);
            EXPECT_EQ(htobe16(2), sb->header.version);
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
