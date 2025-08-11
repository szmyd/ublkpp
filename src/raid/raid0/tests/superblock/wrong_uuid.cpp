#include "test_raid0_common.hpp"

// If we initialize with one clean device and one with the wrong uuid in the superblock
TEST(Raid0, WrongUUIDA) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid0::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid0::k_page_size);
            auto superblock = reinterpret_cast< ublkpp::raid0::SuperBlock* >(iovecs->iov_base);
            superblock->header.uuid[0] = 0xbb;
            return ublkpp::raid0::k_page_size;
        });

    EXPECT_THROW(ublkpp::Raid0Disk(boost::uuids::string_generator()(test_uuid), 128 * Ki,
                                   std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b}),
                 std::runtime_error);
}

TEST(Raid0, WrongUUIDB) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid0::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid0::k_page_size);

            return ublkpp::raid0::k_page_size;
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid0::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid0::k_page_size);
            auto superblock = reinterpret_cast< ublkpp::raid0::SuperBlock* >(iovecs->iov_base);
            superblock->header.uuid[0] = 0xbb;
            return ublkpp::raid0::k_page_size;
        });

    EXPECT_THROW(ublkpp::Raid0Disk(boost::uuids::string_generator()(test_uuid), 128 * Ki,
                                   std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b}),
                 std::runtime_error);
}
