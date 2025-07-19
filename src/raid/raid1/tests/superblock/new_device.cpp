#include "test_raid1_common.hpp"

#include <isa-l/mem_routines.h>

// If we initialize with one clean device and one new one; we should enter degraded mode immediately
TEST(Raid1, NewDeviceB) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            memcpy(iovecs->iov_base, raid1_header, sizeof(ublkpp::raid1::SuperBlock::header));
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);                                  // Expect write to bitmap!
            EXPECT_LT(addr, ublkpp::raid1::reserved_size);                                // Expect write to bitmap!
            EXPECT_NE(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All ones
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0, memcmp(raid1_header, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            auto header = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA,
                      static_cast< ublkpp::raid1::read_route >(header->fields.read_route));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);                                  // Expect write to bitmap!
            EXPECT_LT(addr, ublkpp::raid1::reserved_size);                                // Expect write to bitmap!
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All zeros
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            EXPECT_EQ(0, memcmp(raid1_header, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    EXPECT_TO_WRITE_SB(device_a);
}

TEST(Raid1, NewDeviceA) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            memcpy(iovecs->iov_base, raid1_header, sizeof(ublkpp::raid1::SuperBlock::header));
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);                                  // Expect write to bitmap!
            EXPECT_LT(addr, ublkpp::raid1::reserved_size);                                // Expect write to bitmap!
            EXPECT_NE(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All ones
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0, memcmp(raid1_header, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            auto header = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVB,
                      static_cast< ublkpp::raid1::read_route >(header->fields.read_route));
            EXPECT_EQ(0UL, addr);
            return ublkpp::raid1::k_page_size;
        });

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);                                  // Expect write to bitmap!
            EXPECT_LT(addr, ublkpp::raid1::reserved_size);                                // Expect write to bitmap!
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All zeros
            return ublkpp::raid1::k_page_size;
        })
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            EXPECT_EQ(0, memcmp(raid1_header, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    EXPECT_TO_WRITE_SB(device_b);
}
