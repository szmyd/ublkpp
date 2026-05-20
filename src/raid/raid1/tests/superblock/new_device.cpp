#include "test_raid1_common.hpp"

#include <isa-l/mem_routines.h>

// If we initialize with one clean device and one new one; we should enter degraded mode immediately
TEST(Raid1, NewDeviceB) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 100 * Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = 100 * Gi});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    // __become_active writes the SB to device_a (active, read_route=DEVA)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, 0))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0, memcmp(&normal_superblock, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            auto superblock = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA,
                      static_cast< ublkpp::raid1::read_route >(superblock->fields.read_route));
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    // __become_active writes the SB backup copy to device_b
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, 0))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0, memcmp(&normal_superblock, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            return ublkpp::raid1::k_page_size;
        });
    // init_to writes _num_pages zero pages to device_b (capacity-derived, batched)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(testing::AtLeast(1))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            for (uint32_t i = 0; i < nr_vecs; ++i)
                EXPECT_EQ(0, isal_zero_detect(iovecs[i].iov_base, iovecs[i].iov_len)); // All zeros
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    EXPECT_TO_WRITE_SB(device_a);
    // Destructor sync_to writes all-ones dirty bitmap to device_a (100 GiB = 100 pages, batched nr_vecs=100)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            for (uint32_t i = 0; i < nr_vecs; ++i)
                EXPECT_NE(0, isal_zero_detect(iovecs[i].iov_base, iovecs[i].iov_len)); // All ones
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
}

TEST(Raid1, NewDeviceA) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            auto superblock = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            superblock->fields.device_b = 1;
            return ublkpp::raid1::k_page_size;
        });
    // __become_active writes SB to device_b (active, read_route=DEVB, device_b=1)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, 0))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0, memcmp(&normal_superblock, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            auto superblock = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVB,
                      static_cast< ublkpp::raid1::read_route >(superblock->fields.read_route));
            EXPECT_EQ(1, superblock->fields.device_b);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    // init_to writes _num_pages zero pages to device_a (capacity-derived, batched)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(testing::AtLeast(1))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All zeros
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, 0))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            EXPECT_EQ(0, memcmp(&normal_superblock, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    EXPECT_TO_WRITE_SB(device_b);
    // Destructor sync_to writes all-ones dirty bitmap to device_b (1 GiB = 1 page, nr_vecs=1)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_NE(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All ones
            return ublkpp::raid1::k_page_size;
        });
}

TEST(Raid1, ReversedOrder) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    // __become_active writes SB to device_b (active, read_route=DEVA, device_b=0 — was slot-A disk)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, 0))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0, memcmp(&normal_superblock, iovecs->iov_base, sizeof(ublkpp::raid1::SuperBlock::header)));
            auto superblock = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(ublkpp::raid1::read_route::DEVA,
                      static_cast< ublkpp::raid1::read_route >(superblock->fields.read_route));
            EXPECT_EQ(0, superblock->fields.device_b);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    // init_to writes _num_pages zero pages to device_a (capacity-derived, batched)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(testing::AtLeast(1))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All zeros
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, 0))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            auto superblock = reinterpret_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base);
            EXPECT_EQ(1U, superblock->fields.device_b);
            return ublkpp::raid1::k_page_size;
        });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    EXPECT_TO_WRITE_SB(device_b);
    // Destructor sync_to writes all-ones dirty bitmap to device_b (1 GiB = 1 page, nr_vecs=1)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_NE(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All ones
            return ublkpp::raid1::k_page_size;
        });
}

TEST(Raid1, NewDeviceThrowCantDirty) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    // init_to writes _num_pages zero pages to device_b (capacity-derived, batched)
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, testing::Gt((off_t)0)))
        .Times(testing::AtLeast(1))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);
            EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size)); // All zeros
            return ublkpp::iovec_len(iovecs, iovecs + nr_vecs);
        });
    // device_a fails before __become_active reaches device_b — no SB write expected on device_b
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, 0)).Times(0);

    EXPECT_THROW(ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}

TEST(Raid1, NewDeviceThrowCantClean) {
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = Gi});

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
            return ublkpp::raid1::k_page_size;
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
            EXPECT_EQ(0UL, addr);
            memset(iovecs->iov_base, 000, iovecs->iov_len);
            return ublkpp::raid1::k_page_size;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec*, uint32_t, off_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    EXPECT_THROW(ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                 std::runtime_error);
}
