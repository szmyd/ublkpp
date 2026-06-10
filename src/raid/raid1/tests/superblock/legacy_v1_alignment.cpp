#include "test_raid1_common.hpp"

// Brief: legacy (v1) on-disk superblocks must keep their original max_sectors-aligned
// _reserved_size formula, so existing production arrays never see their data shift.
//
// New (v2) arrays use block-aligned _reserved_size; v1 arrays must continue to use the
// old max_sectors_bytes-aligned formula. This is verified indirectly: under v1, the
// effective capacity reported by Raid1Disk must be a multiple of (max_sectors << 9) - the
// formula's defining invariant.
TEST(Raid1, LegacyV1KeepsMaxSectorsAlignment) {
    constexpr uint64_t k_capacity = Gi;
    auto device_a = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = k_capacity});
    auto device_b = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = k_capacity});

    // Stage a v1 SB on each leg so RAID1 takes the legacy alignment branch.
    auto stage_v1 = [](auto* dev, bool side_b) {
        EXPECT_CALL(*dev, sync_iov(UBLK_IO_OP_READ, _, _, _))
            .Times(1)
            .WillOnce([side_b](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {
                EXPECT_EQ(1U, nr_vecs);
                EXPECT_EQ(ublkpp::raid1::k_page_size, ublkpp::iovec_len(iovecs, iovecs + nr_vecs));
                EXPECT_EQ(0UL, addr);
                memcpy(iovecs->iov_base, &legacy_v1_superblock, ublkpp::raid1::k_page_size);
                if (side_b) static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b = 1;
                return ublkpp::raid1::k_page_size;
            });
        // Allow any number of subsequent writes (RAID1 may rewrite the SB at __become_active
        // and again on shutdown). We don't care about their content, only that the capacity
        // calculation honors v1 alignment.
        EXPECT_CALL(*dev, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .Times(::testing::AnyNumber())
            .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t, off_t) -> io_result { return iovecs->iov_len; });
    };
    stage_v1(device_a.get(), false);
    stage_v1(device_b.get(), true);

    auto uuid = boost::uuids::string_generator{}(test_uuid);
    auto raid = ublkpp::make_raid1_disk(uuid, device_a, device_b);
    ASSERT_NE(raid, nullptr);

    // V1 invariant: capacity (= dev_sectors << 9 after _reserved_size subtraction) must be
    // a multiple of max_sectors_bytes. The default max_io is 512 KiB (TestParams default).
    constexpr uint64_t k_max_sectors_bytes = 512 * Ki;
    EXPECT_EQ(0u, raid->capacity() % k_max_sectors_bytes)
        << "v1 capacity " << raid->capacity() << " is not a multiple of max_sectors_bytes " << k_max_sectors_bytes
        << " - legacy alignment regressed and existing on-disk arrays would shift";
}
