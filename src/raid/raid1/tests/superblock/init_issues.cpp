#include "test_raid1_common.hpp"

// Brief: If either devices should not load/write superblocks correctly, initialization should throw
TEST(Raid1, ReadingSBProblems) {
    // Fail Read SB from DevA
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, true, false, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, true, true, false, false);
        EXPECT_THROW(auto raid_device =
                         ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                     std::runtime_error);
    }
    // Fail Read SB from DevB
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, false, true, true, false);
        EXPECT_THROW(auto raid_device =
                         ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                     std::runtime_error);
    }
    // Fail Read SB from Both
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, true, true, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, true, true, true, false);
        EXPECT_THROW(auto raid_device =
                         ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                     std::runtime_error);
    }
    // Should not throw just dirty SB and pages
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, false, true);
        auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
        auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
        // expect unmount_clean update
        EXPECT_TO_WRITE_SB(device_b);
    }

    // Should not throw just dirty SB
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, false, false, false, true);
        // Expect an extra WRITE to the SB when sync'ing the SB to DevB fails
        EXPECT_SYNC_OP_REPEAT(UBLK_IO_OP_WRITE, 2, device_a, false, false, ublkpp::raid1::k_page_size, 0UL);
        auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
        // expect unmount_clean update
        EXPECT_TO_WRITE_SB(device_a);
    }

    // Fail writing both SBs
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, false, true);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, false, false, false, true);
        EXPECT_THROW(auto raid_device =
                         ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                     std::runtime_error);
    }

    // Fail Second Update to DevA
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, false, false, false, true);
        // Expect an extra WRITE to the SB when sync'ing the SB to DevB fails
        EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .Times(2)
            .WillOnce([](uint8_t, iovec* iov, uint32_t nr_vecs, off_t addr) -> io_result {
                EXPECT_EQ(1, nr_vecs);
                EXPECT_EQ(ublkpp::raid1::k_page_size, iov->iov_len);
                EXPECT_EQ(0UL, addr);
                return iov->iov_len;
            })
            .WillOnce([](uint8_t, iovec* iov, uint32_t nr_vecs, off_t addr) -> io_result {
                EXPECT_EQ(1, nr_vecs);
                EXPECT_EQ(ublkpp::raid1::k_page_size, iov->iov_len);
                EXPECT_EQ(0UL, addr);
                return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
            });
        EXPECT_THROW(auto raid_device =
                         ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b),
                     std::runtime_error);
    }
}
