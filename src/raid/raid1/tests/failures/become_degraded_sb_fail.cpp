#include "test_raid1_common.hpp"

// Test superblock write failure during degradation triggers rollback
// This tests the __become_degraded rollback path (raid1.cpp:500-501) where:
// 1. Array starts healthy (both devices available)
// 2. Write to device_a fails immediately → triggers __become_degraded
// 3. Superblock write to device_b (the new active device) FAILS
// 4. Expected: Rollback the route CAS and return error without degrading
TEST(Raid1, BecomeDegradedSuperblockFails) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    {
        // First write to device_a fails immediately
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        // Superblock write to device_b (during degradation) fails
        EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
            .Times(1)
            .WillOnce([](uint8_t, iovec* iovecs, uint32_t, off_t addr) -> io_result {
                EXPECT_EQ(addr, 0UL); // Superblock write at offset 0
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                // Fail the superblock write
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        // Backup write attempt to device_b should NOT happen because superblock write failed
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);

        // Should fail because we couldn't degrade (superblock write failed, route rolled back)
        ASSERT_FALSE(res);
    }

    // Array should still be healthy (not degraded) because rollback succeeded
    auto state = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, state.device_a);
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, state.device_b);

    // expect unmount_clean on both devices
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
