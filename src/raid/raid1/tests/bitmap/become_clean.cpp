#include "test_raid1_common.hpp"

#include <isa-l/mem_routines.h>

using namespace std::chrono_literals;

// Test the correct clearing of the bitmap
TEST(Raid1, CleanBitmap) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    auto cur_replica_state = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, cur_replica_state.device_a);
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, cur_replica_state.device_b);
    EXPECT_EQ(0, cur_replica_state.bytes_to_sync);

    {
        ublkpp::sub_cmd_t working_sub;
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&working_sub](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, iovec*,
                                     uint32_t, uint64_t) {
                working_sub = sub_cmd;
                return 1;
            });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value()); // Bitmap dirty deferred
        raid_device.on_io_complete(&ublk_data, working_sub);
        remove_io_data(ublk_data);
    }

    cur_replica_state = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, cur_replica_state.device_a);
    EXPECT_EQ(ublkpp::raid1::replica_state::ERROR, cur_replica_state.device_b);
    EXPECT_EQ(32 * Ki, cur_replica_state.bytes_to_sync);

    {
        // Make Device B avail again
        auto ublk_data = make_io_data(UBLK_IO_OP_READ);
        auto res = raid_device.queue_internal_resp(nullptr, &ublk_data, 0b101, 0);
        ASSERT_TRUE(res);
        EXPECT_EQ(0, res.value());
    }

    {
        // Subsequent writes that encompass dirty regions should go to the degraded device and clean dirty new pages
        // if it works
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE, 32 * Ki, 0UL);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&raid_device](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd,
                                     iovec* iovecs, uint32_t, uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
                EXPECT_FALSE(ublkpp::is_replicate(sub_cmd));
                EXPECT_EQ(iovecs->iov_len, 32 * Ki);
                EXPECT_EQ(addr, raid_device.reserved_size());
                return 1;
            });
        ublkpp::sub_cmd_t internal_sub_cmd;
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([&internal_sub_cmd, &raid_device](ublksrv_queue const*, ublk_io_data const*,
                                                        ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t,
                                                        uint64_t addr) {
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                EXPECT_TRUE(ublkpp::is_replicate(sub_cmd));
                EXPECT_TRUE(ublkpp::is_internal(sub_cmd));
                internal_sub_cmd = sub_cmd;
                EXPECT_EQ(iovecs->iov_len, 32 * Ki);
                EXPECT_EQ(addr, raid_device.reserved_size());
                return 1;
            });
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 32 * Ki, 0UL);
        ASSERT_TRUE(res);
        EXPECT_EQ(2, res.value());

        // expect clean SB written on both devices and a bitmap clearing
        EXPECT_CALL(*device_a, sync_iov(_, _, _, _))
            .Times(2)
            .WillOnce([&raid_device](uint8_t, iovec* iovecs, uint32_t, off_t addr) {
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_GE(addr, ublkpp::raid1::k_page_size);  // Expect write to bitmap!
                EXPECT_LT(addr, raid_device.reserved_size()); // Expect write to bitmap!
                EXPECT_EQ(0, isal_zero_detect(iovecs->iov_base, ublkpp::raid1::k_page_size));
                return 1;
            })
            .WillOnce([&raid_device](uint8_t, iovec* iovecs, uint32_t, off_t addr) {
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_EQ(addr, 0UL); // Expect write to SuperBlock!
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });
        EXPECT_TO_WRITE_SB(device_b);

        raid_device.on_io_complete(&ublk_data, 0b100);
        raid_device.queue_internal_resp(nullptr, &ublk_data, internal_sub_cmd, 0UL);
        remove_io_data(ublk_data);
        raid_device.toggle_resync(true);
        std::this_thread::sleep_for(3ms);
    }
    cur_replica_state = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, cur_replica_state.device_a);
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, cur_replica_state.device_b);
    EXPECT_EQ(0, cur_replica_state.bytes_to_sync);

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
