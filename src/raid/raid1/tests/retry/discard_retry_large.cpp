#include "test_raid1_common.hpp"

// Dirty an entire multi-page bitmap from a single discard operation
TEST(Raid1, LargeDiscardRetry) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = 2 * Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = 2 * Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    EXPECT_TO_WRITE_SB(device_a);
    ublkpp::sub_cmd_t working_sub;
    EXPECT_CALL(*device_a, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce(
            [&working_sub](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t sub_cmd, uint32_t, uint64_t) {
                working_sub = sub_cmd;
                return 1;
            });
    EXPECT_CALL(*device_b, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, uint32_t, uint64_t) {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    auto ublk_data = make_io_data(UBLK_IO_OP_DISCARD, 2 * Gi, 0UL);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, 0b10);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());
    raid_device.on_io_complete(&ublk_data, working_sub, 0);
    remove_io_data(ublk_data);

    EXPECT_TO_WRITE_SB(device_a);
    // With batching, 2 consecutive bitmap pages are written in one call
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([&raid_device](uint8_t, iovec*, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(2U, nr_vecs);                       // 2 consecutive pages batched
            EXPECT_GE(addr, ublkpp::raid1::k_page_size);  // Expect write to bitmap!
            EXPECT_LT(addr, raid_device.reserved_size()); // Expect write to bitmap!
            return nr_vecs * ublkpp::raid1::k_page_size;  // Return total bytes written
        })
        .RetiresOnSaturation();
}
