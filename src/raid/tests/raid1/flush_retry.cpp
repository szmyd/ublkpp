#include "test_raid1_common.hpp"

// Flush is a no-op in RAID1
TEST(Raid1, FlushRetry) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, handle_flush(_, _, _)).Times(0);
    EXPECT_CALL(*device_b, handle_flush(_, _, _)).Times(0);
    auto ublk_data = make_io_data(UBLK_IO_OP_FLUSH);
    auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, sub_cmd);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(0, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
