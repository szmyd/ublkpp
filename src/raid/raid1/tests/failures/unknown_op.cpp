#include "test_raid1_common.hpp"

TEST(Raid1, UnknownOp) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto ublk_data = make_io_data(0xFF, 4 * Ki, 8 * Ki);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, 0b10);
    remove_io_data(ublk_data);
    EXPECT_FALSE(res);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
