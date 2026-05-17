#include "test_raid1_common.hpp"

// Brief: Test that we open the underlying devices correctly, and return them to our upper layer.
//
// When a ublk_disk receives a call to `prepare`, it's expected to return a std::set of all
// fds that were opened by the underlying Devices in order to register them with io_uring. Test
// that RAID1 is collecting these FDs and passing the io_uring offset to the lower layers.
TEST(Raid1, OpenDevices) {
    static const auto start_idx = 2;
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    // Each device should be subsequently opened and return a set with their sole FD.
    EXPECT_CALL(*device_a, prepare(_, _)).Times(1).WillOnce([](ublkpp::ublk_rings const*, int const fd_off) {
        EXPECT_EQ(start_idx, fd_off);
        // Return 2 FDs here, maybe it's another RAID1 device?
        return ublkpp::ublk_disk::prepare_result{.fds = {INT_MAX - 2, INT_MAX - 3}};
    });
    EXPECT_CALL(*device_b, prepare(_, _)).Times(1).WillOnce([](ublkpp::ublk_rings const*, int const fd_off) {
        // Device A took 2 uring offsets
        EXPECT_EQ(start_idx + 2, fd_off);
        return ublkpp::ublk_disk::prepare_result{.fds = {INT_MAX - 1}};
    });

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    auto result = raid_device.prepare(nullptr, 2);
    EXPECT_EQ(3, result.fds.size());
    EXPECT_NE(result.fds.end(), std::find(result.fds.begin(), result.fds.end(), (INT_MAX - 3)));
    EXPECT_NE(result.fds.end(), std::find(result.fds.begin(), result.fds.end(), (INT_MAX - 2)));
    EXPECT_NE(result.fds.end(), std::find(result.fds.begin(), result.fds.end(), (INT_MAX - 1)));

    // expect unmount_clean update on both devices (healthy array, no degradation)
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
