#include "test_raid1_common.hpp"

// Brief: Test that we open the underlying devices correctly, and return them to our upper layer.
//
// When a UblkDisk receives a call to `open_for_uring`, it's expected to return a std::set of all
// fds that were opened by the underlying Devices in order to register them with io_uring. Test
// that RAID1 is collecting these FDs and passing the io_uring offset to the lower layers.
TEST(Raid1, OpenDevices) {
    static const auto start_idx = 2;
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    // Each device should be subsequently opened and return a set with their sole FD.
    EXPECT_CALL(*device_a, open_for_uring(_)).Times(1).WillOnce([](int const fd_off) {
        EXPECT_EQ(start_idx, fd_off);
        // Return 2 FDs here, maybe it's another RAID1 device?
        return std::list< int >{INT_MAX - 2, INT_MAX - 3};
    });
    EXPECT_CALL(*device_b, open_for_uring(_)).Times(1).WillOnce([](int const fd_off) {
        // Device A took 2 uring offsets
        EXPECT_EQ(start_idx + 2, fd_off);
        return std::list< int >{INT_MAX - 1};
    });

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    auto fd_list = raid_device.open_for_uring(2);
    EXPECT_EQ(3, fd_list.size());
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 3)));
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 2)));
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 1)));

    EXPECT_CALL(*device_a, collect_async(_, _)).Times(0);
    EXPECT_CALL(*device_b, collect_async(_, _)).Times(0);
    std::list< ublkpp::async_result > result_list;
    raid_device.collect_async(nullptr, result_list);
    EXPECT_EQ(0, result_list.size());

    device_a->uses_ublk_iouring = false;
    device_b->uses_ublk_iouring = false;

    EXPECT_CALL(*device_a, collect_async(_, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, std::list< ublkpp::async_result >& compls) {
            compls.push_back(ublkpp::async_result{nullptr, 0, 5});
        });
    EXPECT_CALL(*device_b, collect_async(_, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, std::list< ublkpp::async_result >& compls) {
            compls.push_back(ublkpp::async_result{nullptr, 1, 10});
        });

    raid_device.collect_async(nullptr, result_list);

    ASSERT_EQ(2, result_list.size());
    EXPECT_EQ(0, result_list.begin()->sub_cmd);
    EXPECT_EQ(5, result_list.begin()->result);
    EXPECT_EQ(1, (++result_list.begin())->sub_cmd);
    EXPECT_EQ(10, (++result_list.begin())->result);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
