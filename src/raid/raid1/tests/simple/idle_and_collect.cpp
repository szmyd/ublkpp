#include "../test_raid1_common.hpp"

// probe_tick on a healthy array — probes both devices unconditionally.
TEST(Raid1, ProbeTickHealthyArray) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto const rs = raid_device.reserved_size();
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, rs)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return ublkpp::raid1::k_page_size;
    });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, rs)).Times(1).WillOnce([](uint8_t, iovec*, uint32_t, off_t) {
        return ublkpp::raid1::k_page_size;
    });

    raid_device.probe_tick(nullptr);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// probe_tick called twice — idempotent, no crash.
TEST(Raid1, ProbeTickIdempotent) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto const rs = raid_device.reserved_size();
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, rs))
        .Times(2)
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) { return ublkpp::raid1::k_page_size; });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, rs))
        .Times(2)
        .WillRepeatedly([](uint8_t, iovec*, uint32_t, off_t) { return ublkpp::raid1::k_page_size; });

    raid_device.probe_tick(nullptr);
    raid_device.probe_tick(nullptr);

    // Expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
