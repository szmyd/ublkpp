#include "test_raid1_common.hpp"

// After a read failure marks a device unavail, a successful write to that device should
// clear the unavail flag without requiring the queue to go idle.
TEST(Raid1, SyncIoWriteClearsUnavail) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::raid1::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    auto const test_off = 8 * Ki;
    auto const test_sz = 80 * Ki;

    // Step 1: read fails on A, succeeds on B -- marks A unavail.
    EXPECT_SYNC_OP(UBLK_IO_OP_READ, device_a, false, true, test_sz, test_off + raid_device.reserved_size());
    EXPECT_SYNC_OP(UBLK_IO_OP_READ, device_b, true, false, test_sz, test_off + raid_device.reserved_size());

    RUN_IN_THREAD({
        iovec iov{nullptr, test_sz};
        auto res = raid_device.sync_iov(UBLK_IO_OP_READ, &iov, 1, test_off);
        ASSERT_TRUE(res);
        EXPECT_EQ(test_sz, static_cast< size_t >(res.value()));
    });

    ASSERT_EQ(ublkpp::raid1::replica_state::UNAVAIL, raid_device.replica_states().device_a);

    // Step 2: write succeeds on both -- A's unavail flag must be cleared.
    EXPECT_SYNC_OP(UBLK_IO_OP_WRITE, device_a, false, false, test_sz, test_off + raid_device.reserved_size());
    EXPECT_SYNC_OP(UBLK_IO_OP_WRITE, device_b, false, false, test_sz, test_off + raid_device.reserved_size());

    iovec iov{nullptr, test_sz};
    auto res = raid_device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, test_off);
    ASSERT_TRUE(res);

    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, raid_device.replica_states().device_a);

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
