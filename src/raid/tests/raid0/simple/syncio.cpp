#include "test_raid0_common.hpp"

TEST(Raid0, SyncIoSuccess) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});

    auto test_op = UBLK_IO_OP_WRITE;
    auto test_off = 8 * Ki;
    auto test_sz = 64 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, false, 24 * Ki, test_off + raid_device.stripe_size());
    EXPECT_SYNC_OP(test_op, device_b, false, 32 * Ki, raid_device.stripe_size());
    EXPECT_SYNC_OP(test_op, device_c, false, 8 * Ki, raid_device.stripe_size());

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    EXPECT_EQ(test_sz, res.value());
}
