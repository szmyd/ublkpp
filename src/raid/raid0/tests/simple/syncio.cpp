#include "test_raid0_common.hpp"

TEST(Raid0, SyncIoSuccess) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    constexpr uint32_t stripe_size = 32 * Ki;
    auto raid_device =
        ublkpp::make_raid0_disk(boost::uuids::random_generator()(), stripe_size,
                                std::vector< std::shared_ptr< ublk_disk > >{device_a, device_b, device_c});

    auto test_op = UBLK_IO_OP_WRITE;
    auto test_off = 8 * Ki;
    auto test_sz = 64 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, false, 24 * Ki, test_off + stripe_size);
    EXPECT_SYNC_OP(test_op, device_b, false, 32 * Ki, stripe_size);
    EXPECT_SYNC_OP(test_op, device_c, false, 8 * Ki, stripe_size);

    auto iov = iovec{.iov_base = nullptr, .iov_len = test_sz};
    auto res = raid_device->sync_iov(test_op, &iov, 1, test_off);
    ASSERT_TRUE(res);
    EXPECT_EQ(test_sz, res.value());
}
