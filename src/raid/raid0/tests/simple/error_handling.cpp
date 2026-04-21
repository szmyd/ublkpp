#include "../test_raid0_common.hpp"

// Regression test for stale alive_cmds: a failed I/O spanning multiple strides must not leave
// non-zero alive_cmds entries behind, which would corrupt the next I/O on the same thread.
//
// Setup: 3 devices, stripe_size=4KiB, stride_width=12KiB.
// I/O 1: 24KiB (2 full strides). Stride 1 accumulates all 3 stripes but dispatches none.
// In stride 2, device A is dispatched first (2 accumulated iovecs) and fails → early return.
// alive_cmds[B]=1 and alive_cmds[C]=1 are left non-zero.
//
// I/O 2: 8KiB (stripe A + stripe B). Without the fix, device B receives nr_vecs=2 with a stale
// iov_base[0] pointing into I/O 1's buffer. With the fix, nr_vecs=1 and iov_base is correct.
TEST(Raid0, FailedMultiStrideIoDoesNotLeaveStaleAliveCount) {
    constexpr uint32_t stripe_sz = 4 * Ki;
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid = ublkpp::Raid0Disk(boost::uuids::string_generator()(test_uuid), stripe_sz,
                                  std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});

    // I/O 1: 2 full strides — device A dispatched first in last stride with 2 accumulated iovecs, fails.
    constexpr uint32_t io1_sz = 2 * 3 * stripe_sz; // 24KiB
    void* buf1{};
    ASSERT_EQ(0, posix_memalign(&buf1, device_a->block_size(), io1_sz));
    auto iov1 = iovec{.iov_base = buf1, .iov_len = io1_sz};

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec*, uint32_t nr_vecs, off_t) -> io_result {
            EXPECT_EQ(2U, nr_vecs); // 2 iovecs accumulated across 2 strides
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
    // B and C are never dispatched: early return after A fails leaves alive_cmds[B]=1, alive_cmds[C]=1.
    ASSERT_FALSE(raid.sync_iov(UBLK_IO_OP_WRITE, &iov1, 1, 0));

    // I/O 2: 8KiB spanning stripe A then stripe B.
    constexpr uint32_t io2_sz = 2 * stripe_sz; // 8KiB
    void* buf2{};
    ASSERT_EQ(0, posix_memalign(&buf2, device_b->block_size(), io2_sz));
    auto iov2 = iovec{.iov_base = buf2, .iov_len = io2_sz};

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([](uint8_t, iovec*, uint32_t nr_vecs, off_t) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            return stripe_sz;
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(1)
        .WillOnce([buf2, stripe_sz](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            // Without the fix: nr_vecs=2, iovecs[0].iov_base is stale (points into buf1 from I/O 1).
            EXPECT_EQ(1U, nr_vecs);
            EXPECT_EQ(static_cast< uint8_t* >(iovecs[0].iov_base), static_cast< uint8_t* >(buf2) + stripe_sz);
            return stripe_sz;
        });

    EXPECT_TRUE(raid.sync_iov(UBLK_IO_OP_WRITE, &iov2, 1, 0));

    free(buf1);
    free(buf2);
}

// Test: async_iov with invalid nr_vecs (0)
TEST(Raid0, AsyncIovInvalidNrVecs) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b});

    auto ublk_data = make_io_data(UBLK_IO_OP_READ);

    // Pass 0 for nr_vecs - should return error
    auto res = raid_device.async_iov(nullptr, &ublk_data, 0, nullptr, 0, 0);

    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), std::make_error_condition(std::errc::invalid_argument));

    remove_io_data(ublk_data);
}

// Test: handle_internal with invalid nr_vecs (0)
TEST(Raid0, HandleInternalInvalidNrVecs) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b});

    auto ublk_data = make_io_data(UBLK_IO_OP_READ);

    // Pass 0 for nr_vecs - should return error
    auto res = raid_device.handle_internal(nullptr, &ublk_data, 0, nullptr, 0, 0, 0);

    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), std::make_error_condition(std::errc::invalid_argument));

    remove_io_data(ublk_data);
}

// Test: async_iov error propagation from device
TEST(Raid0, AsyncIovErrorPropagation) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    // Device A will return an error
    EXPECT_CALL(*device_a, async_iov(UBLK_IO_OP_READ, _, _, _, _, _))
        .Times(1)
        .WillOnce(
            [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) -> io_result {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b});

    auto ublk_data = make_io_data(UBLK_IO_OP_READ);

    // Access first device - should propagate error
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0, nullptr, 4 * Ki, 0);

    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), std::make_error_condition(std::errc::io_error));

    remove_io_data(ublk_data);
}

// Test: handle_flush error propagation
TEST(Raid0, HandleFlushErrorPropagation) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    // Device A returns success
    EXPECT_CALL(*device_a, handle_flush(_, _, _)).Times(1).WillOnce(Return(io_result{1}));

    // Device B returns error - should stop and propagate
    EXPECT_CALL(*device_b, handle_flush(_, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b});

    auto ublk_data = make_io_data(UBLK_IO_OP_FLUSH);
    auto res = raid_device.handle_flush(nullptr, &ublk_data, 0);

    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), std::make_error_condition(std::errc::io_error));

    remove_io_data(ublk_data);
}

// Test: handle_discard error propagation
TEST(Raid0, HandleDiscardErrorPropagation) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    // First device returns error
    EXPECT_CALL(*device_a, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, uint32_t, uint64_t) -> io_result {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b});

    auto ublk_data = make_io_data(UBLK_IO_OP_DISCARD);
    auto res = raid_device.handle_discard(nullptr, &ublk_data, 0, 4 * Ki, 0);

    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), std::make_error_condition(std::errc::io_error));

    remove_io_data(ublk_data);
}

// Test: get_device with invalid offset
TEST(Raid0, GetDeviceInvalidOffset) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b});

    // Try to get device beyond array size
    auto device = raid_device.get_device(5);

    EXPECT_EQ(device, nullptr);
}

// Test: get_device with valid offsets
TEST(Raid0, GetDeviceValidOffsets) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});

    // All valid offsets should return non-null
    EXPECT_NE(raid_device.get_device(0), nullptr);
    EXPECT_NE(raid_device.get_device(1), nullptr);
    EXPECT_NE(raid_device.get_device(2), nullptr);

    // Verify they're the right devices
    EXPECT_EQ(raid_device.get_device(0), device_a);
    EXPECT_EQ(raid_device.get_device(1), device_b);
    EXPECT_EQ(raid_device.get_device(2), device_c);
}
