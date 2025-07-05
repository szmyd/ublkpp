#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/uuid/random_generator.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublksrv.h>

#include "ublkpp/raid/raid1.hpp"
#include "raid1_impl.hpp"
#include "superblock.hpp"
#include "test_disk.hpp"

#define ENABLED_OPTIONS logging, raid1

SISL_LOGGING_INIT(ublk_raid)
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

using ::testing::_;
using ::testing::Return;
using ::ublkpp::Gi;
using ::ublkpp::io_result;

// NOTE: All RAID1 tests have to account for the RESERVED area of the RAID1 device itself,
// this is a section at the HEAD of each backing device that holds a SuperBlock and BitMap
// used for Recovery purposes. The size of this area is CONSTANT in the code (though also
// written to the superblock itself for migration purposes.) so we can make assumptions in
// the tests based on this constant.
using ::ublkpp::raid1::reserved_size;

#define EXPECT_SYNC_OP_REPEAT(OP, CNT, device, fail, sz, off)                                                          \
    EXPECT_CALL(*(device), sync_iov(OP, _, _, _))                                                                      \
        .Times((CNT))                                                                                                  \
        .WillRepeatedly([op = (OP), f = (fail), s = (sz), o = (off)](uint8_t, iovec* iovecs, uint32_t nr_vecs,         \
                                                                     off_t addr) -> io_result {                        \
            EXPECT_EQ(1U, nr_vecs);                                                                                    \
            EXPECT_EQ(s, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));                                          \
            EXPECT_EQ(o, addr);                                                                                        \
            if (f) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));                       \
            if (UBLK_IO_OP_READ == op && nullptr != iovecs->iov_base) memset(iovecs->iov_base, 000, iovecs->iov_len);  \
            return s;                                                                                                  \
        });

#define EXPECT_SYNC_OP(OP, device, fail, sz, off)                                                                      \
    EXPECT_CALL(*(device), sync_iov(OP, _, _, _))                                                                      \
        .Times(1)                                                                                                      \
        .WillOnce([op = (OP), f = (fail), s = (sz), o = (off)](uint8_t, iovec* iovecs, uint32_t nr_vecs,               \
                                                               off_t addr) -> io_result {                              \
            EXPECT_EQ(1U, nr_vecs);                                                                                    \
            EXPECT_EQ(s, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));                                          \
            EXPECT_EQ(o, addr);                                                                                        \
            if (f) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));                       \
            if (UBLK_IO_OP_READ == op && nullptr != iovecs->iov_base) memset(iovecs->iov_base, 000, iovecs->iov_len);  \
            return s;                                                                                                  \
        });

#define EXPECT_SB_OP(OP, device, fail) EXPECT_SYNC_OP(OP, device, fail, ublkpp::raid1::SuperBlock::SIZE, 0UL);
#define EXPECT_TO_WRITE_SB_F(device, fail) EXPECT_SB_OP(UBLK_IO_OP_WRITE, device, fail)
#define EXPECT_TO_WRITE_SB(device) EXPECT_TO_WRITE_SB_F(device, false)

#define EXPECT_TO_READ_SB_F(device, fail) EXPECT_SB_OP(UBLK_IO_OP_READ, device, fail)

#define CREATE_DISK_F(params, no_read, fail_read, no_write, fail_write)                                                \
    [] {                                                                                                               \
        auto device = std::make_shared< ublkpp::TestDisk >((params));                                             \
        /* Expect to load and write clean_unmount bit */                                                               \
        if (!no_read) { EXPECT_TO_READ_SB_F(device, fail_read) }                                                       \
        if (!no_write && !fail_read) { EXPECT_TO_WRITE_SB_F(device, fail_write) }                                      \
        return device;                                                                                                 \
    }()

#define CREATE_DISK(params) CREATE_DISK_F((params), false, false, false, false)

// Brief: If either devices should not load/write superblocks correctly, initialization should throw
TEST(Raid1, FailedReadSBDevA) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, true, false, false);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, true, false, false);
    EXPECT_THROW(auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b),
                 std::runtime_error);
}
TEST(Raid1, FailedReadSBDevB) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, true, false);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, false, true, true, false);
    EXPECT_THROW(auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b),
                 std::runtime_error);
}
TEST(Raid1, FailedReadSBDevBoth) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, true, true, false);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, true, true, false);
    EXPECT_THROW(auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b),
                 std::runtime_error);
}

// Should not throw just dirty SB
TEST(Raid1, FailedUpdateSBDevA) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true);
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_b);
}

// Should not throw just dirty SB
TEST(Raid1, FailedUpdateSBDevB) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, true, false);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true);
    // Expect an extra WRITE to the SB when sync'ing the SB to DevB fails
    EXPECT_SYNC_OP_REPEAT(UBLK_IO_OP_WRITE, 2, device_a, false, ublkpp::raid1::SuperBlock::SIZE, 0UL);
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
}

TEST(Raid1, FailedUpdateSBDevBoth) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true);
    EXPECT_THROW(auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b),
                 std::runtime_error);
}

TEST(Raid1, FailedSecondUpdateDevA) {
    auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, true, false);
    auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, true);
    // Expect an extra WRITE to the SB when sync'ing the SB to DevB fails
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iov, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t nr_vecs, off_t addr) -> io_result {
            EXPECT_EQ(1, nr_vecs);
            EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        });
    EXPECT_THROW(auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b),
                 std::runtime_error);
}

// Brief: Test that RAID1 devices correctly report paramaters based on Devices A and B
//
// Construct a RAID1 device with Identical underlying devices that match on every
// parameter. The final RAID1 parameters should be equivalent to the underlying
// devices themselves.
TEST(Raid1, IdenticalDeviceProbing) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    EXPECT_EQ(raid_device.capacity(), (Gi)-reserved_size);
    EXPECT_STREQ(raid_device.type().c_str(), "Raid1");

    // CanDiscard and DirectIO `true` be default.
    EXPECT_EQ(raid_device.can_discard(), true);
    EXPECT_EQ(raid_device.direct_io, true);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test that RAID1 devices correctly report paramaters based on Devices A and B
//
// Construct a RAID1 device with Differing underlying devices that deviate on every
// parameter. The final RAID1 parameters should represent the lowest feature set of
// both devices including Capacity, BlockSize, Discard
TEST(Raid1, DiffereingDeviceProbing) {
    auto device_a = CREATE_DISK((TestParams{.capacity = 5 * Gi, .l_size = 512, .p_size = 8 * Ki}));
    auto device_b =
        CREATE_DISK((TestParams{.capacity = 3 * Gi, .l_size = 4 * Ki, .p_size = 4 * Ki, .can_discard = false}));

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    // Smallest disk was 3GiB
    EXPECT_EQ(raid_device.capacity(), (3 * Gi) - reserved_size);

    // LBS/PBS represent by shift size, not raw byte count
    EXPECT_EQ(raid_device.block_size(), 4 * Ki);
    EXPECT_EQ(raid_device.params()->basic.physical_bs_shift, ilog2(8 * Ki));

    // Device B lacks Discard support
    EXPECT_EQ(raid_device.can_discard(), false);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test that we open the underlying devices correctly, and return them to our upper layer.
//
// When a UblkDisk receives a call to `open_for_uring`, it's expected to return a std::set of all
// fds that were opened by the underlying Devices in order to register them with io_uring. Test
// that RAID1 is collecting these FDs and passing the io_uring offset to the lower layers.
TEST(Raid1, OpenDevices) {
    static const auto start_idx = 2;
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

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

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);
    auto fd_list = raid_device.open_for_uring(2);
    EXPECT_EQ(3, fd_list.size());
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 3)));
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 2)));
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 1)));

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

TEST(Raid1, UnknownOp) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto ublk_data = make_io_data(0xcafedead, 0xFF, 4 * Ki, 8 * Ki);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, 0b10);
    remove_io_data(ublk_data);
    ASSERT_FALSE(res);

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test a READ through the RAID1 Device. We should only receive the READ on one of the
// two underlying replicas.
TEST(Raid1, SimpleRead) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t nr_vecs, uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // The route should shift up by 1
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            EXPECT_EQ(nr_vecs, 1);
            // It should not have the REPLICATED bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_FALSE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_READ, 4 * Ki, 8 * Ki);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, 0b10);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test retrying a READ through the RAID1 Device, and subsequent READs now go to B
//
// Assuming some I/O READ failed the Target will reissue with the original `route`. Ensure that
// the READ is redirected to the alternative Device.
TEST(Raid1, ReadRetryA) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t, uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b11);
            // It should also have the RETRIED bit set
            EXPECT_TRUE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (12 * Ki) + reserved_size);
            return 1;
        });

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_READ, 4 * Ki, 12 * Ki);
    // Construct a Retry Route that points to Device A in a RAID1 device
    auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b10}, ublkpp::sub_cmd_flags::RETRIED);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, sub_cmd);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // Now test the normal path
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t, uint64_t addr) {
            EXPECT_EQ(data->tag, 0xdeadcafe);
            // The route has changed to point to device_b
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            // It should not have the RETRIED bit set
            EXPECT_FALSE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (12 * Ki) + reserved_size);
            return 1;
        });

    ublk_data = make_io_data(0xdeadcafe, UBLK_IO_OP_READ);
    // Construct a Non-Retry Route
    sub_cmd = ublkpp::sub_cmd_t{0b10};
    res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 12 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Identical to ReadRetryA but for Device B.
TEST(Raid1, ReadRetryB) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t, uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            // It should also have the RETRIED bit set
            EXPECT_TRUE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(iovecs->iov_len, 64 * Ki);
            EXPECT_EQ(addr, (32 * Ki) + reserved_size);
            return 1;
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_READ);
    auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b101}, ublkpp::sub_cmd_flags::RETRIED);
    auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 64 * Ki, 32 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Brief: Test that a simple WRITE operation is replicated to both underlying Devices.
TEST(Raid1, SimpleWrite) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t, uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device A
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            // SubCommand does not have the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 16 * Ki);
            EXPECT_EQ(addr, (12 * Ki) + reserved_size);
            return 1;
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t, uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            // SubCommand has the replicated bit set
            EXPECT_TRUE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 16 * Ki);
            EXPECT_EQ(addr, (12 * Ki) + reserved_size);
            return 1;
        });

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_WRITE, 16 * Ki, 12 * Ki);
    // Construct a Retry Route that points to Device A in a RAID1 device
    auto const current_sub_cmd = 0b10;
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, current_sub_cmd);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(2, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Retry write that failed on DeviceA and check next write does not dirty bitmap again
TEST(Raid1, WriteRetryA) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB(device_b);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                         uint32_t, uint64_t addr) {
                EXPECT_EQ(data->tag, 0xcafedead);
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                // SubCommand has the replicated bit set now
                EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_EQ(addr, (8 * Ki) + reserved_size);
                return 1;
            });

        auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
    }

    // Queued Retries should not Fail Immediately, and not dirty of bitmap
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                         uint32_t, uint64_t addr) {
                EXPECT_EQ(data->tag, 0xcafedeaa);
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                // SubCommand has the replicated bit set now
                EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
                EXPECT_EQ(iovecs->iov_len, 12 * Ki);
                EXPECT_EQ(addr, (16 * Ki) + reserved_size);
                return 1;
            });

        auto ublk_data = make_io_data(0xcafedeaa, UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
    }

    // Subsequent writes should not go to device A
    auto ublk_data = make_io_data(0xcafedeae, UBLK_IO_OP_WRITE);
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t, uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedeae);
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // expect unmount_clean on Device B
    EXPECT_TO_WRITE_SB(device_b);
}

// Retry write that failed on DeviceB
TEST(Raid1, WriteRetryB) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_WRITE);
        auto sub_cmd =
            ublkpp::set_flags(ublkpp::sub_cmd_t{0b101},
                                   ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        // No need to re-write on A side
        EXPECT_EQ(0, res.value());
    }

    // Queued Retries should not Fail Immediately, and not dirty of bitmap
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(0xcafedeaa, UBLK_IO_OP_WRITE);
        auto sub_cmd =
            ublkpp::set_flags(ublkpp::sub_cmd_t{0b101},
                                   ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(0, res.value());
    }

    // expect unmount_clean on Device A
    EXPECT_TO_WRITE_SB(device_a);
}

// Double Failure returns I/O error
TEST(Raid1, WriteDoubleFailure) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_WRITE);
        auto sub_cmd =
            ublkpp::set_flags(ublkpp::sub_cmd_t{0b101},
                                   ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        // No need to re-write on A side
        EXPECT_EQ(0, res.value());
    }

    // Queued Retries should not Fail Immediately, and not dirty of bitmap
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(0xcafedeaa, UBLK_IO_OP_WRITE);
        auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, sub_cmd, nullptr, 12 * Ki, 16 * Ki);
        remove_io_data(ublk_data);
        ASSERT_FALSE(res);
    }

    // expect unmount_clean on last working device
    EXPECT_TO_WRITE_SB_F(device_a, true);
}

// Immediate Write Fail
TEST(Raid1, WriteFailImmediate) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB(device_b);
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce(
                [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                   uint64_t const) { return folly::makeUnexpected(std::make_error_condition(std::errc::io_error)); });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                         uint32_t, uint64_t addr) {
                EXPECT_EQ(data->tag, 0xcafedead);
                EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
                // SubCommand has the replicated bit set now
                EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
                EXPECT_EQ(iovecs->iov_len, 4 * Ki);
                EXPECT_EQ(addr, (8 * Ki) + reserved_size);
                return 1;
            });

        auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_WRITE);
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        EXPECT_EQ(1, res.value());
    }

    // Subsequent writes should not go to device A
    auto ublk_data = make_io_data(0xcafedeae, UBLK_IO_OP_WRITE);
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t, uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedeae);
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());

    // expect unmount_clean on Device B
    EXPECT_TO_WRITE_SB(device_b);
}

// Failed Discards should flip the route too
TEST(Raid1, Discard) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, uint32_t len,
                     uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedeae);
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });
    EXPECT_CALL(*device_b, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, uint32_t len,
                     uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedeae);
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b101);
            EXPECT_TRUE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });

    auto ublk_data = make_io_data(0xcafedeae, UBLK_IO_OP_DISCARD, 4 * Ki, 8 * Ki);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, 0b10);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(2, res.value());
    // expect unmount_clean on devices
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
// Failed Discards should flip the route too
TEST(Raid1, DiscardRetry) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    {
        EXPECT_TO_WRITE_SB(device_a);
        EXPECT_CALL(*device_a, handle_discard(_, _, _, _, _)).Times(0);
        EXPECT_CALL(*device_b, handle_discard(_, _, _, _, _)).Times(0);

        auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_WRITE);
        auto sub_cmd =
            ublkpp::set_flags(ublkpp::sub_cmd_t{0b101},
                                   ublkpp::sub_cmd_flags::RETRIED | ublkpp::sub_cmd_flags::REPLICATED);
        auto res = raid_device.handle_discard(nullptr, &ublk_data, sub_cmd, 4 * Ki, 8 * Ki);
        remove_io_data(ublk_data);
        ASSERT_TRUE(res);
        // No need to re-write on A side
        EXPECT_EQ(0, res.value());
    }

    // Subsequent reads should not go to device B
    auto ublk_data = make_io_data(0xcafedeae, UBLK_IO_OP_READ);
    EXPECT_CALL(*device_a, async_iov(UBLK_IO_OP_READ, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t, uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedeae);
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100);
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr, (8 * Ki) + reserved_size);
            return 1;
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 4 * Ki, 8 * Ki);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());
    // expect unmount_clean on Device A
    EXPECT_TO_WRITE_SB(device_a);
}

// Flush is a no-op in RAID1
TEST(Raid1, FlushRetry) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    EXPECT_CALL(*device_a, handle_flush(_, _, _)).Times(0);
    EXPECT_CALL(*device_b, handle_flush(_, _, _)).Times(0);
    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_FLUSH);
    auto sub_cmd = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100}, ublkpp::sub_cmd_flags::RETRIED);
    auto res = raid_device.queue_tgt_io(nullptr, &ublk_data, sub_cmd);
    remove_io_data(ublk_data);
    ASSERT_TRUE(res);
    EXPECT_EQ(0, res.value());

    // expect unmount_clean update
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Test basic R/W on the Raid1Disk::sync_io
TEST(Raid1, SyncIoSuccess) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto test_op = UBLK_IO_OP_READ;
    auto test_off = 8 * Ki;
    auto test_sz = 12 * Ki;

    // Reads will only go to device_a at start
    EXPECT_SYNC_OP(test_op, device_a, false, test_sz, test_off + ublkpp::raid1::reserved_size);

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    EXPECT_EQ(test_sz, res.value());

    test_op = UBLK_IO_OP_WRITE;
    test_off = 1024 * Ki;
    test_sz = 16 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, false, test_sz, test_off + ublkpp::raid1::reserved_size);
    EXPECT_SYNC_OP(test_op, device_b, false, test_sz, test_off + ublkpp::raid1::reserved_size);

    res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    EXPECT_EQ(test_sz, res.value());

    // expect unmount_clean on devices
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

TEST(Raid1, SyncIoWriteFailA) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + ublkpp::raid1::reserved_size);
    EXPECT_CALL(*device_b, sync_iov(test_op, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([test_sz](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ(test_off + ublkpp::raid1::reserved_size, addr);
            return iov->iov_len;
        });

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    // No need to re-write on A side
    EXPECT_EQ(test_sz, res.value());

    // expect unmount_clean on Device B
    EXPECT_TO_WRITE_SB(device_b);
}

TEST(Raid1, SyncIoWriteFailB) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_CALL(*device_a, sync_iov(test_op, _, _, _))
        .Times(2)
        .WillOnce([test_off, test_sz](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ(test_off + ublkpp::raid1::reserved_size, addr);
            return iov->iov_len;
        })
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        });
    EXPECT_SYNC_OP(test_op, device_b, true, test_sz, test_off + ublkpp::raid1::reserved_size);

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    // No need to re-write on A side
    EXPECT_EQ(test_sz, res.value());


    // expect unmount_clean on Device A
    EXPECT_TO_WRITE_SB(device_a);
}

// This test fails the initial sync_io to the working device and then fails the SB update to dirty the bitmap on
// the replica. The I/O should fail in this case.
TEST(Raid1, SyncIoWriteFailDirty) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + ublkpp::raid1::reserved_size);
    EXPECT_SB_OP(test_op, device_b, true);

    ASSERT_FALSE(raid_device.sync_io(UBLK_IO_OP_WRITE, nullptr, test_sz, test_off));

    // Even though I/O failed, the status is still OK since devices are in same state pre-I/O
    // expect attempt to sync on last working disk
    EXPECT_TO_WRITE_SB_F(device_b, true);
}

// This test fails the initial WRITE sync_io to the working device and then succeeds the SB update to dirty the bitmap
// on the replica, however the re-issued WRITE fails on the replica. The device *IS* degraded after this.
TEST(Raid1, SyncIoWriteFailBoth) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_WRITE;
    auto const test_off = 8 * Ki;
    auto const test_sz = 12 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + ublkpp::raid1::reserved_size);
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(2)
        .WillOnce([](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(ublkpp::raid1::SuperBlock::SIZE, iov->iov_len);
            EXPECT_EQ(0UL, addr);
            return iov->iov_len;
        })
        .WillOnce([test_off, test_sz](uint8_t, iovec* iov, uint32_t, off_t addr) -> io_result {
            EXPECT_EQ(test_sz, iov->iov_len);
            EXPECT_EQ(test_off + ublkpp::raid1::reserved_size, addr);
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        });

    ASSERT_FALSE(raid_device.sync_io(UBLK_IO_OP_WRITE, nullptr, test_sz, test_off));

    // expect attempt to sync on last working disk
    EXPECT_TO_WRITE_SB_F(device_b, true);
}

// This test is similar to SyncIoFailBoth, however in the case of a READ the SB is not written first and in
// this case reading from the replica is a success. The device is *NOT* degraded after this.
TEST(Raid1, SyncIoReadDevAFail) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_READ;
    auto const test_off = 8 * Ki;
    auto const test_sz = 80 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + ublkpp::raid1::reserved_size);
    EXPECT_SYNC_OP(test_op, device_b, false, test_sz, test_off + ublkpp::raid1::reserved_size);

    auto res = raid_device.sync_io(test_op, nullptr, test_sz, test_off);
    ASSERT_TRUE(res);
    EXPECT_EQ(test_sz, res.value());


    // expect unmount_clean on both (READ fails do not dirty bitmap)
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// This test is similar to SyncIoDevAFail, but the re-issued READ fails too. Still device is *NOT* degraded!
TEST(Raid1, SyncIoReadFailBoth) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::random_generator()(), device_a, device_b);

    auto const test_op = UBLK_IO_OP_READ;
    auto const test_off = 64 * Ki;
    auto const test_sz = 1024 * Ki;

    EXPECT_SYNC_OP(test_op, device_a, true, test_sz, test_off + ublkpp::raid1::reserved_size);
    EXPECT_SYNC_OP(test_op, device_b, true, test_sz, test_off + ublkpp::raid1::reserved_size);

    ASSERT_FALSE(raid_device.sync_io(test_op, nullptr, test_sz, test_off));

    // expect attempt to sync both SBs
    EXPECT_TO_WRITE_SB_F(device_a, true);
    EXPECT_TO_WRITE_SB_F(device_b, true);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
