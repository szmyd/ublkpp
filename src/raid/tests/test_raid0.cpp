#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/uuid/random_generator.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublksrv.h>

#include "ublkpp/raid/raid0.hpp"
#include "raid0_impl.hpp"
#include "superblock.hpp"
#include "test_disk.hpp"

SISL_LOGGING_INIT(ublk_raid)

SISL_OPTIONS_ENABLE(logging)

using ::testing::_;
using ::testing::Return;
using ::ublkpp::Gi;
using ::ublkpp::io_result;
using ::ublkpp::UblkDisk;

#define EXPECT_SYNC_OP(OP, device, fail, sz, off)                                                                      \
    EXPECT_CALL(*(device), sync_iov(OP, _, _, _))                                                                      \
        .Times(1)                                                                                                      \
        .WillOnce([op = (OP), f = (fail), s = (sz), o = (off)](uint8_t, iovec* iovecs, uint32_t nr_vecs,               \
                                                               off_t addr) -> io_result {                              \
            EXPECT_EQ(1U, nr_vecs);                                                                                    \
            EXPECT_EQ(s, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));                                               \
            EXPECT_EQ(o, addr);                                                                                        \
            if (f) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));                       \
            if (UBLK_IO_OP_READ == op && nullptr != iovecs->iov_base) memset(iovecs->iov_base, 000, iovecs->iov_len);  \
            return s;                                                                                                  \
        });

#define EXPECT_SB_OP(OP, device, fail) EXPECT_SYNC_OP(OP, device, fail, ublkpp::raid0::SuperBlock::SIZE, 0UL);

#define CREATE_DISK_F(params, no_read, fail_read, no_write, fail_write)                                                \
    [] {                                                                                                               \
        auto device = std::make_shared< ublkpp::TestDisk >((params));                                                  \
        if (!no_read) { EXPECT_SB_OP(UBLK_IO_OP_READ, device, fail_read) }                                             \
        if (!no_write && !fail_read) { EXPECT_SB_OP(UBLK_IO_OP_WRITE, device, fail_write) }                            \
        return device;                                                                                                 \
    }()

#define CREATE_DISK(params) CREATE_DISK_F((params), false, false, false, false)

// Brief: If any device should not load/write superblocks correctly, initialization should throw
TEST(Raid0, FailedReadSB) {
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, true, false, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, true, true, false, false);
        EXPECT_THROW(auto raid_device =
                         ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                           std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b}),
                     std::runtime_error);
    }
    {
        auto device_a = CREATE_DISK_F(TestParams{.capacity = Gi}, false, false, false, false);
        auto device_b = CREATE_DISK_F(TestParams{.capacity = Gi}, false, true, false, false);
        EXPECT_THROW(auto raid_device =
                         ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                           std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b}),
                     std::runtime_error);
    }
}

// Brief: Test that RAID0 devices correctly report paramaters based on Devices A and B
//
// Construct a RAID0 device with 3 Identical underlying devices that match on every
// parameter. The final RAID0 parameters should be equivalent to the underlying
// devices themselves with the capacity being 3x the device size.
TEST(Raid0, IdenticalDeviceProbing) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});
    EXPECT_EQ(raid_device.capacity(), (3 * Gi) - (32 * 3 * Ki));
    EXPECT_STREQ(raid_device.type().c_str(), "Raid0");

    // CanDiscard and DirectIO `true` be default.
    EXPECT_EQ(raid_device.can_discard(), true);
    EXPECT_EQ(raid_device.direct_io, true);
}

// Brief: Test that RAID0 devices correctly report paramaters based on Devices A and B
//
// Construct a RAID0 device with Differing underlying devices that deviate on every
// parameter. The final RAID0 parameters should represent the lowest feature set of
// both devices including Capacity, BlockSize, Discard and DirectI/O support.
TEST(Raid0, DiffereingDeviceProbing) {
    auto device_a = CREATE_DISK((TestParams{.capacity = 5 * Gi, .l_size = 512, .p_size = 8 * Ki, .direct_io = false}));
    auto device_b =
        CREATE_DISK((TestParams{.capacity = 3 * Gi, .l_size = 4 * Ki, .p_size = 4 * Ki, .can_discard = false}));

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b});
    // Smallest disk was 3GiB, so 2 * 3GiB
    EXPECT_EQ(raid_device.capacity(), (6 * Gi) - (32 * 2 * Ki));

    // LBS/PBS represent by shift size, not raw byte count
    EXPECT_EQ(raid_device.block_size(), 4 * Ki);
    EXPECT_EQ(raid_device.params()->basic.physical_bs_shift, ilog2(8 * Ki));

    // Device B lacks Discard support
    EXPECT_EQ(raid_device.can_discard(), false);
    // Device A lacks DirectI/O support
    EXPECT_EQ(raid_device.direct_io, false);
}

// Brief: Test that we open the underlying devices correctly, and return them to our upper layer.
//
// When a UblkDisk receives a call to `open_for_uring`, it's expected to return a std::set of all
// fds that were opened by the underlying Devices in order to register them with io_uring. Test
// that RAID0 is collecting these FDs and passing the io_uring offset to the lower layers.
TEST(Raid0, OpenDevices) {
    static const auto start_idx = 2;
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});

    // Each device should be subsequently opened and return a set with their sole FD.
    EXPECT_CALL(*device_a, open_for_uring(_)).Times(1).WillOnce([](int const fd_off) {
        EXPECT_EQ(start_idx, fd_off);
        // Return 2 FDs here, maybe it's another RAID0 device?
        return std::list< int >{INT_MAX - 2, INT_MAX - 3};
    });
    EXPECT_CALL(*device_b, open_for_uring(_)).Times(1).WillOnce([](int const fd_off) {
        // Device A took 2 uring offsets
        EXPECT_EQ(start_idx + 2, fd_off);
        return std::list< int >{INT_MAX - 1};
    });

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b});
    auto fd_list = raid_device.open_for_uring(2);
    EXPECT_EQ(3, fd_list.size());
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 3)));
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 2)));
    EXPECT_NE(fd_list.end(), std::find(fd_list.begin(), fd_list.end(), (INT_MAX - 1)));
}

// Brief: Test a READ through the RAID0 Device. We should only receive the READ on one of the
// three underlying stripes.
TEST(Raid0, SimpleRead) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    EXPECT_CALL(*device_a, async_iov(UBLK_IO_OP_READ, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t nr_vecs, uint64_t addr) -> io_result {
            EXPECT_EQ(data->tag, 0xcafedead);
            // The route should shift up by 4
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100000);
            // It should also not have the REPLICATED bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_FALSE(ublkpp::is_retry(sub_cmd));
            EXPECT_EQ(nr_vecs, 1);
            EXPECT_EQ(iovecs->iov_len, 4 * Ki);
            EXPECT_EQ(addr - (32 * Ki), 8 * Ki);
            return 1;
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*device_c, async_iov(_, _, _, _, _, _)).Times(0);

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_READ);
    auto const current_route = 0b10; // Pretend we've already gone through some upper layer
    auto res = raid_device.handle_rw(nullptr, &ublk_data, current_route, nullptr, 4 * Ki, 8 * Ki);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());
    remove_io_data(ublk_data);
}

// Brief: Test that a simple WRITE operation is again only received on a single stripe.
TEST(Raid0, SimpleWrite) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t nr_vecs, uint64_t addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100000);
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(nr_vecs, 1);
            EXPECT_EQ(iovecs->iov_len, 16 * Ki);
            // This is the first chunk of the second device
            EXPECT_EQ(addr, (36 * Ki) + (32 * Ki));
            return 1;
        });

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a});

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_WRITE);
    auto const current_route = 0b10; // Pretend we've already gone through some upper layer

    // Set the address to within the second stripe (32KiB stripes)
    auto res = raid_device.handle_rw(nullptr, &ublk_data, current_route, nullptr, 16 * Ki, 36 * Ki);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());
    remove_io_data(ublk_data);
}

// Test that a overlapping I/O is correct Split and then formed into multiple iovec structures
TEST(Raid0, SplitWrite) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    void* fake_buffer;
    ASSERT_EQ(0, posix_memalign(&fake_buffer, device_a->block_size(), 96 * Ki));
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([fake_buffer](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd,
                                iovec* iovecs, uint32_t nr_vecs, uint64_t addr) -> io_result {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100000);
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(nr_vecs, 1);
            EXPECT_EQ(iovecs->iov_len, 32 * Ki);
            EXPECT_EQ((uint8_t*)iovecs->iov_base, (uint8_t*)fake_buffer + (60 * Ki));
            EXPECT_EQ(addr, (32 * Ki) * 2);
            return 1;
        });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([fake_buffer](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd,
                                iovec* iovecs, uint32_t nr_vecs, uint64_t addr) -> io_result {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100001);
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(nr_vecs, 2);
            EXPECT_EQ(ublkpp::__iovec_len(iovecs, iovecs + nr_vecs), 32 * Ki);
            EXPECT_EQ((uint8_t*)iovecs->iov_base, (uint8_t*)fake_buffer);
            EXPECT_EQ((uint8_t*)(iovecs + 1)->iov_base, (uint8_t*)fake_buffer + (92 * Ki));
            // This is the first chunk of the second device
            EXPECT_EQ(addr - (32 * Ki), (36 * Ki) - (32 * Ki));
            return 1;
        });
    EXPECT_CALL(*device_c, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([fake_buffer](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd,
                                iovec* iovecs, uint32_t nr_vecs, uint64_t addr) -> io_result {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100010);
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(nr_vecs, 1);
            EXPECT_EQ(iovecs->iov_len, 32 * Ki);
            EXPECT_EQ((uint8_t*)iovecs->iov_base, (uint8_t*)fake_buffer + (28 * Ki));
            // This is the first chunk of the second device
            EXPECT_EQ(addr, (32 * Ki));
            return 1;
        });

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_WRITE);

    // Set the address to within the second stripe (32KiB stripes)
    auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, fake_buffer, 96 * Ki, 36 * Ki);
    ASSERT_TRUE(res);
    EXPECT_EQ(3, res.value());
    remove_io_data(ublk_data);
    free(fake_buffer);
}

TEST(Raid0, RetrySplitWritePortion) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _)).Times(0);
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec* iovecs,
                     uint32_t nr_vecs, uint64_t addr) -> io_result {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100001);
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            EXPECT_EQ(nr_vecs, 1);
            EXPECT_EQ(iovecs->iov_len, 28 * Ki);
            // This is the first chunk of the second device
            EXPECT_EQ(addr - (32 * Ki), (36 * Ki) - (32 * Ki));
            return 1;
        });
    EXPECT_CALL(*device_c, async_iov(_, _, _, _, _, _)).Times(0);

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_WRITE);
    auto const retried_route = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100001}, ublkpp::sub_cmd_flags::RETRIED);

    // Set the address to within the second stripe (32KiB stripes)
    auto res = raid_device.handle_rw(nullptr, &ublk_data, retried_route, nullptr, 44 * Ki, 36 * Ki);
    ASSERT_TRUE(res);
    EXPECT_EQ(1, res.value());
    remove_io_data(ublk_data);
}

// Brief: Test that a simple DISCARD operation is received on all underlying devices
TEST(Raid0, SimpleDiscard) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    EXPECT_CALL(*device_a, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, uint32_t const len,
                     uint64_t const addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device A
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100000);
            EXPECT_EQ(len, 28 * Ki);
            EXPECT_EQ(addr, (32 * Ki) + (100 * Ki));
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            return 1;
        });

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a});

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_DISCARD);
    auto const current_route = 0b10; // Pretend we've already gone through some upper layer

    // Set the address to within the second stripe (32KiB stripes)
    auto res = raid_device.handle_discard(nullptr, &ublk_data, current_route, 28 * Ki, 100 * Ki);
    ASSERT_TRUE(res);
    // Should have 3 total discards even though it wraps around to DeviceB twice
    EXPECT_EQ(1, res.value());
    remove_io_data(ublk_data);
}

// Brief: Test that a simple DISCARD operation is received on all underlying devices
TEST(Raid0, MergedDiscard) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    EXPECT_CALL(*device_a, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, uint32_t const len,
                     uint64_t const addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device A
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100000);
            EXPECT_EQ(len, 32 * Ki);
            EXPECT_EQ(addr, (64 * Ki));
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            return 1;
        });
    EXPECT_CALL(*device_b, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, uint32_t const len,
                     uint64_t const addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100001);
            EXPECT_EQ(len, 36 * Ki);
            EXPECT_EQ(addr, (32 * Ki) + (4 * Ki));
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            return 1;
        });
    EXPECT_CALL(*device_c, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, uint32_t const len,
                     uint64_t const addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device C
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100010);
            EXPECT_EQ(len, 32 * Ki);
            EXPECT_EQ(addr, 32 * Ki);
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            return 1;
        });

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_DISCARD);
    auto const current_route = 0b10; // Pretend we've already gone through some upper layer

    // Set the address to within the second stripe (32KiB stripes)
    auto res = raid_device.handle_discard(nullptr, &ublk_data, current_route, 100 * Ki, 36 * Ki);
    ASSERT_TRUE(res);
    // Should have 3 total discards even though it wraps around to DeviceB twice
    EXPECT_EQ(3, res.value());
    remove_io_data(ublk_data);
}

// Brief: Test that a simple DISCARD RETRY operation is received on retried sub_cmd
TEST(Raid0, MergedDiscardRetry) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    EXPECT_CALL(*device_a, handle_discard(_, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, uint32_t const len,
                     uint64_t const addr) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device A
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100000);
            EXPECT_EQ(len, 32 * Ki);
            EXPECT_EQ(addr, (64 * Ki));
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            return 1;
        });
    EXPECT_CALL(*device_b, handle_discard(_, _, _, _, _)).Times(0);
    EXPECT_CALL(*device_c, handle_discard(_, _, _, _, _)).Times(0);

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_DISCARD);
    auto const retried_route = ublkpp::set_flags(ublkpp::sub_cmd_t{0b100000}, ublkpp::sub_cmd_flags::RETRIED);

    // Set the address to within the second stripe (32KiB stripes)
    auto res = raid_device.handle_discard(nullptr, &ublk_data, retried_route, 100 * Ki, 36 * Ki);
    ASSERT_TRUE(res);
    // Should have 1 total discards retries
    EXPECT_EQ(1, res.value());
    remove_io_data(ublk_data);
}

// Brief: Test that a simple FLUSH operation is received on all underlying devices
TEST(Raid0, SimpleFlush) {
    auto device_a = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK(TestParams{.capacity = Gi});
    auto device_c = CREATE_DISK(TestParams{.capacity = Gi});
    EXPECT_CALL(*device_a, handle_flush(_, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100000);
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            return 1;
        });
    EXPECT_CALL(*device_b, handle_flush(_, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100001);
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            return 1;
        });
    EXPECT_CALL(*device_c, handle_flush(_, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd) {
            EXPECT_EQ(data->tag, 0xcafedead);
            // Route is for Device B
            EXPECT_EQ(sub_cmd & ublkpp::_route_mask, 0b100010);
            // SubCommand has the replicated bit set
            EXPECT_FALSE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::REPLICATED));
            return 1;
        });

    auto raid_device = ublkpp::Raid0Disk(boost::uuids::random_generator()(), 32 * Ki,
                                         std::vector< std::shared_ptr< UblkDisk > >{device_a, device_b, device_c});

    auto ublk_data = make_io_data(0xcafedead, UBLK_IO_OP_FLUSH);
    auto const current_route = 0b10; // Pretend we've already gone through some upper layer

    auto res = raid_device.handle_flush(nullptr, &ublk_data, current_route);
    ASSERT_TRUE(res);
    EXPECT_EQ(3, res.value());
    remove_io_data(ublk_data);
}

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

#define TEST_ACCESS(NR, SS, ADDR, LEN, DOFF, LOFF, SZ)                                                                 \
    {                                                                                                                  \
        auto const ss = (SS);                                                                                          \
        auto [d_off, l_off, sz] = ublkpp::raid0::next_subcmd(ss * (NR), ss, (ADDR), (LEN));                            \
        EXPECT_EQ((DOFF), d_off);                                                                                      \
        EXPECT_EQ((LOFF), l_off);                                                                                      \
        EXPECT_EQ((SZ), sz);                                                                                           \
    }

// Some some various outcomes of writes across a RAID stripe used by the I/O handlers
TEST(Raid0, CalcTuples) {
    // Access on first stripe of 3-device RAID, first chunk
    TEST_ACCESS(3, Ki, 0x0000, 512, 0, 0, 512);
    // Access on second stripe of 3-device RAID, third chunk
    TEST_ACCESS(3, Ki, ((Ki) * 6) + Ki + 512, 512, 1, (2 * Ki) + 512, 512);
    // Access on second stripe of 3-device RAID, second chunk, across third device
    TEST_ACCESS(3, 128 * Ki, ((128 * Ki) * 4) + Ki, 128 * Ki, 1, (129 * Ki), 127 * Ki);
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
