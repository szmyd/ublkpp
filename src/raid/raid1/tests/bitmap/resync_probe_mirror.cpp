#include "test_raid1_common.hpp"

#include <thread>

using namespace std::chrono_literals;

// Test: resync task unblocks itself via probe_mirror when UNAVAIL is set (Bug 1 fix).
//
// Before the fix, __become_degraded always set unavail on the failed device, and the
// resync task's wait loops only slept — nobody ever cleared unavail in the degraded path.
// The idle probe skips degraded arrays, so resync was deadlocked forever.
//
// With the fix, the resync task calls probe_mirror after each sleep, which performs a
// read at reserved_size.  If the device has recovered the unavail flag is cleared and
// resync proceeds without any external intervention.
TEST(Raid1, ResyncUnblocksViaProbeMirror) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Step 1: Degrade device_b via write failure.
    // __become_degraded sets unavail on B and transitions route to DEVA.
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        });
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(1)
        .WillOnce(
            [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) { return 1; });
    EXPECT_TO_WRITE_SB(device_a); // degradation superblock

    auto write_data = make_io_data(UBLK_IO_OP_WRITE, 4 * Ki, 12 * Ki);
    ASSERT_TRUE(raid_device.queue_tgt_io(nullptr, &write_data, 0b10));
    raid_device.on_io_complete(&write_data, 0b100, 0);
    remove_io_data(write_data);

    ASSERT_EQ(ublkpp::raid1::replica_state::ERROR, raid_device.replica_states().device_b);

    // Step 2: Set up mock for resync + probe operations.
    // device_b READ at any addr — the resync task's probe_mirror calls sync_iov(READ, ...)
    // at reserved_size; returning success clears unavail and unblocks resync.
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AtLeast(1)) // at least one probe_mirror call expected
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });

    // device_a READs — resync copies data from A (clean) to B (dirty)
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AtLeast(0))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            if (nullptr != iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });

    // device_b WRITEs — resync data writes + __become_clean SB + destructor SB
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AtLeast(0))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });

    // device_a WRITEs — bitmap page clears + __become_clean SB + destructor SB
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AtLeast(0))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });

    // Step 3: Enable resync. The task starts, sees unavail on B, sleeps avail_delay,
    // then calls probe_mirror which succeeds → clears unavail → resync proceeds.
    raid_device.toggle_resync(true);

    // Wait up to 12s (avail_delay=5s default + resync time + buffer).
    ASSERT_TRUE(wait_for_clean_state(raid_device, 12s));

    auto const states = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, states.device_a);
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, states.device_b);
    EXPECT_EQ(0, states.bytes_to_sync);

    // Destructor SB writes (route=EITHER after __become_clean → writes to both).
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
