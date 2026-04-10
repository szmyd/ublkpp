#include "test_raid1_common.hpp"

#include <atomic>
#include <thread>

using namespace std::chrono_literals;

// Test that writes arriving DURING active resync correctly pause/resume the outstanding_writes counter
// This is the critical synchronization test for the atomic counter fixes
TEST(Raid1, ConcurrentIODuringResync) {
    auto device_a = CREATE_DISK_A(TestParams{.capacity = Gi});
    auto device_b = CREATE_DISK_B(TestParams{.capacity = Gi});
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);

    raid_device.toggle_resync(false);

    // Step 1: Create MANY dirty regions across the disk to ensure resync takes measurable time
    // We'll create 50 regions of 32Ki each = 1.6MB of dirty data
    std::vector< uint64_t > dirty_offsets;
    for (uint64_t i = 0; i < 50; ++i) {
        dirty_offsets.push_back(i * 64 * Ki); // Every 64Ki offset, dirty 32Ki
    }

    {
        // Set up expectations for creating dirty regions
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(static_cast< int >(dirty_offsets.size()))
            .WillRepeatedly([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                               uint64_t) { return 1; });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t, uint64_t) {
                return std::unexpected(std::make_error_condition(std::errc::io_error));
            });
        EXPECT_TO_WRITE_SB(device_a);

        // Dirty multiple regions by failing writes to device B
        for (auto offset : dirty_offsets) {
            auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
            auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 32 * Ki, offset);
            raid_device.on_io_complete(&ublk_data, 0b100, 0);
            remove_io_data(ublk_data);
            ASSERT_TRUE(res);
        }
    }

    // Verify we have dirty regions
    auto cur_replica_state = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::ERROR, cur_replica_state.device_b);
    EXPECT_EQ(dirty_offsets.size() * 32 * Ki, cur_replica_state.bytes_to_sync);

    {
        // Make Device B available again
        auto ublk_data = make_io_data(UBLK_IO_OP_READ);
        auto sub_cmd = ublkpp::set_flags(0b101, ublkpp::sub_cmd_flags::INTERNAL);
        auto res = raid_device.queue_internal_resp(nullptr, &ublk_data, sub_cmd, 0);
        ASSERT_TRUE(res);
        EXPECT_EQ(0, res.value());
        remove_io_data(ublk_data);
    }

    // Step 2: Set up flexible expectations for resync operations
    // Resync will read from device A and write to device B
    // Add delays to make resync take measurable time (simulates slower I/O)
    std::atomic< int > resync_reads{0};
    std::atomic< int > resync_writes{0};

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AtLeast(0))
        .WillRepeatedly([&resync_reads](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            resync_reads.fetch_add(1, std::memory_order_relaxed);
            EXPECT_EQ(1U, nr_vecs);
            if (nullptr != iovecs->iov_base) {
                // Simulate reading data
                memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            }
            // Simulate slow I/O - 2ms per read operation
            std::this_thread::sleep_for(2ms);
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AtLeast(0))
        .WillRepeatedly([&resync_writes](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            resync_writes.fetch_add(1, std::memory_order_relaxed);
            EXPECT_EQ(1U, nr_vecs);
            // Simulate slow I/O - 2ms per write operation
            std::this_thread::sleep_for(2ms);
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });

    // Allow superblock reads for both devices
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AtLeast(0))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });

    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AtLeast(0))
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            EXPECT_EQ(1U, nr_vecs);
            if (nullptr != iovecs->iov_base) {
                memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);
                static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b = 1;
            }
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });

    // Step 3: Set up expectations for concurrent async writes during resync
    std::atomic< int > concurrent_writes{0};
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(::testing::AtLeast(0))
        .WillRepeatedly([&concurrent_writes](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*,
                                             uint32_t, uint64_t) -> io_result {
            concurrent_writes.fetch_add(1, std::memory_order_relaxed);
            return 1;
        });

    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(::testing::AtLeast(0))
        .WillRepeatedly([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                           uint64_t) -> io_result { return 1; });

    // Step 4: Start resync
    raid_device.toggle_resync(true);

    // Give resync a moment to start
    std::this_thread::sleep_for(5ms);

    // Step 5: Issue concurrent writes to different regions while resync is running
    // These should pause/resume the resync via the outstanding_writes counter
    std::vector< uint64_t > concurrent_offsets = {128 * Ki, 384 * Ki, 768 * Ki, 1536 * Ki};
    std::vector< ublkpp::sub_cmd_t > working_subs;
    std::vector< ublk_io_data > io_datas;

    for (auto offset : concurrent_offsets) {
        auto ublk_data = make_io_data(UBLK_IO_OP_WRITE);
        io_datas.push_back(ublk_data);

        // Issue the write - this should ENQUEUE (increment counter, pause resync if 0->1)
        auto res = raid_device.handle_rw(nullptr, &ublk_data, 0b10, nullptr, 32 * Ki, offset);
        ASSERT_TRUE(res);
        EXPECT_GE(res.value(), 1); // Should get at least 1 async write queued

        // Store the sub_cmd for later completion (we'll complete these in a moment)
        working_subs.push_back(0b100); // Primary device A
        if (res.value() == 2) {
            working_subs.push_back(0b101); // Replica device B
        }
    }

    // Let the writes be in-flight while resync is paused
    std::this_thread::sleep_for(10ms);

    // Record how many resync operations happened before we pause with writes
    auto resync_ops_before =
        resync_reads.load(std::memory_order_relaxed) + resync_writes.load(std::memory_order_relaxed);

    // Step 6: Complete the concurrent writes
    // This should DEQUEUE (decrement counter, resume resync when counter hits 0)
    size_t io_idx = 0;
    for (size_t i = 0; i < concurrent_offsets.size(); ++i) {
        // Complete primary write
        raid_device.on_io_complete(&io_datas[i], 0b100, 0);

        // If there was a replica write, complete it too
        if (io_idx + 1 < working_subs.size() && working_subs[io_idx + 1] == 0b101) {
            raid_device.on_io_complete(&io_datas[i], 0b101, 0);
            io_idx++;
        }
        io_idx++;
    }

    // Clean up io_data
    for (auto& io_data : io_datas) {
        remove_io_data(io_data);
    }

    // Step 7: Give resync time to resume and complete
    std::this_thread::sleep_for(100ms);

    // Verify resync resumed and made progress after writes completed
    auto resync_ops_after =
        resync_reads.load(std::memory_order_relaxed) + resync_writes.load(std::memory_order_relaxed);
    EXPECT_GT(resync_ops_after, resync_ops_before) << "Resync should have resumed and made progress after writes";

    // Verify concurrent writes actually happened
    EXPECT_GE(concurrent_writes.load(std::memory_order_relaxed), static_cast< int >(concurrent_offsets.size()))
        << "Concurrent writes should have been issued";

    // Step 8: Verify final state - all regions should eventually be clean
    // (May take additional time depending on resync_delay)
    std::this_thread::sleep_for(50ms);

    cur_replica_state = raid_device.replica_states();
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, cur_replica_state.device_a);
    EXPECT_EQ(ublkpp::raid1::replica_state::CLEAN, cur_replica_state.device_b);
    EXPECT_EQ(0, cur_replica_state.bytes_to_sync) << "All dirty regions should be cleaned by resync";

    // Cleanup expectations
    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
