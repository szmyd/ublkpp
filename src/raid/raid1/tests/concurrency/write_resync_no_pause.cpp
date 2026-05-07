#include "test_raid1_common.hpp"

#include <atomic>
#include <future>
#include <thread>

using namespace std::chrono_literals;

// Sets up standard resync I/O expectations on both devices.
// device_a is the clean mirror (source of resync reads).
// device_b is the dirty mirror (target of resync writes + bitmap page writes).
static void expect_resync_io(std::shared_ptr< ublkpp::TestDisk >& device_a,
                             std::shared_ptr< ublkpp::TestDisk >& device_b,
                             std::atomic< int >* resync_reads_out = nullptr,
                             std::atomic< int >* resync_writes_out = nullptr) {
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([resync_reads_out](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            if (resync_reads_out) resync_reads_out->fetch_add(1, std::memory_order_relaxed);
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([resync_writes_out](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            if (resync_writes_out) resync_writes_out->fetch_add(1, std::memory_order_relaxed);
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                           uint64_t) -> io_result { return 1; });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                           uint64_t) -> io_result { return 1; });
}

// Completes an in-flight write io_data, sending the right number of on_io_complete
// calls based on how many async writes were queued (pending_cnt from handle_rw).
static void complete_write(ublkpp::Raid1Disk& dev, ublk_io_data& io, int pending_cnt) {
    dev.on_io_complete(&io, 0b100, 0);                       // primary (device_a) completion
    if (pending_cnt >= 2) dev.on_io_complete(&io, 0b101, 0); // replica (device_b) completion
}

// Verify that a write to an unrelated LBA does not block resync of a different dirty region.
// Old mechanism: ANY write paused ALL resync.
// New mechanism: only conflicting LBA ranges are skipped.
TEST(Raid1, UnrelatedWriteDoesNotBlockResync) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Step 1: Dirty LBA 0 by failing device_b's async write
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                         uint64_t) -> io_result { return 1; });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce(
                [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                   uint64_t) -> io_result { return std::unexpected(std::make_error_condition(std::errc::io_error)); });
        EXPECT_TO_WRITE_SB(device_a);

        auto io = make_io_data(UBLK_IO_OP_WRITE, 32 * Ki, 0UL);
        ASSERT_TRUE(raid_device.handle_rw(nullptr, &io, 0b10, nullptr, 32 * Ki, 0UL));
        raid_device.on_io_complete(&io, 0b100, 0);
        remove_io_data(io);
    }

    ASSERT_GT(raid_device.replica_states().bytes_to_sync, 0U);

    // Step 2: Restore device_b
    {
        auto io = make_io_data(UBLK_IO_OP_READ);
        ASSERT_TRUE(raid_device.queue_internal_resp(nullptr, &io,
                                                    ublkpp::set_flags(0b101, ublkpp::sub_cmd_flags::INTERNAL), 0));
        remove_io_data(io);
    }

    // Step 3: Set up resync and concurrent write expectations
    std::atomic< int > resync_reads{0};
    expect_resync_io(device_a, device_b, &resync_reads, nullptr);

    // Step 4: Issue a write to a non-overlapping LBA (512 MiB away from dirty LBA 0).
    // handle_rw calls enqueue_write; we hold it in-flight by not calling on_io_complete yet.
    constexpr uint64_t k_unrelated_lba = 512 * Mi;
    auto inflight_io = make_io_data(UBLK_IO_OP_WRITE, 32 * Ki, k_unrelated_lba);
    auto const pending = raid_device.handle_rw(nullptr, &inflight_io, 0b10, nullptr, 32 * Ki, k_unrelated_lba);
    ASSERT_TRUE(pending);

    // Step 5: Start resync — it must copy LBA 0 despite the in-flight write at 512 MiB
    raid_device.toggle_resync(true);

    EXPECT_TRUE(wait_for_clean_state(raid_device, 2000ms))
        << "Resync must complete even with an unrelated write held in-flight";

    EXPECT_GE(resync_reads.load(), 1) << "Resync should have performed at least one read";

    // Step 6: Release the in-flight write now that resync has finished
    complete_write(raid_device, inflight_io, pending.value());
    remove_io_data(inflight_io);

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Verify that resync skips a conflicting in-flight write (bitmap stays dirty),
// and successfully copies the region after the write completes.
TEST(Raid1, ConflictingWriteSkipped_ResyncRetries) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Step 1: Dirty LBA 0
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                         uint64_t) -> io_result { return 1; });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce(
                [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                   uint64_t) -> io_result { return std::unexpected(std::make_error_condition(std::errc::io_error)); });
        EXPECT_TO_WRITE_SB(device_a);

        auto io = make_io_data(UBLK_IO_OP_WRITE, 32 * Ki, 0UL);
        ASSERT_TRUE(raid_device.handle_rw(nullptr, &io, 0b10, nullptr, 32 * Ki, 0UL));
        raid_device.on_io_complete(&io, 0b100, 0);
        remove_io_data(io);
    }

    ASSERT_GT(raid_device.replica_states().bytes_to_sync, 0U);

    // Step 2: Restore device_b
    {
        auto io = make_io_data(UBLK_IO_OP_READ);
        ASSERT_TRUE(raid_device.queue_internal_resp(nullptr, &io,
                                                    ublkpp::set_flags(0b101, ublkpp::sub_cmd_flags::INTERNAL), 0));
        remove_io_data(io);
    }

    // Step 3: Set up resync expectations, tracking whether resync actually reads from device_a
    std::atomic< int > resync_reads{0};
    expect_resync_io(device_a, device_b, &resync_reads, nullptr);

    // Step 4: Issue a write overlapping LBA 0 (the dirty region) and hold it in-flight.
    // This calls enqueue_write internally; dequeue_write is only called on on_io_complete.
    auto conflict_io = make_io_data(UBLK_IO_OP_WRITE, 32 * Ki, 0UL);
    auto const pending = raid_device.handle_rw(nullptr, &conflict_io, 0b10, nullptr, 32 * Ki, 0UL);
    ASSERT_TRUE(pending);

    // Step 5: Start resync — it should detect the conflict and skip LBA 0 every sweep
    raid_device.toggle_resync(true);

    // Give resync several yield cycles to attempt copying — it must skip each time
    std::this_thread::sleep_for(300ms);

    // Resync must not have performed any reads (Phase 1 conflict check skips before reading)
    EXPECT_EQ(0, resync_reads.load()) << "Resync must not read the conflicting region while write is in-flight";
    EXPECT_GT(raid_device.replica_states().bytes_to_sync, 0U)
        << "Bitmap must remain dirty while the conflicting write is in-flight";

    // Step 6: Complete the write — dequeue_write is called, resync can now proceed
    complete_write(raid_device, conflict_io, pending.value());
    remove_io_data(conflict_io);

    // Resync should now copy the dirty region (or the write path's handle_internal cleans it)
    EXPECT_TRUE(wait_for_clean_state(raid_device, 2000ms))
        << "State must become clean after the conflicting write is dequeued";

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}

// Verify that Phase 2 (post-copy conflict check) detects a write that arrives AFTER Phase 1
// passes but BEFORE clean_region is called, and correctly keeps the bitmap dirty.
//
// Sequence:
//   Phase 1 check: no write in-flight → resync proceeds, begins copy READ
//   [test thread registers a write while READ is in progress]
//   Copy WRITE completes → Phase 2 detects the in-flight write → skip clean_region
//   → bitmap stays dirty until the write completes → resync re-copies → clean
TEST(Raid1, Phase2ConflictDetectedAfterCopy) {
    auto device_a = CREATE_DISK_A((TestParams{.capacity = Gi, .id = "DiskA"}));
    auto device_b = CREATE_DISK_B((TestParams{.capacity = Gi, .id = "DiskB"}));
    auto raid_device = ublkpp::Raid1Disk(boost::uuids::string_generator()(test_uuid), device_a, device_b);
    raid_device.toggle_resync(false);

    // Step 1: Dirty LBA 0 by failing device_b's async write
    {
        EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                         uint64_t) -> io_result { return 1; });
        EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
            .Times(1)
            .WillOnce(
                [](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                   uint64_t) -> io_result { return std::unexpected(std::make_error_condition(std::errc::io_error)); });
        EXPECT_TO_WRITE_SB(device_a);

        auto io = make_io_data(UBLK_IO_OP_WRITE, 32 * Ki, 0UL);
        ASSERT_TRUE(raid_device.handle_rw(nullptr, &io, 0b10, nullptr, 32 * Ki, 0UL));
        raid_device.on_io_complete(&io, 0b100, 0);
        remove_io_data(io);
    }

    ASSERT_GT(raid_device.replica_states().bytes_to_sync, 0U);

    // Step 2: Restore device_b
    {
        auto io = make_io_data(UBLK_IO_OP_READ);
        ASSERT_TRUE(raid_device.queue_internal_resp(nullptr, &io,
                                                    ublkpp::set_flags(0b101, ublkpp::sub_cmd_flags::INTERNAL), 0));
        remove_io_data(io);
    }

    // Step 3: Set up mock so the FIRST resync READ of LBA 0 signals that Phase 1 has
    // passed, then blocks until the test thread registers a write for the same region.
    std::promise< void > copy_read_started;
    std::promise< void > write_registered;

    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillOnce([&](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            copy_read_started.set_value();
            write_registered.get_future().wait();
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        })
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            if (iovecs->iov_base) memset(iovecs->iov_base, 0xAA, iovecs->iov_len);
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_a, sync_iov(UBLK_IO_OP_WRITE, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_b, sync_iov(UBLK_IO_OP_READ, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t) -> io_result {
            return ublkpp::__iovec_len(iovecs, iovecs + nr_vecs);
        });
    EXPECT_CALL(*device_a, async_iov(_, _, _, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                           uint64_t) -> io_result { return 1; });
    EXPECT_CALL(*device_b, async_iov(_, _, _, _, _, _))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([](ublksrv_queue const*, ublk_io_data const*, ublkpp::sub_cmd_t, iovec*, uint32_t,
                           uint64_t) -> io_result { return 1; });

    // Step 4: Start resync — Phase 1 sees no write registered and proceeds into the copy
    raid_device.toggle_resync(true);

    // Step 5: Wait for resync to start reading LBA 0 (Phase 1 has already passed)
    copy_read_started.get_future().wait();

    // Step 6: Register a write to LBA 0 AFTER Phase 1 passed; Phase 2 must catch it
    auto conflict_io = make_io_data(UBLK_IO_OP_WRITE, 32 * Ki, 0UL);
    auto const pending = raid_device.handle_rw(nullptr, &conflict_io, 0b10, nullptr, 32 * Ki, 0UL);
    ASSERT_TRUE(pending);

    // Step 7: Unblock the resync read — Phase 2 will detect the in-flight write and skip
    // clean_region, leaving the bitmap dirty
    write_registered.set_value();

    // Step 8: Bitmap must remain dirty while the write is still in-flight (Phase 1 also
    // blocks all subsequent sweeps until dequeue_write fires)
    std::this_thread::sleep_for(300ms);
    EXPECT_GT(raid_device.replica_states().bytes_to_sync, 0U)
        << "Bitmap must remain dirty while Phase-2-detected write is in-flight";

    // Step 9: Complete the write — dequeue_write unregisters from the region tracker
    complete_write(raid_device, conflict_io, pending.value());
    remove_io_data(conflict_io);

    // Resync can now copy the dirty region and mark it clean
    EXPECT_TRUE(wait_for_clean_state(raid_device, 2000ms))
        << "State must become clean after the Phase-2 write completes";

    EXPECT_TO_WRITE_SB(device_a);
    EXPECT_TO_WRITE_SB(device_b);
}
