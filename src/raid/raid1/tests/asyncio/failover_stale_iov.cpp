#include "async_raid1_common.hpp"

// Regression test for stale iovec in the RAID1 failover read path.
//
// In RAID10, RAID0::__distribute populates thread_local sub_cmds and passes iovecs pointing
// into it to RAID1::async_iov. While __failover_read_async is suspended at co_await
// primary_task, a concurrent I/O on the same queue thread can overwrite sub_cmds. The fix
// snapshots iovecs into a frame-local array before the first co_await.
//
// This test simulates the race by mutating the caller-side iov (MockUblksrv's TagState::iov,
// analogous to sub_cmds) after the primary is in-flight. The failover must see the original
// iov_base and iov_len, not the stale values.
TEST_F(AsyncRaid1Fixture, FailoverUsesSnapshotIovecNotStaleCopy) {
    // Thread is required: __select_read_devices uses thread_local last_read for round-robin
    // routing. Running in a fresh thread prevents this test from mutating that state and
    // causing routing surprises in other tests that share the main gtest thread.
    std::thread([this] {
        constexpr size_t k_req_len = 4 * Ki;

        auto* const buf = mock->io_buf(0);
        iovec captured_failover_iov{};

        EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _))
            .WillOnce([&captured_failover_iov](ublksrv_queue const*, ublk_io_data const*, iovec* iov, uint32_t,
                                               uint64_t) -> io_result {
                captured_failover_iov = *iov;
                return 1;
            });

        auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, k_req_len / 512, buf);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        // Simulate sub_cmds overwrite: stomp both iov_base and iov_len on the caller-side iov
        // after the primary is in-flight. Without the snapshot fix, the failover would use these
        // stale values and read into the wrong buffer with the wrong length.
        auto& orig_iov = mock->iov_ref(0);
        orig_iov.iov_base = reinterpret_cast< void* >(static_cast< uintptr_t >(0xdeadbeef));
        orig_iov.iov_len = 128 * Ki;

        // Primary fails (simulating SIGKILL'd AM) -> failover starts synchronously.
        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());

        // Failover must have used the snapshotted iov_base/iov_len, not the stale values.
        EXPECT_EQ(captured_failover_iov.iov_base, buf);
        EXPECT_EQ(captured_failover_iov.iov_len, k_req_len);

        auto completions = mock->inject_cqe(0, static_cast< int >(k_req_len));
        ASSERT_EQ(completions.size(), 1u);
        EXPECT_EQ(completions[0].result, static_cast< int >(k_req_len));
    }).join();
}
