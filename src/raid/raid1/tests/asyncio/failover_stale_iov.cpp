#include "async_raid1_common.hpp"

// Regression test for stale iovec in the RAID1 failover read path.
//
// In RAID10, RAID0::__distribute populates thread_local sub_cmds and passes iovecs pointing
// into it to RAID1::async_iov. While __failover_read_async is suspended at co_await
// primary_task, a concurrent I/O on the same queue thread can overwrite sub_cmds, leaving
// iovecs stale when the failover SQE is submitted.
//
// The fix snapshots iovecs into a coroutine-frame-local copy before the first co_await so
// the failover always uses the original values. This test simulates the race by mutating the
// caller-side iov after the primary is in-flight and verifying the failover still sees the
// original iov_len.
TEST_F(AsyncRaid1Fixture, FailoverUsesSnapshotIovecNotStaleCopy) {
    std::thread([this] {
        constexpr size_t k_req_len = 4 * Ki;
        constexpr size_t k_stale_len = 128 * Ki;

        // Capture the iov_len that disk_b's submit_iov receives during the failover.
        size_t failover_iov_len = 0;
        EXPECT_CALL(*disk_b, submit_iov(_, _, _, _, _))
            .WillOnce([&failover_iov_len](ublksrv_queue const*, ublk_io_data const*, iovec* iov, uint32_t,
                                          uint64_t) -> io_result {
                failover_iov_len = iov->iov_len;
                return 1;
            });

        auto res = mock->submit_io(0, UBLK_IO_OP_READ, 0, k_req_len / 512, nullptr);
        ASSERT_TRUE(res);
        EXPECT_EQ(res.value(), 1u);

        // Simulate concurrent I/O overwriting thread_local sub_cmds: mutate the
        // caller-side iov_len after the primary is in-flight. Without the snapshot fix,
        // the failover would use this stale value; with the fix it sees the original 4 KiB.
        mock->iov_ref(0).iov_len = k_stale_len;

        // Primary fails (simulating SIGKILL'd AM) -> failover starts synchronously.
        EXPECT_TRUE(mock->inject_cqe(0, -EIO).empty());

        // Failover must have used the snapshotted iov_len (4 KiB), not the stale 128 KiB.
        EXPECT_EQ(failover_iov_len, k_req_len);

        auto completions = mock->inject_cqe(0, static_cast< int >(k_req_len));
        ASSERT_EQ(completions.size(), 1u);
        EXPECT_EQ(completions[0].result, static_cast< int >(k_req_len));
    }).join();
}
