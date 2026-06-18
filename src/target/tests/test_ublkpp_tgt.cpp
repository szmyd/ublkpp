#include <atomic>
#include <chrono>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "ublkpp/lib/cqe_state.hpp"
#include "ublkpp/lib/ublk_disk.hpp"
#include "ublkpp/target_testing.hpp"
#include "metrics/ublk_raid_metrics.hpp"

SISL_LOGGING_INIT(ublk_tgt)

SISL_OPTIONS_ENABLE(logging)

// ---------------------------------------------------------------------------
// TrackedDisk: minimal ublk_disk whose destructor records that device = {}
// fired. Used to verify begin_shutdown() / wait_for_drain() drain behaviour
// without kernel infrastructure (queue threads, ublksrv handshake, etc.).
// ---------------------------------------------------------------------------
struct TrackedDisk : ublkpp::ublk_disk {
    std::atomic< int >& _destroy_count;
    explicit TrackedDisk(std::atomic< int >& counter) : _destroy_count(counter) {}
    ~TrackedDisk() override { _destroy_count.fetch_add(1, std::memory_order_relaxed); }
    std::string id() const noexcept override { return "test-tracked-disk"; }
};

TEST(cqe_state, NextStateAllocatesDistinctStates) {
    ublkpp::async_io io{};
    io._pool.reserve(2);
    auto* s1 = io.next_state();
    auto* s2 = io.next_state();
    EXPECT_NE(s1, s2);
    EXPECT_EQ(s1->_owner, &io);
    EXPECT_EQ(s2->_owner, &io);
    EXPECT_EQ(io._pool.size(), 2u);
}

TEST(cqe_state, BuildCqeStateDataEncodesTargetBit) {
    ublkpp::async_io io{};
    io._pool.reserve(1);
    ublk_io_data fake{};
    fake.private_data = &io;
    auto const [state, user_data] = ublkpp::build_cqe_state_data(&fake);
    // bit 63 is the target-io marker checked by run_queue_loop
    EXPECT_NE(user_data & (1ULL << 63), 0ULL);
    auto* decoded = reinterpret_cast< ublkpp::cqe_state* >(user_data & ~(1ULL << 63));
    EXPECT_EQ(state, decoded);
    EXPECT_EQ(state->_owner, &io);
}

// ---------------------------------------------------------------------------
// Shutdown drain: all_idle() predicate
// ---------------------------------------------------------------------------

TEST(ShutdownDrain, AllIdleTrueInitially) {
    ublkpp::UblkIOMetrics m{"test-shutdown-idle-1"};
    EXPECT_TRUE(m.all_idle());
}

TEST(ShutdownDrain, AllIdleFalseWhenReadQueued) {
    ublkpp::UblkIOMetrics m{"test-shutdown-idle-2"};
    m._queued_reads.fetch_add(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle());
}

TEST(ShutdownDrain, AllIdleFalseWhenWriteQueued) {
    ublkpp::UblkIOMetrics m{"test-shutdown-idle-3"};
    m._queued_writes.fetch_add(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle());
}

TEST(ShutdownDrain, AllIdleFalseWhenOtherQueued) {
    ublkpp::UblkIOMetrics m{"test-shutdown-idle-4"};
    m._queued_other.fetch_add(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle());
    m._queued_other.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_TRUE(m.all_idle());
}

TEST(ShutdownDrain, AllIdleFalseUntilAllCountersReachZero) {
    ublkpp::UblkIOMetrics m{"test-shutdown-idle-5"};
    m._queued_reads.fetch_add(1, std::memory_order_relaxed);
    m._queued_writes.fetch_add(1, std::memory_order_relaxed);
    m._queued_other.fetch_add(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle());
    m._queued_reads.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle());
    m._queued_writes.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle()); // _queued_other still live
    m._queued_other.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_TRUE(m.all_idle());
}

// ---------------------------------------------------------------------------
// Counter partitioning: FLUSH (op=2) has no counter; DISCARD (op=3) and
// WRITE_ZEROES (op=5) use _queued_other so all_idle() gates on them.
// Counters are manipulated directly (matching the existing AllIdleFalse*
// tests) because record_queue_depth_change requires a live ublksrv_queue.
// ---------------------------------------------------------------------------

TEST(ShutdownDrain, SingleReadCounterGatesAllIdle) {
    // A single in-flight read prevents all_idle() from returning true; decrementing
    // it allows drain to proceed. FLUSH ops have no counter and need no drain step.
    ublkpp::UblkIOMetrics m{"test-flush-no-counter"};
    m._queued_reads.fetch_add(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle());
    m._queued_reads.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_TRUE(m.all_idle()); // FLUSH would need to drain here if it had a counter; it doesn't
}

TEST(ShutdownDrain, DiscardAndWriteZeroesAreTrackedInOtherCounter) {
    // DISCARD and WRITE_ZEROES access device* via async_iov just like reads/writes.
    // They must be counted in _queued_other so all_idle() → device = {} only
    // fires when no coroutine is suspended at co_await device->async_iov.
    ublkpp::UblkIOMetrics m{"test-other-counter"};

    // Simulate a DISCARD in-flight
    m._queued_other.fetch_add(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle()); // device still in use
    m._queued_other.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_TRUE(m.all_idle());

    // Simulate a WRITE_ZEROES in-flight alongside a concurrent read
    m._queued_reads.fetch_add(1, std::memory_order_relaxed);
    m._queued_other.fetch_add(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle());
    m._queued_reads.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle()); // WRITE_ZEROES still live
    m._queued_other.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_TRUE(m.all_idle());
}

// ---------------------------------------------------------------------------
// begin_shutdown() / wait_for_drain() (no kernel infra)
// make_for_test() is the public factory; tests that also need metrics() or
// try_drain() use ublkpp_tgt_test_peer directly.
// ---------------------------------------------------------------------------

TEST(ShutdownDrain, BeginShutdownOnIdleSystemResetsDeviceSynchronously) {
    std::atomic< int > destroy_count{0};
    auto disk = std::make_shared< TrackedDisk >(destroy_count);
    auto tgt = ublkpp::ublkpp_tgt_test_peer::make(disk);
    disk.reset(); // release local ref; only tgt->device holds a reference now
    EXPECT_EQ(destroy_count.load(), 0);
    tgt.begin_shutdown();
    // idle path: no in-flight ops → device = {} fires synchronously inside begin_shutdown
    EXPECT_EQ(destroy_count.load(), 1);
}

TEST(ShutdownDrain, WaitForDrainReturnsImmediatelyAfterIdleShutdown) {
    std::atomic< int > destroy_count{0};
    auto disk = std::make_shared< TrackedDisk >(destroy_count);
    auto tgt = ublkpp::ublkpp_tgt_test_peer::make(std::move(disk));
    tgt.begin_shutdown();
    // _drain_complete was set synchronously by the idle path — wait() must not block.
    tgt.wait_for_drain();
    EXPECT_EQ(destroy_count.load(), 1);
}

TEST(ShutdownDrainDeathTest, WaitForDrainWithoutBeginShutdownAsserts) {
    // Without begin_shutdown(), _drain_complete is never set and wait_for_drain()
    // would block forever. The RELEASE_ASSERT detects this misuse and aborts.
    std::atomic< int > destroy_count{0};
    auto tgt = ublkpp::ublkpp_tgt_test_peer::make(std::make_shared< TrackedDisk >(destroy_count));
    // RELEASE_ASSERT routes its message through SISL's spdlog logger, not stderr,
    // so we can't match the message text. Verify the process aborts (any death).
    ASSERT_DEATH(tgt.wait_for_drain(), "");
}

TEST(ShutdownDrain, NonIdlePathFiresDeviceResetWhenLastOpCompletes) {
    // Exercises try_drain(): the non-idle drain path normally reached via __handle_io_async()
    // which requires kernel infrastructure. Here we simulate the path directly:
    //   1. increment counter → system appears non-idle
    //   2. begin_shutdown() → sees counter > 0, skips device = {}, returns immediately
    //   3. decrement counter → simulate op completion
    //   4. try_drain() → the same check queue threads perform; should fire device = {}
    std::atomic< int > destroy_count{0};
    auto disk = std::make_shared< TrackedDisk >(destroy_count);
    auto tgt = ublkpp::ublkpp_tgt_test_peer::make(disk);
    disk.reset(); // release local ref; only tgt->device holds the last reference

    // Simulate one in-flight op
    ublkpp::ublkpp_tgt_test_peer::metrics(tgt)._queued_reads.fetch_add(1, std::memory_order_relaxed);
    tgt.begin_shutdown(); // non-idle: skips device = {}
    EXPECT_EQ(destroy_count.load(), 0) << "device = {} should not fire while op is in-flight";

    // Op completes: decrement counter then trigger the drain check
    ublkpp::ublkpp_tgt_test_peer::metrics(tgt)._queued_reads.fetch_sub(1, std::memory_order_seq_cst);
    ublkpp::ublkpp_tgt_test_peer::try_drain(tgt);
    EXPECT_EQ(destroy_count.load(), 1) << "device = {} should fire when last in-flight op completes";
}

TEST(ShutdownDrain, WaitForDrainBlocksUntilNonIdleDrainCompletes) {
    // End-to-end non-idle path: begin_shutdown() returns immediately (non-idle), then
    // wait_for_drain() must block until a second thread fires the drain via try_drain().
    // The drainer sleeps briefly to allow wait_for_drain() to enter its wait before signaling
    // _drain_complete. If wait_for_drain() returned early (before drain), destroy_count would
    // be 0 on the EXPECT below.
    std::atomic< int > destroy_count{0};
    auto disk = std::make_shared< TrackedDisk >(destroy_count);
    auto tgt = ublkpp::ublkpp_tgt_test_peer::make(disk);
    disk.reset();

    auto& m = ublkpp::ublkpp_tgt_test_peer::metrics(tgt);
    m._queued_reads.fetch_add(1, std::memory_order_relaxed);
    tgt.begin_shutdown(); // non-idle: _drain_complete not yet signaled

    std::thread drainer{[&] {
        // Sleep to let wait_for_drain() enter the condvar wait before we signal it.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        m._queued_reads.fetch_sub(1, std::memory_order_seq_cst);
        ublkpp::ublkpp_tgt_test_peer::try_drain(tgt); // signals _drain_complete
    }};

    tgt.wait_for_drain(); // must block until drainer calls try_drain()
    EXPECT_EQ(destroy_count.load(), 1);
    drainer.join();
}

TEST(ShutdownDrain, BeginShutdownIdempotentDoesNotDoubleReset) {
    std::atomic< int > destroy_count{0};
    auto disk = std::make_shared< TrackedDisk >(destroy_count);
    auto tgt = ublkpp::ublkpp_tgt_test_peer::make(disk);
    disk.reset();
    tgt.begin_shutdown();
    tgt.begin_shutdown(); // idempotent: _shutting_down guard short-circuits; CAS is not re-entered
    tgt.begin_shutdown();
    EXPECT_EQ(destroy_count.load(), 1); // device = {} called exactly once
}

TEST(ShutdownDrain, ConcurrentTryDrainFiresDeviceResetExactlyOnce) {
    // Two threads simultaneously decrement counters and call try_drain(). The CAS in
    // try_drain() guarantees the device = {} fires exactly once regardless of interleaving.
    std::atomic< int > destroy_count{0};
    auto disk = std::make_shared< TrackedDisk >(destroy_count);
    auto tgt = ublkpp::ublkpp_tgt_test_peer::make(disk);
    disk.reset();

    auto& m = ublkpp::ublkpp_tgt_test_peer::metrics(tgt);
    m._queued_reads.fetch_add(1, std::memory_order_relaxed);
    m._queued_writes.fetch_add(1, std::memory_order_relaxed);
    tgt.begin_shutdown();
    ASSERT_EQ(destroy_count.load(), 0);

    std::atomic< int > ready{0};
    std::atomic< bool > go{false};
    auto decrement_and_drain = [&](std::atomic< uint64_t >& counter) {
        ready.fetch_add(1, std::memory_order_release);
        while (!go.load(std::memory_order_acquire)) {}
        counter.fetch_sub(1, std::memory_order_seq_cst);
        ublkpp::ublkpp_tgt_test_peer::try_drain(tgt);
    };

    std::thread t1{decrement_and_drain, std::ref(m._queued_reads)};
    std::thread t2{decrement_and_drain, std::ref(m._queued_writes)};
    while (ready.load(std::memory_order_acquire) < 2) {}
    go.store(true, std::memory_order_release);
    t1.join();
    t2.join();

    EXPECT_EQ(destroy_count.load(), 1);
}

// ---------------------------------------------------------------------------
// apply_op_for_test: verifies op → counter routing without a live queue
// ---------------------------------------------------------------------------

TEST(ApplyOpForTest, ReadOpIncrementsAndDecrementsQueuedReads) {
    ublkpp::UblkIOMetrics m{"test-apply-op-read"};
    EXPECT_EQ(m._queued_reads.load(), 0u);
    m.apply_op_for_test(0, true);
    EXPECT_EQ(m._queued_reads.load(), 1u);
    EXPECT_EQ(m._queued_writes.load(), 0u);
    EXPECT_EQ(m._queued_other.load(), 0u);
    m.apply_op_for_test(0, false);
    EXPECT_EQ(m._queued_reads.load(), 0u);
}

TEST(ApplyOpForTest, WriteOpIncrementsAndDecrementsQueuedWrites) {
    ublkpp::UblkIOMetrics m{"test-apply-op-write"};
    m.apply_op_for_test(1, true);
    EXPECT_EQ(m._queued_writes.load(), 1u);
    EXPECT_EQ(m._queued_reads.load(), 0u);
    EXPECT_EQ(m._queued_other.load(), 0u);
    m.apply_op_for_test(1, false);
    EXPECT_EQ(m._queued_writes.load(), 0u);
}

TEST(ApplyOpForTest, DiscardOpUsesOtherCounter) {
    ublkpp::UblkIOMetrics m{"test-apply-op-discard"};
    m.apply_op_for_test(3, true); // UBLK_IO_OP_DISCARD
    EXPECT_EQ(m._queued_other.load(), 1u);
    EXPECT_EQ(m._queued_reads.load(), 0u);
    EXPECT_EQ(m._queued_writes.load(), 0u);
    m.apply_op_for_test(3, false);
    EXPECT_EQ(m._queued_other.load(), 0u);
}

TEST(ApplyOpForTest, WriteZeroesOpUsesOtherCounter) {
    ublkpp::UblkIOMetrics m{"test-apply-op-write-zeroes"};
    m.apply_op_for_test(5, true); // UBLK_IO_OP_WRITE_ZEROES
    EXPECT_EQ(m._queued_other.load(), 1u);
    m.apply_op_for_test(5, false);
    EXPECT_EQ(m._queued_other.load(), 0u);
}

TEST(ApplyOpForTest, FlushOpDoesNotTouchAnyCounter) {
    ublkpp::UblkIOMetrics m{"test-apply-op-flush"};
    m.apply_op_for_test(2, true); // UBLK_IO_OP_FLUSH — no counter
    EXPECT_EQ(m._queued_reads.load(), 0u);
    EXPECT_EQ(m._queued_writes.load(), 0u);
    EXPECT_EQ(m._queued_other.load(), 0u);
    EXPECT_TRUE(m.all_idle());
}

// ---------------------------------------------------------------------------
// record_io_bytes: byte counter accumulation
// ---------------------------------------------------------------------------

TEST(IOBytes, ReadBytesAccumulate) {
    ublkpp::UblkIOMetrics m{"test-bytes-read"};
    m.record_io_bytes(0, 4096);
    m.record_io_bytes(0, 8192);
    EXPECT_EQ(m._read_bytes_total.load(std::memory_order_relaxed), 12288u);
    EXPECT_EQ(m._write_bytes_total.load(std::memory_order_relaxed), 0u);
}

TEST(IOBytes, WriteBytesAccumulate) {
    ublkpp::UblkIOMetrics m{"test-bytes-write"};
    m.record_io_bytes(1, 16384);
    EXPECT_EQ(m._write_bytes_total.load(std::memory_order_relaxed), 16384u);
    EXPECT_EQ(m._read_bytes_total.load(std::memory_order_relaxed), 0u);
}

TEST(IOBytes, NonDataOpIgnored) {
    ublkpp::UblkIOMetrics m{"test-bytes-flush"};
    m.record_io_bytes(2, 512); // op=2 (FLUSH), not 0 or 1
    EXPECT_EQ(m._read_bytes_total.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(m._write_bytes_total.load(std::memory_order_relaxed), 0u);
}

TEST(IOBytes, DiscardAndWriteZeroesNotCounted) {
    // __handle_io_async calls record_io_bytes for all non-FLUSH ops (including DISCARD=3 and
    // WRITE_ZEROES=5). The function must silently ignore them — only READ/WRITE move bytes.
    ublkpp::UblkIOMetrics m{"test-bytes-discard"};
    m.record_io_bytes(3, 4096); // UBLK_IO_OP_DISCARD
    m.record_io_bytes(5, 4096); // UBLK_IO_OP_WRITE_ZEROES
    EXPECT_EQ(m._read_bytes_total.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(m._write_bytes_total.load(std::memory_order_relaxed), 0u);
}

// ---------------------------------------------------------------------------
// record_io_latency: dispatches to read or write histogram; flush is ignored.
// SISL histograms have no shadow atomic so EXPECT_NO_THROW is the only
// observable here — the bytes/error tests above use atomics instead.
// ---------------------------------------------------------------------------

TEST(IOLatency, ReadLatencyObserved) {
    ublkpp::UblkIOMetrics m{"test-latency-read"};
    EXPECT_NO_THROW(m.record_io_latency(0, 1000));
    EXPECT_NO_THROW(m.record_io_latency(0, 512));
}

TEST(IOLatency, WriteLatencyObserved) {
    ublkpp::UblkIOMetrics m{"test-latency-write"};
    EXPECT_NO_THROW(m.record_io_latency(1, 2048));
}

TEST(IOLatency, FlushOpIgnored) {
    ublkpp::UblkIOMetrics m{"test-latency-flush"};
    EXPECT_NO_THROW(m.record_io_latency(2, 9999)); // op=2 (FLUSH) — no histogram to observe
}

// ---------------------------------------------------------------------------
// record_io_error: error counters accumulate per op type
// ---------------------------------------------------------------------------

TEST(IOError, ReadErrorAccumulates) {
    ublkpp::UblkIOMetrics m{"test-error-read"};
    m.record_io_error(0);
    m.record_io_error(0);
    EXPECT_EQ(m._read_errors.load(std::memory_order_relaxed), 2u);
    EXPECT_EQ(m._write_errors.load(std::memory_order_relaxed), 0u);
}

TEST(IOError, WriteErrorAccumulates) {
    ublkpp::UblkIOMetrics m{"test-error-write"};
    m.record_io_error(1);
    EXPECT_EQ(m._write_errors.load(std::memory_order_relaxed), 1u);
    EXPECT_EQ(m._read_errors.load(std::memory_order_relaxed), 0u);
}

TEST(IOError, FlushOpIgnored) {
    ublkpp::UblkIOMetrics m{"test-error-flush"};
    m.record_io_error(2); // op=2 (FLUSH), not counted
    EXPECT_EQ(m._read_errors.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(m._write_errors.load(std::memory_order_relaxed), 0u);
}

TEST(IOError, DiscardAndWriteZeroesNotCounted) {
    // __handle_io_async calls record_io_error for all non-FLUSH ops on failure (including
    // DISCARD=3 and WRITE_ZEROES=5). The function must silently ignore them — errors are
    // only attributed to READ/WRITE, matching the byte counters.
    ublkpp::UblkIOMetrics m{"test-error-discard"};
    m.record_io_error(3); // UBLK_IO_OP_DISCARD
    m.record_io_error(5); // UBLK_IO_OP_WRITE_ZEROES
    EXPECT_EQ(m._read_errors.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(m._write_errors.load(std::memory_order_relaxed), 0u);
}

// ---------------------------------------------------------------------------
// UblkRaidMetrics: smoke tests for new methods.
// SISL gauges have no readable back-channel, so EXPECT_NO_THROW verifies
// dispatch without value verification — same constraint as the latency tests above.
// ---------------------------------------------------------------------------

TEST(RaidMetrics, RecordDegradedStateDoesNotThrow) {
    ublkpp::UblkRaidMetrics m{"test-parent", "test-raid-degraded"};
    EXPECT_NO_THROW(m.record_degraded_state(true));
    EXPECT_NO_THROW(m.record_degraded_state(false));
    EXPECT_NO_THROW(m.record_degraded_state(true)); // toggle is idempotent
}

TEST(RaidMetrics, RecordResyncInitialSizeDoesNotThrow) {
    ublkpp::UblkRaidMetrics m{"test-parent", "test-raid-initial-size"};
    EXPECT_NO_THROW(m.record_resync_initial_size(1024 * 1024)); // 1 MiB at resync start
    EXPECT_NO_THROW(m.record_resync_initial_size(0));           // clear when resync completes
}

TEST(RaidMetrics, RecordDirtyPagesWithRemainingBytesDoesNotThrow) {
    ublkpp::UblkRaidMetrics m{"test-parent", "test-raid-dirty-pages"};
    EXPECT_NO_THROW(m.record_dirty_pages(10, 10 * 32 * 1024)); // 10 pages × 32 KiB chunks
    EXPECT_NO_THROW(m.record_dirty_pages(0, 0));               // all clean after resync
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    // Death tests use fork() which is unsafe in a multi-threaded process. "threadsafe"
    // mode re-execs the binary instead, giving the child a clean single-threaded start.
    ::testing::GTEST_FLAG(death_test_style) = "threadsafe";
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
