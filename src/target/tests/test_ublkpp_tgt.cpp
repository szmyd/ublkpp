#include <atomic>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "ublkpp/lib/cqe_state.hpp"
#include "ublkpp/lib/ublk_disk.hpp"
#include "ublkpp/target_testing.hpp"

SISL_LOGGING_INIT(ublk_tgt)

SISL_OPTIONS_ENABLE(logging)

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
// TrackedDisk: minimal ublk_disk whose destructor records that device.reset()
// fired. Used to verify begin_shutdown() / wait_for_drain() drain behaviour
// without kernel infrastructure (queue threads, ublksrv handshake, etc.).
// ---------------------------------------------------------------------------
struct TrackedDisk : ublkpp::ublk_disk {
    std::atomic< int >& _destroy_count;
    explicit TrackedDisk(std::atomic< int >& counter) : _destroy_count(counter) {}
    ~TrackedDisk() override { _destroy_count.fetch_add(1, std::memory_order_relaxed); }
    std::string id() const noexcept override { return "test-tracked-disk"; }
};

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

TEST(ShutdownDrain, FlushOpHasNoCounter) {
    // FLUSH completes instantly (result=0) and never dereferences device*, so it
    // must not participate in any queue-depth counter. Verify that no amount of
    // read/write/other activity prevents all_idle() from returning true once those
    // counters drain — and that the missing FLUSH counter is not a false-idle path.
    ublkpp::UblkIOMetrics m{"test-flush-no-counter"};
    m._queued_reads.fetch_add(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle());
    m._queued_reads.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_TRUE(m.all_idle()); // FLUSH would need to drain here if it had a counter; it doesn't
}

TEST(ShutdownDrain, DiscardAndWriteZeroesAreTrackedInOtherCounter) {
    // DISCARD and WRITE_ZEROES access device* via async_iov just like reads/writes.
    // They must be counted in _queued_other so all_idle() → device.reset() only
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
// begin_shutdown() / wait_for_drain() via make_for_test (no kernel infra)
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

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
