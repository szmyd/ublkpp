#include <atomic>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "ublkpp/lib/cqe_state.hpp"
#include "metrics/ublk_io_metrics.hpp"

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
// Shutdown drain: all_idle() predicate and _device_reset_done CAS protocol
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

TEST(ShutdownDrain, AllIdleFalseUntilBothCountersReachZero) {
    ublkpp::UblkIOMetrics m{"test-shutdown-idle-4"};
    m._queued_reads.fetch_add(1, std::memory_order_relaxed);
    m._queued_writes.fetch_add(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle());
    m._queued_reads.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_FALSE(m.all_idle()); // write still queued
    m._queued_writes.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_TRUE(m.all_idle());
}

// _device_reset_done CAS: the protocol used by both begin_shutdown() and try_drain_device()
// to ensure device.reset() is called exactly once across concurrent queue threads.
TEST(ShutdownDrain, DeviceResetDoneCasWinsOnFirstAttempt) {
    std::atomic< bool > done{false};
    bool expected = false;
    EXPECT_TRUE(done.compare_exchange_strong(expected, true, std::memory_order_acq_rel));
    EXPECT_TRUE(done.load(std::memory_order_relaxed));
}

TEST(ShutdownDrain, DeviceResetDoneCasLosesOnSecondAttempt) {
    std::atomic< bool > done{false};

    bool exp1 = false;
    done.compare_exchange_strong(exp1, true, std::memory_order_acq_rel); // first wins
    EXPECT_TRUE(done.load(std::memory_order_relaxed));

    bool exp2 = false;
    EXPECT_FALSE(done.compare_exchange_strong(exp2, true, std::memory_order_acq_rel)); // second loses
    EXPECT_TRUE(exp2);                                                                 // updated to the current value
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
