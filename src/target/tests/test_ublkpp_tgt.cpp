#include <atomic>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "ublkpp/lib/cqe_state.hpp"
#include "ublkpp/lib/ublk_disk.hpp"
#include "ublkpp/target.hpp"
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

namespace {
struct TrackedDisk : ublkpp::ublk_disk {
    bool* destroyed;
    explicit TrackedDisk(bool* flag) : destroyed(flag) {}
    ~TrackedDisk() override { *destroyed = true; }
    std::string id() const noexcept override { return "tracked"; }
};
} // namespace

TEST(ShutdownDrain, BeginShutdownOnIdleSystemResetsDeviceSynchronously) {
    bool destroyed = false;
    auto tgt = ublkpp::ublkpp_tgt::make_for_test(std::make_shared< TrackedDisk >(&destroyed));
    tgt.begin_shutdown();
    EXPECT_TRUE(destroyed) << "device.reset() was not called synchronously by begin_shutdown() on an idle system";
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
