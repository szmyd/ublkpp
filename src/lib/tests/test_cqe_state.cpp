#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <coroutine>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublksrv.h>

#include <ublkpp/lib/cqe_state.hpp>

SISL_LOGGING_INIT(ublksrv)

SISL_OPTIONS_ENABLE(logging)

namespace {

// ============================================================================
// Coroutine helpers for testing push_completion with a live waiter
// ============================================================================

struct FireAndForget {
    struct promise_type {
        FireAndForget get_return_object() { return {}; }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() {}
    };
};

// Suspends the coroutine and installs its handle as io->waiter.
struct InstallWaiter {
    ublkpp::async_io* io;
    bool await_ready() noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept { io->waiter = h; }
    void await_resume() noexcept {}
};

static FireAndForget suspend_into_io(ublkpp::async_io* io) { co_await InstallWaiter{io}; }

// ============================================================================
// async_io::ensure
// ============================================================================

TEST(AsyncIo, EnsureCreatesNewState) {
    ublkpp::async_io io{};
    auto* s = io.ensure(ublkpp::sub_cmd_t{1});
    ASSERT_NE(s, nullptr);
    EXPECT_EQ(s->sub_cmd, ublkpp::sub_cmd_t{1});
    EXPECT_EQ(s->result, 0);
    EXPECT_EQ(s->owner, &io);
}

TEST(AsyncIo, EnsureIsIdempotent) {
    ublkpp::async_io io{};
    auto* s1 = io.ensure(ublkpp::sub_cmd_t{5});
    auto* s2 = io.ensure(ublkpp::sub_cmd_t{5});
    EXPECT_EQ(s1, s2);
    EXPECT_EQ(io.pool.size(), 1u);
}

TEST(AsyncIo, EnsureDistinctSubCmds) {
    ublkpp::async_io io{};
    auto* s1 = io.ensure(ublkpp::sub_cmd_t{1});
    auto* s2 = io.ensure(ublkpp::sub_cmd_t{2});
    auto* s3 = io.ensure(ublkpp::sub_cmd_t{3});
    EXPECT_NE(s1, s2);
    EXPECT_NE(s2, s3);
    EXPECT_EQ(io.pool.size(), 3u);
}

TEST(AsyncIo, EnsurePoolAddressStability) {
    ublkpp::async_io io{};
    auto* first = io.ensure(ublkpp::sub_cmd_t{0});
    for (int i = 1; i < 64; ++i)
        io.ensure(ublkpp::sub_cmd_t(i));
    EXPECT_EQ(first, io.ensure(ublkpp::sub_cmd_t{0}));
}

// ============================================================================
// async_io::push_completion
// ============================================================================

TEST(AsyncIo, PushCompletionNoWaiter) {
    ublkpp::async_io io{};
    ublkpp::CqeState s{&io, 42, ublkpp::sub_cmd_t{7}};
    io.push_completion(&s);
    ASSERT_EQ(io.completions.size(), 1u);
    EXPECT_EQ(io.completions.front(), &s);
    EXPECT_FALSE(io.waiter);
}

TEST(AsyncIo, PushCompletionMaintainsFIFO) {
    ublkpp::async_io io{};
    ublkpp::CqeState s1{&io, 1, ublkpp::sub_cmd_t{1}};
    ublkpp::CqeState s2{&io, 2, ublkpp::sub_cmd_t{2}};
    ublkpp::CqeState s3{&io, 3, ublkpp::sub_cmd_t{3}};
    io.push_completion(&s1);
    io.push_completion(&s2);
    io.push_completion(&s3);
    EXPECT_EQ(io.completions[0], &s1);
    EXPECT_EQ(io.completions[1], &s2);
    EXPECT_EQ(io.completions[2], &s3);
}

TEST(AsyncIo, PushCompletionResumesAndClearsWaiter) {
    ublkpp::async_io io{};
    ublkpp::CqeState state{&io, 99, ublkpp::sub_cmd_t{3}};
    suspend_into_io(&io); // starts, suspends, installs io.waiter
    ASSERT_TRUE(io.waiter);
    io.push_completion(&state); // clears waiter, pushes state, resumes coroutine
    EXPECT_FALSE(io.waiter);
    EXPECT_EQ(io.completions.size(), 1u);
    EXPECT_EQ(io.completions.front(), &state);
}

// ============================================================================
// build_cqe_state_data
// ============================================================================

TEST(BuildCqeStateData, RegistersStateInPool) {
    ublkpp::async_io io{};
    ublk_io_data fake{};
    fake.private_data = &io;
    auto const result = ublkpp::build_cqe_state_data(&fake, 3);
    // bit 63 marks it as a target SQE
    EXPECT_NE(result & (1ULL << 63), 0ULL);
    // lower bits decode to the registered CqeState
    auto* state = reinterpret_cast< ublkpp::CqeState* >(result & ~(1ULL << 63));
    ASSERT_EQ(io.pool.size(), 1u);
    EXPECT_EQ(state, &io.pool.front());
    EXPECT_EQ(state->sub_cmd, ublkpp::sub_cmd_t{3});
    EXPECT_EQ(state->owner, &io);
}

TEST(BuildCqeStateData, IsIdempotentForSameSubCmd) {
    ublkpp::async_io io{};
    ublk_io_data fake{};
    fake.private_data = &io;
    ublkpp::build_cqe_state_data(&fake, 5);
    ublkpp::build_cqe_state_data(&fake, 5);
    EXPECT_EQ(io.pool.size(), 1u);
}

TEST(BuildCqeStateData, DifferentSubCmdsGrowPool) {
    ublkpp::async_io io{};
    ublk_io_data fake{};
    fake.private_data = &io;
    ublkpp::build_cqe_state_data(&fake, 1);
    ublkpp::build_cqe_state_data(&fake, 2);
    ublkpp::build_cqe_state_data(&fake, 3);
    EXPECT_EQ(io.pool.size(), 3u);
}

} // anonymous namespace

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
