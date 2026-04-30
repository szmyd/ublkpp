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
    EXPECT_EQ(s->owner, &io);
    EXPECT_EQ(s->result, 0);
    EXPECT_FALSE(s->result_ready);
    EXPECT_FALSE(s->waiter);
    EXPECT_EQ(s->sub_cmd, ublkpp::sub_cmd_t{1});
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
    ublkpp::CqeState s{.owner = &io, .result = 42, .sub_cmd = ublkpp::sub_cmd_t{7}};
    io.push_completion(&s);
    ASSERT_EQ(io.completions.size(), 1u);
    EXPECT_EQ(io.completions.front(), &s);
    EXPECT_FALSE(io.waiter);
}

TEST(AsyncIo, PushCompletionMaintainsFIFO) {
    ublkpp::async_io io{};
    ublkpp::CqeState s1{.owner = &io, .result = 1, .sub_cmd = ublkpp::sub_cmd_t{1}};
    ublkpp::CqeState s2{.owner = &io, .result = 2, .sub_cmd = ublkpp::sub_cmd_t{2}};
    ublkpp::CqeState s3{.owner = &io, .result = 3, .sub_cmd = ublkpp::sub_cmd_t{3}};
    io.push_completion(&s1);
    io.push_completion(&s2);
    io.push_completion(&s3);
    EXPECT_EQ(io.completions[0], &s1);
    EXPECT_EQ(io.completions[1], &s2);
    EXPECT_EQ(io.completions[2], &s3);
}

TEST(AsyncIo, PushCompletionResumesAndClearsWaiter) {
    ublkpp::async_io io{};
    ublkpp::CqeState state{.owner = &io, .result = 99, .sub_cmd = ublkpp::sub_cmd_t{3}};
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

// ============================================================================
// CqeAwaitable
// ============================================================================

// Coroutine that co_awaits a CqeAwaitable and writes the result to *out.
static FireAndForget await_cqe_and_capture(ublkpp::CqeState* state, int* out) {
    *out = co_await ublkpp::CqeAwaitable{state};
}

TEST(CqeAwaitable, AwaitReadyFalseWhenNotReady) {
    ublkpp::async_io io{};
    ublkpp::CqeState state{.owner = &io, .result_ready = false};
    EXPECT_FALSE(ublkpp::CqeAwaitable{&state}.await_ready());
}

TEST(CqeAwaitable, AwaitReadyTrueWhenReady) {
    ublkpp::async_io io{};
    ublkpp::CqeState state{.owner = &io, .result_ready = true};
    EXPECT_TRUE(ublkpp::CqeAwaitable{&state}.await_ready());
}

TEST(CqeAwaitable, AwaitResumeReturnsResult) {
    ublkpp::async_io io{};
    ublkpp::CqeState state{.owner = &io, .result = 42, .result_ready = true};
    EXPECT_EQ(ublkpp::CqeAwaitable{&state}.await_resume(), 42);
}

TEST(CqeAwaitable, AwaitSuspendInstallsWaiterInState) {
    ublkpp::async_io io{};
    ublkpp::CqeState state{.owner = &io, .result_ready = false};
    EXPECT_FALSE(state.waiter);
    int captured = -1;
    await_cqe_and_capture(&state, &captured); // suspends; installs handle in state.waiter
    ASSERT_TRUE(state.waiter);
    EXPECT_EQ(captured, -1); // not resumed yet

    state.result = 7;
    state.result_ready = true;
    state.waiter.resume(); // triggers await_resume() → captured = 7
    EXPECT_EQ(captured, 7);
}

TEST(CqeAwaitable, FastPathSkipsSuspendWhenAlreadyReady) {
    ublkpp::async_io io{};
    ublkpp::CqeState state{.owner = &io, .result = 55, .result_ready = true};
    int captured = -1;
    await_cqe_and_capture(&state, &captured); // await_ready=true → no suspension
    EXPECT_FALSE(state.waiter);
    EXPECT_EQ(captured, 55);
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
