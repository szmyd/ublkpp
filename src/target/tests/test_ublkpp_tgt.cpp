#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "ublkpp/lib/cqe_state.hpp"

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
    EXPECT_TRUE(sisl::async::is_managed_user_data(user_data));
    auto* decoded = static_cast< ublkpp::cqe_state* >(sisl::async::decode_managed_user_data(user_data));
    EXPECT_EQ(state, decoded);
    EXPECT_EQ(state->_owner, &io);
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
