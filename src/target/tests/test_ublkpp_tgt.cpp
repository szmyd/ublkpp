#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "ublkpp/lib/sub_cmd.hpp"

SISL_LOGGING_INIT(ublk_tgt)

SISL_OPTIONS_ENABLE(logging)

using ublkpp::sub_cmd_flags;
using ublkpp::sub_cmd_t;

TEST(SubCmd, FlagSetting) {
    auto repl_set = ublkpp::set_flags(0, sub_cmd_flags::REPLICATED);
    EXPECT_TRUE(test_flags(repl_set, sub_cmd_flags::REPLICATED));
    EXPECT_FALSE(test_flags(repl_set, sub_cmd_flags::RETRIED));

    auto both_set = ublkpp::set_flags(repl_set, sub_cmd_flags::RETRIED);
    EXPECT_TRUE(test_flags(both_set, sub_cmd_flags::REPLICATED));
    EXPECT_TRUE(test_flags(both_set, sub_cmd_flags::RETRIED));

    auto retry_set = ublkpp::unset_flags(both_set, sub_cmd_flags::REPLICATED);
    EXPECT_FALSE(test_flags(retry_set, sub_cmd_flags::REPLICATED));
    EXPECT_TRUE(test_flags(retry_set, sub_cmd_flags::RETRIED));

    auto neither_set = ublkpp::unset_flags(retry_set, sub_cmd_flags::RETRIED);
    EXPECT_FALSE(test_flags(neither_set, sub_cmd_flags::REPLICATED));
    EXPECT_FALSE(test_flags(neither_set, sub_cmd_flags::RETRIED));

    auto multi_set = ublkpp::set_flags(0, sub_cmd_flags::RETRIED | sub_cmd_flags::REPLICATED);
    EXPECT_TRUE(test_flags(multi_set, sub_cmd_flags::REPLICATED));
    EXPECT_TRUE(test_flags(multi_set, sub_cmd_flags::RETRIED));

    auto multi_unset = ublkpp::unset_flags(multi_set, sub_cmd_flags::RETRIED | sub_cmd_flags::REPLICATED);
    EXPECT_FALSE(test_flags(multi_unset, sub_cmd_flags::REPLICATED));
    EXPECT_FALSE(test_flags(multi_unset, sub_cmd_flags::RETRIED));
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
