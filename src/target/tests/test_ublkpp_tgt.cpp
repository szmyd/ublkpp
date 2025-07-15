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
    auto repl_set = ublkpp::set_flags(0, sub_cmd_flags::REPLICATE);
    EXPECT_TRUE(ublkpp::is_replicate(repl_set));
    EXPECT_FALSE(ublkpp::is_retry(repl_set));

    auto both_set = ublkpp::set_flags(repl_set, sub_cmd_flags::RETRIED);
    EXPECT_TRUE(ublkpp::is_replicate(both_set));
    EXPECT_TRUE(ublkpp::is_retry(both_set));

    auto retry_set = ublkpp::unset_flags(both_set, sub_cmd_flags::REPLICATE);
    EXPECT_FALSE(ublkpp::is_replicate(retry_set));
    EXPECT_TRUE(ublkpp::is_retry(retry_set));

    auto neither_set = ublkpp::unset_flags(retry_set, sub_cmd_flags::RETRIED);
    EXPECT_FALSE(ublkpp::is_replicate(neither_set));
    EXPECT_FALSE(ublkpp::is_retry(neither_set));

    auto multi_set = ublkpp::set_flags(0, sub_cmd_flags::RETRIED | sub_cmd_flags::REPLICATE);
    EXPECT_TRUE(ublkpp::is_replicate(multi_set));
    EXPECT_TRUE(ublkpp::is_retry(multi_set));

    auto multi_unset = ublkpp::unset_flags(multi_set, sub_cmd_flags::RETRIED | sub_cmd_flags::REPLICATE);
    EXPECT_FALSE(ublkpp::is_replicate(multi_unset));
    EXPECT_FALSE(ublkpp::is_retry(multi_unset));
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
