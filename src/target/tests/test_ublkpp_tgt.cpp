#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "ublkpp/lib/sub_cmd.hpp"
#include "ublkpp/ublkpp.hpp"

SISL_LOGGING_INIT(ublk_tgt)

SISL_OPTIONS_ENABLE(logging, ublkpp_tgt)

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

TEST(MemoryEstimation, QueueMemory) {
    // Test uses default SISL options:
    // qdepth=128, max_io_size=524288 (512 KiB), nr_hw_queues=1
    uint64_t memory = ublkpp::ublkpp_tgt::estimate_queue_memory();

    // Expected breakdown with defaults:
    // - ublksrv buffers: 1 × (128 × 524288 + 128 × 40) ≈ 64 MiB
    // - thread stack: 1 × 8 MiB = 8 MiB
    // - target overhead: 4 KiB
    // Total: ~72 MiB
    EXPECT_GT(memory, 70ULL * 1024 * 1024); // > 70 MiB
    EXPECT_LT(memory, 75ULL * 1024 * 1024); // < 75 MiB
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, logging, ublkpp_tgt);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
