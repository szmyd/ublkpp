#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include "ublkpp/lib/sub_cmd.hpp"

SISL_LOGGING_INIT(ublk_tgt)

SISL_OPTIONS_ENABLE(logging)

using ublkpp::sub_cmd_t;

TEST(SubCmd, ShiftRoute) {
    // shift_route takes the low bits of sub_cmd and shifts them left by route_size,
    // embedding child routing into the caller's routing word.
    auto const routed = ublkpp::shift_route(sub_cmd_t{0b11}, 2);
    EXPECT_EQ(routed, sub_cmd_t{0b1100});

    // Chained shift: simulate RAID10 (RAID0 over RAID1)
    auto const raid1_bits = ublkpp::shift_route(sub_cmd_t{0b1}, 1); // RAID1: 1 bit
    auto const raid0_bits = ublkpp::shift_route(raid1_bits, 6);     // RAID0: 6 bits
    EXPECT_EQ(raid0_bits & ublkpp::_route_mask, sub_cmd_t{0b10000000});
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
