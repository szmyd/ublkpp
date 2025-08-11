#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublksrv_utils.h>

#include "lib/logging.hpp"
#include "tests/test_disk.hpp"

SISL_LOGGING_INIT(ublksrv)

SISL_OPTIONS_ENABLE(logging)

TEST(Logging, FlagSetting) {
    ublk_dbg(0, "Test Debug Log %d", 0);
    ublk_ctrl_dbg(0, "Test Ctrl Debug Log %s", "string_param");
    ublk_err("Test Debug Log %p", nullptr);
    ublk_log("Test ::ublk_log %f", 1.1);
}

TEST(UblkDisk, ToString) {
    auto test_disk = std::make_shared< ublkpp::TestDisk >(TestParams{.capacity = ublkpp::Gi});
    LOGINFO("Test disk: {}", test_disk);
    EXPECT_STREQ("TestDisk", test_disk->id().c_str());
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
