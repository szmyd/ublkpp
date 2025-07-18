#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#define ENABLED_OPTIONS logging, raid1

SISL_LOGGING_INIT(ublk_raid)
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

extern "C" {
struct ublksrv_queue;
extern int ublksrv_queue_send_event(ublksrv_queue const*) {
    LOGTRACE("Queue event!");
    return 0;
}
}

int main(int argc, char* argv[]) {
    int parsed_argc = argc;
    ::testing::InitGoogleTest(&parsed_argc, argv);
    SISL_OPTIONS_LOAD(parsed_argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
