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
    // avail_delay=0 so probe-wait loops don't stall tests; still overridable from the command line.
    std::vector< const char* > args(argv, argv + parsed_argc);
    args.push_back("--avail_delay=0");
    auto sisl_argc = static_cast< int >(args.size());
    SISL_OPTIONS_LOAD(sisl_argc, args.data(), ENABLED_OPTIONS);
    sisl::logging::SetLogger(std::string(argv[0]));
    spdlog::set_pattern("[%D %T.%e] [%n] [%^%l%$] [%t] %v");
    parsed_argc = 1;
    return RUN_ALL_TESTS();
}
