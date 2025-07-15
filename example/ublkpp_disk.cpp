#include <filesystem>
#include <future>
#include <ostream>
#include <system_error>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <ublkpp/ublkpp.hpp>
#include <ublkpp/drivers/fs_disk.hpp>
#include <ublkpp/drivers/iscsi_disk.hpp>
#include <ublkpp/raid/raid0.hpp>
#include <ublkpp/raid/raid1.hpp>

#define ENABLED_OPTIONS logging, ublkpp_tgt, raid1, iscsi, example_app

SISL_OPTION_GROUP(example_app,
                  (uuid, "", "vol_id", "Volume UUID to use (else random)", ::cxxopts::value< std::string >(), ""),
                  (raid0, "", "raid0", "Devices for RAID0 device", ::cxxopts::value< std::vector< std::string > >(),
                   "<path>[,<path>,...]"),
                  (raid1, "", "raid1", "Devices for RAID1 device", ::cxxopts::value< std::vector< std::string > >(),
                   "<path>[,<path>,...]"),
                  (raid10, "", "raid10", "Devices for RAID10 device", ::cxxopts::value< std::vector< std::string > >(),
                   "<path>[,<path>,...]"),
                  (stripe_size, "", "stripe_size", "RAID-0 Stripe Size",
                   ::cxxopts::value< uint32_t >()->default_value("131072"), ""))

SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)
SISL_LOGGING_INIT(ublksrv, UBLK_LOG_MODS)

///
// Clean shutdown
static std::promise< int > s_stop_code;
static void handle(int signal);
///

using Result = folly::Expected< std::filesystem::path, std::error_condition >;
static auto k_target = std::unique_ptr< ublkpp::ublkpp_tgt >();

template < typename D >
Result _run_target(boost::uuids::uuid const& vol_id, std::unique_ptr< D >&& dev) {

    // Wait for initialization to complete
    auto res = ublkpp::ublkpp_tgt::run(vol_id, std::move(dev));
    if (!res) { return folly::makeUnexpected(res.error()); }
    k_target = std::move(res.value());
    return k_target->device_path();
}

// Return a device based on the format of the input
// From libiscsi.h iSCSI URLs are in the form:
//   iscsi://[<username>[%<password>]@]<host>[:<port>]/<target-iqn>/<lun>
static std::shared_ptr< ublkpp::UblkDisk > get_driver(std::string const& resource) {
    if (auto path = std::filesystem::path(resource); std::filesystem::exists(path))
        return std::make_shared< ublkpp::FSDisk >(path);
    return std::make_shared< ublkpp::iSCSIDisk >(resource);
}

Result create_raid0(boost::uuids::uuid const& id, std::vector< std::string > const& layout) {
    auto dev = std::unique_ptr< ublkpp::Raid0Disk >();
    try {
        auto devices = std::vector< std::shared_ptr< ublkpp::UblkDisk > >();
        for (auto const& disk : layout) {
            devices.push_back(get_driver(disk));
        }
        if (0 < devices.size())
            dev = std::make_unique< ublkpp::Raid0Disk >(id, SISL_OPTIONS["stripe_size"].as< uint32_t >(),
                                                        std::move(devices));
    } catch (std::runtime_error const& e) {}
    if (!dev) return folly::makeUnexpected(std::make_error_condition(std::errc::operation_not_permitted));
    return _run_target(id, std::move(dev));
}

Result create_raid1(boost::uuids::uuid const& id, std::vector< std::string > const& layout) {
    auto dev = std::unique_ptr< ublkpp::Raid1Disk >();
    try {
        dev = std::make_unique< ublkpp::Raid1Disk >(id, get_driver(*layout.begin()), get_driver(*(layout.begin() + 1)));
    } catch (std::runtime_error const& e) {}
    if (!dev) return folly::makeUnexpected(std::make_error_condition(std::errc::operation_not_permitted));
    return _run_target(id, std::move(dev));
}

Result create_raid10(boost::uuids::uuid const& id, std::vector< std::string > const& layout) {
    if (1 > layout.size()) {
        LOGERROR("Zero mirrors in Array [uuid:{}]!", to_string(id))
        return folly::makeUnexpected(std::make_error_condition(std::errc::invalid_argument));
    }

    auto dev = std::unique_ptr< ublkpp::Raid0Disk >();
    try {
        auto cnt{0U};
        auto devices = std::vector< std::shared_ptr< ublkpp::UblkDisk > >();
        auto dev_a = std::shared_ptr< ublkpp::UblkDisk >();
        for (auto const& mirror : layout) {
            auto new_dev = get_driver(mirror);
            if (0 == cnt++ % 2)
                dev_a = new_dev;
            else
                devices.push_back(std::make_shared< ublkpp::Raid1Disk >(id, dev_a, new_dev));
        }
        dev =
            std::make_unique< ublkpp::Raid0Disk >(id, SISL_OPTIONS["stripe_size"].as< uint32_t >(), std::move(devices));
    } catch (std::runtime_error const& e) {}
    if (!dev) return folly::makeUnexpected(std::make_error_condition(std::errc::operation_not_permitted));
    return _run_target(id, std::move(dev));
}

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, ENABLED_OPTIONS);
    sisl::logging::SetLogger(std::string(argv[0]),
                             BOOST_PP_STRINGIZE(PACKAGE_NAME), BOOST_PP_STRINGIZE(PACKAGE_VERSION));
    spdlog::set_pattern("[%D %T] [%^%l%$] [%n] [%t] %v");

    signal(SIGINT, handle);
    signal(SIGTERM, handle);

    auto exit_future = s_stop_code.get_future();

    auto vol_id = (0 < SISL_OPTIONS["vol_id"].count())
        ? boost::uuids::string_generator()(SISL_OPTIONS["vol_id"].as< std::string >())
        : boost::uuids::random_generator()();
    Result res;
    if (0 < SISL_OPTIONS["raid0"].count()) {
        res = create_raid0(vol_id, SISL_OPTIONS["raid0"].as< std::vector< std::string > >());
    } else if (0 < SISL_OPTIONS["raid1"].count()) {
        res = create_raid1(vol_id, SISL_OPTIONS["raid1"].as< std::vector< std::string > >());
    } else if (0 < SISL_OPTIONS["raid10"].count()) {
        res = create_raid10(vol_id, SISL_OPTIONS["raid10"].as< std::vector< std::string > >());
    } else
        std::cout << SISL_PARSER.help({}) << std::endl;

    if (!res) return -1;

    exit_future.wait();
    k_target.reset();
    return exit_future.get();
}

static void handle(int signal) {
    switch (signal) {
    case SIGINT:
        [[fallthrough]];
    case SIGTERM: {
        // if multiple SIGTERM are received, the set_value will throw an exception
        try {
            LOGWARN("SIGNAL: {}", strsignal(signal));
            s_stop_code.set_value(signal);
        } catch (std::future_error const& e) { LOGERROR("Failed to set stop code: {}", e.what()); }
    } break;
        ;
    default:
        LOGERROR("Unhandled SIGNAL: {}", strsignal(signal));
        break;
    }
}
