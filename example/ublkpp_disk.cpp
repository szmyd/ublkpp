#include <filesystem>
#include <future>
#include <ostream>
#include <system_error>
#include <vector>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <sisl/http/http_server.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <ublkpp/drivers.hpp>
#include <ublkpp/raid.hpp>
#include <ublkpp/target.hpp>

SISL_OPTION_GROUP(ublkpp_disk,
                  (uuid, "", "vol_id", "Volume UUID to use (else random)", ::cxxopts::value< std::string >(), ""),
                  (loop, "", "loop", "Attach a single device 1-to-1", ::cxxopts::value< std::string >(), "<path>"),
                  (raid0, "", "raid0", "Devices for RAID0 device", ::cxxopts::value< std::vector< std::string > >(),
                   "<path>[,<path>,...]"),
                  (raid1, "", "raid1", "Devices for RAID1 device", ::cxxopts::value< std::vector< std::string > >(),
                   "<path>[,<path>,...]"),
                  (raid10, "", "raid10", "Devices for RAID10 device", ::cxxopts::value< std::vector< std::string > >(),
                   "<path>[,<path>,...]"),
                  (stripe_size, "", "stripe_size", "RAID-0 Stripe Size",
                   ::cxxopts::value< uint32_t >()->default_value("131072"), ""),
                  (device_id, "", "device_id", "Recover existing device",
                   cxxopts::value< int32_t >()->default_value("-1"), "<ublkid>"))

#define ENABLED_OPTIONS logging, ublkpp_tgt, raid1, ublkpp_disk

SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

SISL_LOGGING_INIT(ublksrv, UBLKPP_LOG_MODS)

///
// Clean shutdown
static std::promise< int > s_stop_code;
static void handle(int signal);
///

using Result = std::expected< std::filesystem::path, std::error_condition >;
static auto k_target = std::unique_ptr< ublkpp::ublkpp_tgt >();

static Result _run_target(boost::uuids::uuid const& vol_id, std::shared_ptr< ublkpp::ublk_disk > dev) {

    // Wait for initialization to complete
    auto res = ublkpp::ublkpp_tgt::run(vol_id, std::move(dev), SISL_OPTIONS["device_id"].as< int32_t >());
    if (!res) { return std::unexpected(res.error()); }
    k_target = std::move(res.value());
    return k_target->device_path();
}

// Return a device based on the format of the input
// Optional: pass in a unique identifier for metrics tracking
static std::shared_ptr< ublkpp::ublk_disk > get_driver(std::string const& resource,
                                                       std::string const& metrics_id = "") {
    if (auto path = std::filesystem::path(resource); std::filesystem::exists(path)) {
        return ublkpp::make_fs_disk(path, metrics_id);
    }
    return ublkpp::make_missing_disk();
}

Result create_loop(boost::uuids::uuid const& id, std::string const& path) {
    auto dev = std::shared_ptr< ublkpp::ublk_disk >();
    try {
        auto loop_id = fmt::format("loop_{}", boost::uuids::to_string(id).substr(0, 8));
        dev = get_driver(path, loop_id);
    } catch (std::runtime_error const& e) {}
    if (!dev) return std::unexpected(std::make_error_condition(std::errc::operation_not_permitted));
    return _run_target(id, std::move(dev));
}

Result create_raid0(boost::uuids::uuid const& id, std::vector< std::string > const& layout) {
    auto dev = std::shared_ptr< ublkpp::ublk_disk >();
    try {
        auto devices = std::vector< std::shared_ptr< ublkpp::ublk_disk > >();
        auto raid_uuid = boost::uuids::to_string(id);

        // Create stripe devices with RAID0 UUID for correlation
        for (auto const& disk : layout) {
            devices.push_back(get_driver(disk, raid_uuid));
        }

        if (0 < devices.size())
            dev = ublkpp::make_raid0_disk(id, SISL_OPTIONS["stripe_size"].as< uint32_t >(), std::move(devices));
    } catch (std::runtime_error const& e) {}
    if (!dev) return std::unexpected(std::make_error_condition(std::errc::operation_not_permitted));
    return _run_target(id, std::move(dev));
}

Result create_raid1(boost::uuids::uuid const& id, std::vector< std::string > const& layout) {
    if (layout.size() != 2) {
        LOGERROR("--raid1 requires exactly 2 leg paths, got {}", layout.size())
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    auto dev = std::shared_ptr< ublkpp::ublk_disk >();
    auto raid_uuid_str = boost::uuids::to_string(id);
    try {
        auto leg_a = get_driver(layout[0], raid_uuid_str);
        auto leg_b = get_driver(layout[1], raid_uuid_str);

        auto vol_type = ublkpp::md::disk_type::md_none;
        if (std::filesystem::exists(layout[0]))
            vol_type = ublkpp::md::probe(leg_a);
        else if (std::filesystem::exists(layout[1]))
            vol_type = ublkpp::md::probe(leg_b);
        if (vol_type == ublkpp::md::disk_type::md_dirty) {
            LOGERROR("md array is dirty; stop md-raid and retry")
            return std::unexpected(std::make_error_condition(std::errc::device_or_resource_busy));
        }
        if (vol_type != ublkpp::md::disk_type::md_none)
            dev = ublkpp::md::make_md_raid1_disk(id, {std::move(leg_a), std::move(leg_b)}, raid_uuid_str);
        else
            dev = ublkpp::make_raid1_disk(id, std::move(leg_a), std::move(leg_b), raid_uuid_str);
    } catch (std::runtime_error const& e) { LOGERROR("raid1 construction failed: {}", e.what()) }
    if (!dev) return std::unexpected(std::make_error_condition(std::errc::operation_not_permitted));
    return _run_target(id, std::move(dev));
}

Result create_raid10(boost::uuids::uuid const& id, std::vector< std::string > const& layout) {
    if (layout.size() < 4 || (layout.size() & 1U) != 0) {
        LOGERROR("--raid10 requires an even number of legs >= 4, got {}", layout.size())
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    auto dev = std::shared_ptr< ublkpp::ublk_disk >();
    auto raid_uuid_str = boost::uuids::to_string(id);
    try {
        auto legs = std::vector< std::shared_ptr< ublkpp::ublk_disk > >();
        legs.reserve(layout.size());
        for (auto const& path : layout)
            legs.push_back(get_driver(path, raid_uuid_str));

        auto vol_type = ublkpp::md::disk_type::md_none;
        for (size_t i = 0; i < layout.size(); ++i) {
            if (std::filesystem::exists(layout[i])) {
                vol_type = ublkpp::md::probe(legs[i]);
                break;
            }
        }
        if (vol_type == ublkpp::md::disk_type::md_dirty) {
            LOGERROR("md array is dirty; stop md-raid and retry")
            return std::unexpected(std::make_error_condition(std::errc::device_or_resource_busy));
        }
        if (vol_type != ublkpp::md::disk_type::md_none)
            dev = ublkpp::md::make_md_raid10_disk(id, std::move(legs), raid_uuid_str);
        else
            dev = ublkpp::make_raid10_disk(id, SISL_OPTIONS["stripe_size"].as< uint32_t >(), std::move(legs),
                                           raid_uuid_str);
    } catch (std::runtime_error const& e) { LOGERROR("raid10 construction failed: {}", e.what()) }
    if (!dev) return std::unexpected(std::make_error_condition(std::errc::operation_not_permitted));
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
    if (0 < SISL_OPTIONS["loop"].count()) {
        res = create_loop(vol_id, SISL_OPTIONS["loop"].as< std::string >());
    } else if (0 < SISL_OPTIONS["raid0"].count()) {
        res = create_raid0(vol_id, SISL_OPTIONS["raid0"].as< std::vector< std::string > >());
    } else if (0 < SISL_OPTIONS["raid1"].count()) {
        res = create_raid1(vol_id, SISL_OPTIONS["raid1"].as< std::vector< std::string > >());
    } else if (0 < SISL_OPTIONS["raid10"].count()) {
        res = create_raid10(vol_id, SISL_OPTIONS["raid10"].as< std::vector< std::string > >());
    } else
        std::cout << SISL_PARSER.help({}) << std::endl;

    auto http_server = sisl::HttpServer{};
    if (res) {
        try {
            http_server.register_metrics_endpoint();
            http_server.start();
        } catch (std::runtime_error const& e) { LOGERROR("setup routes failed, {}", e.what()) }

        exit_future.wait();
        http_server.stop();
        ublkpp::ublkpp_tgt::remove(std::move(k_target));
    } else
        s_stop_code.set_value(EIO);
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
