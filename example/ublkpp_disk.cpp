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
#include <ublkpp/raid/raid0.hpp>
#include <ublkpp/raid/raid1.hpp>

#ifdef HAVE_HOMEBLOCKS
#include <ublkpp/drivers/homeblk_disk.hpp>
#include <homeblks/home_blks.hpp>
#include <homeblks/volume_mgr.hpp>
#endif
#ifdef HAVE_ISCSI
#include <ublkpp/drivers/iscsi_disk.hpp>
#endif

SISL_OPTION_GROUP(
    ublkpp_disk, (uuid, "", "vol_id", "Volume UUID to use (else random)", ::cxxopts::value< std::string >(), ""),
    (loop, "", "loop", "Attach a single device 1-to-1", ::cxxopts::value< std::string >(), "<path>"),
    (raid0, "", "raid0", "Devices for RAID0 device", ::cxxopts::value< std::vector< std::string > >(),
     "<path>[,<path>,...]"),
    (raid1, "", "raid1", "Devices for RAID1 device", ::cxxopts::value< std::vector< std::string > >(),
     "<path>[,<path>,...]"),
    (raid10, "", "raid10", "Devices for RAID10 device", ::cxxopts::value< std::vector< std::string > >(),
     "<path>[,<path>,...]"),
#ifdef HAVE_HOMEBLOCKS
    (capacity, "", "capacity", "HomeBlks disk capacity GiB", ::cxxopts::value< uint32_t >()->default_value("2"),
     "<GiB>"),
    (homeblks_dev, "", "homeblks_dev", "path to the device to run HomeBlocks on", cxxopts::value< std::string >(), ""),
#endif
    (stripe_size, "", "stripe_size", "RAID-0 Stripe Size", ::cxxopts::value< uint32_t >()->default_value("131072"), ""))

#ifdef HAVE_HOMEBLOCKS
#define HOMEBLKS_OPTIONS , homeblocks, iomgr
#else
#define HOMEBLKS_OPTIONS
#endif

#ifdef HAVE_ISCSI
#define ISCSI_OPTIONS , iscsi
#else
#define ISCSI_OPTIONS
#endif

#define ENABLED_OPTIONS logging, ublkpp_tgt, raid1, fs_disk, ublkpp_disk HOMEBLKS_OPTIONS ISCSI_OPTIONS

SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

#ifdef HAVE_HOMEBLOCKS
#define HOMEBLKS_MODS , HOMEBLOCKS_LOG_MODS, HOMESTORE_LOG_MODS, IOMGR_LOG_MODS
#else
#define HOMEBLKS_MODS
#endif

#ifdef HAVE_ISCSI
#define ISCSI_MODS , libiscsi
#else
#define ISCSI_MODS
#endif

SISL_LOGGING_INIT(ublksrv, UBLK_LOG_MODS HOMEBLKS_MODS ISCSI_MODS)

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

#ifdef HAVE_HOMEBLOCKS
class UblkPPApplication : public homeblocks::HomeBlocksApplication {
public:
    using dev_list_t = std::list< homeblocks::device_info_t >;
    dev_list_t const _devices;
    uint32_t const _io_threads;
    homeblocks::peer_id_t const _id;
    std::shared_ptr< homeblocks::HomeBlocks > _hb;

    explicit UblkPPApplication(dev_list_t const& dev_list, homeblocks::peer_id_t p = boost::uuids::random_generator()(),
                               uint32_t const io_threads = 1) :
            _devices(dev_list), _io_threads(io_threads), _id(p) {}
    ~UblkPPApplication() override = default;

    // implement all the virtual functions in HomeObjectApplication
    uint64_t app_mem_size() const override { return 20; }
    bool spdk_mode() const override { return false; }
    uint32_t threads() const override { return _io_threads; }
    dev_list_t devices() const override { return _devices; }

    std::optional< homeblocks::peer_id_t >
    discover_svc_id(std::optional< homeblocks::peer_id_t > const&) const override {
        return _id;
    }
};
static std::shared_ptr< UblkPPApplication > _app;

static std::shared_ptr< UblkPPApplication > init_homeblocks(std::string const& hb_dev) {
    auto const path = std::filesystem::path(hb_dev);
    if (!std::filesystem::exists(path)) {
        LOGERROR("Homestore device: {} does not exist!", path.native())
        return nullptr;
    }

    auto dev_list = std::list< homeblocks::device_info_t >();
    dev_list.emplace_back(homeblocks::device_info_t{path.native(), homeblocks::DevType::NVME});
    auto app = std::make_shared< UblkPPApplication >(dev_list);
    app->_hb = homeblocks::init_homeblocks(decltype(app)::weak_type(app));

    return app;
}

static auto create_hb_volume(UblkPPApplication& app, boost::uuids::uuid const& vol_uuid) {
    homeblocks::VolumeInfo vol_info;
    vol_info.page_size = 4 * Ki;
    vol_info.size_bytes = SISL_OPTIONS["capacity"].as< uint32_t >() * Gi;
    vol_info.id = vol_uuid;
    vol_info.name = "ublkpp_disk";
    LOGINFO("Creating volume: {}", vol_info.to_string());

    return app._hb->volume_manager()
        ->create_volume(std::move(vol_info))
        .via(&folly::InlineExecutor::instance())
        .thenValue([](auto&& e) {
            if (e.hasError()) { return std::make_error_condition(std::errc::io_error); }
            return std::error_condition();
        })
        .get();
}
#endif

// Return a device based on the format of the input
static std::unique_ptr< ublkpp::UblkDisk > get_driver(std::string const& resource) {
#ifdef HAVE_HOMEBLOCKS
    if (0 < SISL_OPTIONS["homeblks_dev"].count()) {
        if (0 < SISL_OPTIONS["capacity"].count()) {
            try {
                auto const vol_uuid = boost::uuids::string_generator()(resource);
                if (!_app) {
                    if (_app = init_homeblocks(SISL_OPTIONS["homeblks_dev"].as< std::string >()); !_app) return nullptr;
                }
                if (auto e = create_hb_volume(*_app, vol_uuid); e) return nullptr;
                return std::make_unique< ublkpp::HomeBlkDisk >(vol_uuid, SISL_OPTIONS["capacity"].as< uint32_t >() * Gi,
                                                               _app->_hb->volume_manager(), 512 * Ki);
            } catch (std::runtime_error const& e) {}
        } else {
            LOGERROR("HomeBlocks device requires --capacity argument!")
        }
        return nullptr;
    }
#endif
    if (auto path = std::filesystem::path(resource); std::filesystem::exists(path))
        return std::make_unique< ublkpp::FSDisk >(path);
#ifdef HAVE_ISCSI
    // From libiscsi.h iSCSI URLs are in the form:
    //   iscsi://[<username>[%<password>]@]<host>[:<port>]/<target-iqn>/<lun>
    return std::make_unique< ublkpp::iSCSIDisk >(resource);
#else
    return nullptr;
#endif
}

Result create_loop(boost::uuids::uuid const& id, std::string const& path) {
    auto dev = std::unique_ptr< ublkpp::UblkDisk >();
    try {
        dev = get_driver(path);
    } catch (std::runtime_error const& e) {}
    if (!dev) return folly::makeUnexpected(std::make_error_condition(std::errc::operation_not_permitted));
    return _run_target(id, std::move(dev));
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
        auto dev_a = std::unique_ptr< ublkpp::UblkDisk >();
        for (auto const& mirror : layout) {
            auto new_dev = get_driver(mirror);
            if (0 == cnt++ % 2)
                dev_a = std::move(new_dev);
            else
                devices.push_back(std::make_shared< ublkpp::Raid1Disk >(id, std::move(dev_a), std::move(new_dev)));
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

    if (!res) return -1;

    exit_future.wait();
    k_target.reset();
#ifdef HAVE_HOMEBLOCKS
    if (_app) _app->_hb->shutdown();
#endif
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
