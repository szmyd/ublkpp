#include <memory>
#include <filesystem>
#include <system_error>

#include <boost/uuid/uuid.hpp>
#include <folly/Expected.h>

#define UBLK_LOG_MODS ublksrv, ublk_tgt, ublk_raid, ublk_drivers

namespace ublkpp {

class UblkDisk;
struct ublkpp_tgt_impl;

struct ublkpp_tgt {
    ublkpp_tgt(std::shared_ptr<ublkpp_tgt_impl> p);
    ~ublkpp_tgt();

    std::filesystem::path device_path() const;

private:
    std::shared_ptr< ublkpp_tgt_impl > _p;
};

using run_result_t = folly::Expected< std::shared_ptr< ublkpp_tgt >, std::error_condition >;

extern run_result_t run(boost::uuids::uuid const& vol_id, std::unique_ptr< UblkDisk > device);

} // namespace ublkpp
