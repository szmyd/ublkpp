#include <memory>
#include <string>
#include <system_error>

#include <boost/uuid/uuid.hpp>
#include <folly/Expected.h>

struct ublksrv_ctrl_dev;
struct ublksrv_dev;
struct ublksrv_dev_data;
struct ublksrv_tgt_type;

namespace ublkpp {

class UblkDisk;

struct ublkpp_tgt {
    bool device_added{false};
    boost::uuids::uuid volume_uuid;
    // Owned by us
    std::shared_ptr< UblkDisk > device;
    std::unique_ptr< ublksrv_tgt_type const > tgt_type;

    // Owned by libublksrv
    ublksrv_ctrl_dev* ctrl_dev{nullptr};
    ublksrv_dev const* ublk_dev{nullptr};

    // Owned by us
    std::unique_ptr< ublksrv_dev_data > dev_data;

    ublkpp_tgt(boost::uuids::uuid const& vol_id, std::shared_ptr< UblkDisk >&& d);
    ~ublkpp_tgt();
};

using run_result_t = folly::Expected< std::string, std::error_condition >;
run_result_t run(std::shared_ptr< ublkpp_tgt > target);

} // namespace ublkpp
