#pragma once

#include <memory>
#include <filesystem>
#include <system_error>

#include <boost/uuid/uuid.hpp>
#include <folly/Expected.h>

#define UBLK_LOG_MODS ublksrv, ublk_tgt, ublk_raid, ublk_drivers, libiscsi

namespace ublkpp {

class UblkDisk;
struct ublkpp_tgt_impl;

struct ublkpp_tgt {
    using run_result_t = folly::Expected< std::unique_ptr< ublkpp_tgt >, std::error_condition >;

    ~ublkpp_tgt();

    static run_result_t run(boost::uuids::uuid const& vol_id, std::shared_ptr< UblkDisk > device);
    std::filesystem::path device_path() const;
    std::shared_ptr< UblkDisk > device() const;

private:
    explicit ublkpp_tgt(std::shared_ptr< ublkpp_tgt_impl > p);
    std::shared_ptr< ublkpp_tgt_impl > _p;
};

} // namespace ublkpp
