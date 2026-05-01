#pragma once

#include <expected>
#include <memory>
#include <filesystem>
#include <system_error>

#include <boost/uuid/uuid.hpp>

#ifdef HAVE_LIBISCSI
#define UBLK_LOG_MODS ublksrv, ublk_tgt, ublk_raid, ublk_drivers, libiscsi
#else
#define UBLK_LOG_MODS ublksrv, ublk_tgt, ublk_raid, ublk_drivers
#endif

namespace ublkpp {

class UblkDisk;
struct ublkpp_tgt_impl;

struct ublkpp_tgt {
    using run_result_t = std::expected< std::unique_ptr< ublkpp_tgt >, std::error_condition >;

    // Intentionally does NOT call remove(). Dropping the unique_ptr without calling remove()
    // is the correct path for SIGTERM / pod restart: the kernel preserves the ublk device under
    // UBLK_F_USER_RECOVERY and the next process reconnects via the recovering path. Calling
    // remove() with an active mount would deadlock -- del_dev blocks until the mount releases,
    // but the mount cannot release because the queues are already stopped.
    ~ublkpp_tgt();

    static run_result_t run(boost::uuids::uuid const& vol_id, std::shared_ptr< UblkDisk > device, int device_id = -1);

    // Cleanly stops the device and removes it from the kernel. Only call after the block device
    // has been unmounted. Consuming the unique_ptr prevents accidental double-remove.
    static void remove(std::unique_ptr< ublkpp_tgt > tgt);

    std::filesystem::path device_path() const;
    std::shared_ptr< UblkDisk > device() const;
    int device_id() const;

private:
    explicit ublkpp_tgt(std::shared_ptr< ublkpp_tgt_impl > p);
    std::shared_ptr< ublkpp_tgt_impl > _p;
};

} // namespace ublkpp
