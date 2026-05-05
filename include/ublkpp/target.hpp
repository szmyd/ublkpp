#pragma once

#include <expected>
#include <memory>
#include <filesystem>
#include <system_error>

#include <boost/uuid/uuid.hpp>

// Convenience module list for SISL_LOGGING_INIT. Prepend `ublksrv` (the underlying
// kernel-interface library, always required) and append any consumer-specific modules.
// `libiscsi` is folded in automatically when HAVE_ISCSI is set (propagated via the
// Conan package's cpp_info.defines). Example:
//   SISL_LOGGING_INIT(ublksrv, UBLKPP_LOG_MODS, my_app)
#ifdef HAVE_ISCSI
#define UBLKPP_LOG_MODS ublk_tgt, ublk_raid, ublk_drivers, libiscsi
#else
#define UBLKPP_LOG_MODS ublk_tgt, ublk_raid, ublk_drivers
#endif

namespace ublkpp {

class ublk_disk;
using disk_handle = std::shared_ptr< ublk_disk >;
struct ublkpp_tgt_impl;

struct ublkpp_tgt {
    using run_result_t = std::expected< std::unique_ptr< ublkpp_tgt >, std::error_condition >;

    // Intentionally does NOT call remove(). Dropping the unique_ptr without calling remove()
    // is the correct path for SIGTERM / pod restart: the kernel preserves the ublk device under
    // UBLK_F_USER_RECOVERY and the next process reconnects via the recovering path. Calling
    // remove() with an active mount would deadlock -- del_dev blocks until the mount releases,
    // but the mount cannot release because the queues are already stopped.
    ~ublkpp_tgt();

    // Brings up the ublk target. `vol_id` identifies the volume (woven into superblock + metric
    // labels). `device_id`: -1 lets the kernel assign /dev/ublkbN; >=0 attempts to recover a
    // kernel-preserved device under UBLK_F_USER_RECOVERY (no_such_device if not found).
    // Returns the live tgt or an error_condition (system_category from the ublksrv handshake,
    // operation_not_permitted on permission/setup failure, invalid_argument on bad geometry).
    static run_result_t run(boost::uuids::uuid const& vol_id, disk_handle device, int device_id = -1);

    // Cleanly stops the device and removes it from the kernel. Only call after the block device
    // has been unmounted. Consuming the unique_ptr prevents accidental double-remove.
    static void remove(std::unique_ptr< ublkpp_tgt > tgt);

    std::filesystem::path device_path() const;
    disk_handle device() const;
    int device_id() const;

private:
    explicit ublkpp_tgt(std::shared_ptr< ublkpp_tgt_impl > p);
    std::shared_ptr< ublkpp_tgt_impl > _p;
};

} // namespace ublkpp
