#pragma once

#include <expected>
#include <memory>
#include <filesystem>
#include <system_error>

#include <boost/uuid/uuid.hpp>

// Convenience module list for SISL_LOGGING_INIT. Append the consumer's own
// modules after this list.
#define UBLKPP_LOG_MODS ublk_tgt, ublk_raid, ublk_drivers

namespace ublkpp {

class ublk_disk;
using disk_handle = std::shared_ptr< ublk_disk >;
struct ublkpp_tgt_impl;

struct ublkpp_tgt {
    using run_result_t = std::expected< std::unique_ptr< ublkpp_tgt >, std::error_condition >;

    // Dropping the unique_ptr without calling remove() leaves the ublk device in whatever
    // state the kernel assigns on process exit. Call begin_shutdown() first to ensure the
    // backing store is cleanly flushed (RAID-1 dirty bitmap, clean_unmount flag) before exit.
    ~ublkpp_tgt();

    // Brings up the ublk target. `vol_id` identifies the volume (woven into superblock + metric
    // labels). `device_id`: -1 lets the kernel assign /dev/ublkbN; >=0 attempts to recover a
    // kernel-preserved device under UBLK_F_USER_RECOVERY (no_such_device if not found).
    // Returns the live tgt or an error_condition (system_category from the ublksrv handshake,
    // operation_not_permitted on permission/setup failure, invalid_argument on bad geometry).
    static run_result_t run(boost::uuids::uuid const& vol_id, disk_handle device, int device_id = -1);

    // Signals the target to begin a graceful drain. After this call, reads and writes are
    // rejected with EIO before they reach the backing device; FLUSH ops are allowed through
    // (they complete instantly with result=0 and do not access device*). In-flight ops complete
    // normally. When the last in-flight op finishes (or immediately if the system is already
    // idle), device.reset() is called exactly once — flushing the RAID-1 dirty bitmap and
    // writing clean_unmount=1. Idempotent: subsequent calls are no-ops (guarded by CAS).
    // Note: if there are in-flight ops when this is called, it returns immediately and
    // device.reset() fires later on a queue thread; there is no completion callback.
    // Must be called from a normal thread context, not a signal handler: the idle-drain path
    // calls device.reset() synchronously (may block until the RAID-1 resync task joins).
    //
    // Canonical SIGTERM bridge:
    //   static std::atomic<bool> g_shutdown{false};
    //   signal(SIGTERM, [](int) { g_shutdown.store(true, std::memory_order_relaxed); });
    //   // ... in main loop:
    //   if (g_shutdown.load(std::memory_order_relaxed)) {
    //       tgt->begin_shutdown();
    //       _exit(0);  // skip C++ destructors; OS kills queue threads
    //   }
    void begin_shutdown();

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
