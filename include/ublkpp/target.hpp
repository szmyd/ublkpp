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

    // Signals the target to begin a graceful drain. After this call, new I/O is dropped (left
    // uncompleted / OWNED_BY_SRV) so the kernel requeues it to the next daemon under
    // UBLK_F_USER_RECOVERY(_REISSUE) when this process exits via the recovery path (drop the
    // unique_ptr, NOT remove()). In-flight ops complete normally. When the last in-flight op
    // finishes (or immediately if already idle), the backing device is destroyed exactly once,
    // flushing the RAID-1 dirty bitmap and writing clean_unmount=1. Idempotent (guarded by CAS).
    // Note: if there are in-flight ops when this is called, it returns immediately and
    // the backing device is destroyed later on a queue thread; there is no completion callback.
    // Must be called from a normal thread context, not a signal handler: the idle-drain path
    // destroys the backing device synchronously (may block until the RAID-1 resync task joins).
    //
    // Canonical SIGTERM bridge:
    //   static std::atomic<bool> g_shutdown{false};
    //   signal(SIGTERM, [](int) { g_shutdown.store(true, std::memory_order_relaxed); });
    //   // ... in main loop:
    //   if (g_shutdown.load(std::memory_order_relaxed)) {
    //       tgt->begin_shutdown();
    //       // For the common case (no I/O in-flight at SIGTERM time), begin_shutdown()
    //       // destroys the backing device synchronously before returning — clean_unmount=1 is
    //       // already written. Any exit form is safe at this point.
    //       //
    //       // For the rare non-idle case (ops in-flight), begin_shutdown() returns
    //       // immediately; the backing device is destroyed later on a queue thread. Call
    //       // wait_for_drain() after begin_shutdown() to block until the flush has completed,
    //       // giving a guaranteed clean_unmount=1 before process exit.
    //       tgt->wait_for_drain();
    //       return 0;
    //   }
    void begin_shutdown();

    // Blocks until this target has released its backing device reference and all in-flight I/O
    // has been rejected or completed. Returns immediately if begin_shutdown() already destroyed
    // the backing device synchronously (idle case). Must be called after begin_shutdown();
    // calling without begin_shutdown() aborts (RELEASE_ASSERT).
    //
    // The RAID-1 dirty bitmap flush and clean_unmount=1 write are guaranteed complete when this
    // returns: _drain_complete is notified only after device = disk_handle{} destructs the
    // backing store synchronously. Drop the ublkpp_tgt immediately after to join queue threads
    // (or use begin_shutdown() + wait_for_drain() + drop as an atomic unit).
    void wait_for_drain();

    // Cleanly stops the device and removes it from the kernel. Only call after the block device
    // has been unmounted. Consuming the unique_ptr prevents accidental double-remove.
    static void remove(std::unique_ptr< ublkpp_tgt > tgt);

    std::filesystem::path device_path() const;
    disk_handle device() const;
    // Asserts (RELEASE_ASSERT) if called on a make_for_test() target (dev_data is null).
    int device_id() const;

private:
    explicit ublkpp_tgt(std::shared_ptr< ublkpp_tgt_impl > p);
    std::shared_ptr< ublkpp_tgt_impl > _p;
    friend struct ublkpp_tgt_test_peer;
};

} // namespace ublkpp
