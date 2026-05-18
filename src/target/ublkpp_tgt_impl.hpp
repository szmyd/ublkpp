#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <thread>

#include <boost/uuid/uuid.hpp>
#include <exec/async_scope.hpp>
#include <liburing.h>
#include <ublksrv.h>

#include "lib/resync_dispatch.hpp"
#include "metrics/ublk_io_metrics.hpp"

struct ublksrv_ctrl_dev;
struct ublksrv_dev;
struct ublksrv_dev_data;
struct ublksrv_tgt_type;

namespace ublkpp {

class ublk_disk;

struct ublkpp_tgt_impl {
    bool device_added{false};
    bool device_recovering{false};
    boost::uuids::uuid volume_uuid;
    std::filesystem::path device_path;
    // Owned by us
    std::shared_ptr< ublk_disk > device;
    std::unique_ptr< ublksrv_tgt_type const > tgt_type;

    // Owned by libublksrv
    ublksrv_ctrl_dev* ctrl_dev{nullptr};
    ublksrv_dev const* ublk_dev{nullptr};

    // == Metrics ==
    UblkIOMetrics metrics;

    // == Resync ring + coroutine loop ==
    // One io_uring ring and one dedicated thread per volume drive all RAID1 resync coroutines.
    // The ring is initialized in the constructor. run_resync_queue_loop runs on _resync_handler,
    // processes CQEs from _resync_ring, and spawns RAID1 resync coroutines into _resync_scope.
    // Torn down in destroy(): device.reset() drains all coroutines, then we signal the loop to
    // stop and join the thread before exiting the ring.
    io_uring _resync_ring{};
    ublksrv_queue _resync_queue{};
    const bool _resync_ring_valid;     // initialized in constructor; true iff _resync_ring is usable
    ResyncDispatcher _resync_dispatch; // I/O threads post pending launches here
    exec::async_scope _resync_scope;   // tracks all active resync coroutines
    std::thread _resync_handler;       // drives _resync_ring for this volume
    std::atomic< bool > _resync_loop_stop{false};

    // == ======= ==
    // Owned by us
    std::unique_ptr< ublksrv_dev_data > dev_data;
    std::vector< std::thread > queue_handlers;

    ublkpp_tgt_impl(boost::uuids::uuid const& vol_id, std::shared_ptr< ublk_disk > d);
    ~ublkpp_tgt_impl();
    void destroy();
};

} // namespace ublkpp
