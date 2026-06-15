#pragma once

#include <atomic>
#include <filesystem>
#include <memory>

#include <boost/uuid/uuid.hpp>

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

    // == ======= ==
    // Owned by us
    std::unique_ptr< ublksrv_dev_data > dev_data;
    std::vector< std::thread > queue_handlers;

    // Shutdown drain: set by begin_shutdown(); gates __handle_io_async so no new I/O reaches
    // the backing device. When the last in-flight op decrements the metrics counter to zero,
    // the thread that wins the CAS on _device_reset_done calls device.reset() exactly once,
    // flushing the RAID-1 dirty bitmap and writing clean_unmount=1 before process exit.
    // _drain_complete is set and notified (notify_all) by both device.reset() call sites
    // (idle path in begin_shutdown and non-idle path in try_drain_device) so wait_for_drain()
    // unblocks in the idle case without special-casing.
    std::atomic< bool > _shutting_down{false};
    std::atomic< bool > _device_reset_done{false};
    std::atomic< bool > _drain_complete{false};

    ublkpp_tgt_impl(boost::uuids::uuid const& vol_id, std::shared_ptr< ublk_disk > d);
    ~ublkpp_tgt_impl();
    void destroy();
    // Fires device.reset() if all counters are zero and the CAS has not fired yet.
    // Called by queue threads after decrementing counters; exposed for testing.
    void try_drain();
};

} // namespace ublkpp
