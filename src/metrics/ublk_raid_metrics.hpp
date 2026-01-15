#pragma once

#include <cstdint>
#include <string>

#include <sisl/metrics/metrics.hpp>

namespace ublkpp {

// RAID-level metrics - tracks RAID device operations
// Labels: uuid, raid_device_id
struct UblkRaidMetrics : public sisl::MetricsGroupWrapper {
    UblkRaidMetrics(std::string const& uuid, std::string const& raid_device_id);
    ~UblkRaidMetrics();

    void record_device_degraded(std::string const& device_name);
    void record_device_swap();

    // RAID1 resync metrics
    void record_resync_start();
    void record_resync_progress(uint64_t bytes);
    void record_resync_complete(uint64_t duration_seconds);
    void record_active_resyncs(uint64_t count);
    void record_dirty_pages(uint64_t pages);
};

} // namespace ublkpp
