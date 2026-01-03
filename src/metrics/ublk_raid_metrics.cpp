#include "ublkpp/metrics/ublk_raid_metrics.hpp"

#include <fmt/format.h>

namespace ublkpp {

UblkRaidMetrics::UblkRaidMetrics(std::string const& uuid, std::string const& raid_device_id)
    : sisl::MetricsGroup{"ublk_raid_metrics", raid_device_id} {
    // Use raid_device_id as entity name to ensure each RAID has unique metrics
    // Use uuid (app UUID) as label so you can filter by application in Sherlock/Prometheus

    REGISTER_COUNTER(raid_degraded_count, "RAID device degradation events", "ublk_raid_degraded_count",
                     {"uuid", uuid});
    // RAID1 resync metrics
    REGISTER_COUNTER(resync_started_total, "Total number of resyncs started", "ublk_resync_started_total",
                     {"uuid", uuid});
    REGISTER_COUNTER(resync_bytes_total, "Total bytes resynced", "ublk_resync_bytes_total",
                     {"uuid", uuid});
    REGISTER_HISTOGRAM(resync_duration_ms, "Resync duration in milliseconds", "ublk_resync_duration_ms",
                       {"uuid", uuid}, HistogramBucketsType(ExponentialOfTwoBuckets));
    REGISTER_GAUGE(active_resyncs, "Number of active resyncs", "ublk_active_resyncs",
                   {"uuid", uuid});
    REGISTER_GAUGE(dirty_pages, "Number of dirty bitmap pages", "ublk_dirty_pages",
                   {"uuid", uuid});
    register_me_to_farm();
}

UblkRaidMetrics::~UblkRaidMetrics() { deregister_me_from_farm(); }

void UblkRaidMetrics::record_device_degraded() {
    COUNTER_INCREMENT(*this, raid_degraded_count, 1);
}

void UblkRaidMetrics::record_resync_start() {
    COUNTER_INCREMENT(*this, resync_started_total, 1);
}

void UblkRaidMetrics::record_resync_progress(uint64_t bytes) {
    COUNTER_INCREMENT(*this, resync_bytes_total, bytes);
}

void UblkRaidMetrics::record_resync_complete(uint64_t duration_ms) {
    HISTOGRAM_OBSERVE(*this, resync_duration_ms, duration_ms);
}

void UblkRaidMetrics::record_active_resyncs(uint64_t count) {
    GAUGE_UPDATE(*this, active_resyncs, count);
}

void UblkRaidMetrics::record_dirty_pages(uint64_t pages) {
    GAUGE_UPDATE(*this, dirty_pages, pages);
}

} // namespace ublkpp
