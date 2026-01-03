#include "ublkpp/metrics/ublk_fsdisk_metrics.hpp"

#include <ublksrv.h>

namespace ublkpp {

UblkFSDiskMetrics::UblkFSDiskMetrics(std::string const& uuid, std::string const& raid_uuid, std::string const& disk_path)
    : sisl::MetricsGroup{"ublk_fsdisk_metrics", disk_path} {
    REGISTER_HISTOGRAM(fake, "Disk I/O latency in microseconds", "ublk_disk_io_latency_us",
                   {"blabla", uuid}, HistogramBucketsType(ExponentialOfTwoBuckets));

    // Use disk_path as entity name to ensure each disk has unique metrics
    // Use raid_uuid as label so you can filter by RAID in Sherlock/Prometheus
    // The uuid (app UUID) can be accessed via the entity hierarchy if needed

    REGISTER_COUNTER(disk_io_ops_total, "Total disk I/O operations", "ublk_disk_io_ops_total",
                     {"raid_uuid", raid_uuid});
    REGISTER_HISTOGRAM(disk_io_latency_us, "Disk I/O latency in microseconds", "ublk_disk_io_latency_us",
                       {"raid_uuid", raid_uuid}, HistogramBucketsType(ExponentialOfTwoBuckets));
    register_me_to_farm();
}

UblkFSDiskMetrics::~UblkFSDiskMetrics() { deregister_me_from_farm(); }

void UblkFSDiskMetrics::record_io_start(ublk_io_data const* data, sub_cmd_t sub_cmd) {
    if (!data) return;

    auto key = std::make_pair(static_cast<uint16_t>(data->tag), static_cast<uint16_t>(sub_cmd));
    t_disk_io_timings[key] = io_timing{std::chrono::steady_clock::now()};
}

void UblkFSDiskMetrics::record_io_complete(ublk_io_data const* data, sub_cmd_t sub_cmd) {
    if (!data) return;

    auto key = std::make_pair(static_cast<uint16_t>(data->tag), static_cast<uint16_t>(sub_cmd));

    if (auto it = t_disk_io_timings.find(key); it != t_disk_io_timings.end()) {
        auto const& timing = it->second;
        auto const end_time = std::chrono::steady_clock::now();
        auto const latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - timing.start_time).count();

        HISTOGRAM_OBSERVE(*this, disk_io_latency_us, latency_us);
        COUNTER_INCREMENT(*this, disk_io_ops_total, 1);

        t_disk_io_timings.erase(it);
    }
}

} // namespace ublkpp
