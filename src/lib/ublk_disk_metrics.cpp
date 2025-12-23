#include "ublk_disk_metrics.hpp"

namespace ublkpp {

UblkDiskMetrics::UblkDiskMetrics(std::string const& device_name) : sisl::MetricsGroup("UblkDisk", device_name.c_str()) {
    REGISTER_HISTOGRAM(ublk_write_queue_distribution, "Distribution of volume write queue depths",
                       HistogramBucketsType(SteppedUpto32Buckets));
    REGISTER_HISTOGRAM(ublk_read_queue_distribution, "Distribution of volume read queue depths",
                       HistogramBucketsType(SteppedUpto32Buckets));
    REGISTER_HISTOGRAM(device0_latency_us, "Device 0 I/O latency in microseconds",
                       HistogramBucketsType(ExponentialOfTwoBuckets));
    REGISTER_HISTOGRAM(device1_latency_us, "Device 1 I/O latency in microseconds",
                       HistogramBucketsType(ExponentialOfTwoBuckets));
    REGISTER_HISTOGRAM(device0_degraded_count, "Device 0 degradation events",
                       HistogramBucketsType(ExponentialOfTwoBuckets));
    REGISTER_HISTOGRAM(device1_degraded_count, "Device 1 degradation events",
                       HistogramBucketsType(ExponentialOfTwoBuckets));

    register_me_to_farm();
}

UblkDiskMetrics::~UblkDiskMetrics() { deregister_me_from_farm(); }

} // namespace ublkpp
