#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <string>

#include <sisl/metrics/metrics.hpp>

struct ublk_io_data;
struct ublksrv_queue;

namespace ublkpp {

struct io_timing {
    std::chrono::steady_clock::time_point start_time;
};

// FSDisk-level metrics - tracks individual disk operations
//
// Constructor parameters:
//   parent_id: The ID of the parent device that contains this disk. This is used as a label
//              to correlate metrics across the device hierarchy (e.g., RAID -> FSDisk).
//              For standalone disks not part of a RAID, you can use the disk's own ID.
//   disk_path: The filesystem path or identifier for this specific disk instance.
struct UblkFSDiskMetrics : public sisl::MetricsGroup {
    UblkFSDiskMetrics(std::string const& parent_id, std::string const& disk_path);
    ~UblkFSDiskMetrics();

    // Keyed on (instance, tag) so fan-out I/O delivering the same tag to multiple FSDisk
    // instances on the same queue thread does not alias entries.
    static inline thread_local std::map< std::pair< const UblkFSDiskMetrics*, uint16_t >, io_timing > t_disk_io_timings;

    void record_io_start(ublk_io_data const* data);
    void record_io_complete(ublk_io_data const* data);
};

} // namespace ublkpp
