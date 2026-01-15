#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <string>

#include <sisl/metrics/metrics.hpp>

#include "ublkpp/lib/sub_cmd.hpp"

struct ublk_io_data;
struct ublksrv_queue;

namespace ublkpp {

struct io_timing {
    std::chrono::steady_clock::time_point start_time;
};

// FSDisk-level metrics - tracks individual disk operations
// Labels: raid_device_id, disk_path
struct UblkFSDiskMetrics : public sisl::MetricsGroupWrapper {
    UblkFSDiskMetrics(std::string const& raid_device_id, std::string const& disk_path);
    ~UblkFSDiskMetrics();

    static inline thread_local std::map<std::pair<uint16_t, uint16_t>, io_timing> t_disk_io_timings;

    void record_io_start(ublk_io_data const* data, sub_cmd_t sub_cmd);
    void record_io_complete(ublk_io_data const* data, sub_cmd_t sub_cmd);
};

} // namespace ublkpp
