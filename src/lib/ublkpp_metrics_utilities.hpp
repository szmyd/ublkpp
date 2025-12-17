#pragma once

#include <chrono>
#include <map>

#include "ublkpp/lib/sub_cmd.hpp"

struct ublk_io_data;
struct ublksrv_queue;

namespace ublkpp {

struct io_timing {
    std::chrono::steady_clock::time_point start_time;
    uint8_t device_id;
};

// Thread-local timing map: each queue handler thread tracks its own I/Os
// Key: (tag, sub_cmd) uniquely identifies an I/O within this thread
extern thread_local std::map<std::pair<uint16_t, uint16_t>, io_timing> t_io_timings;

// Metrics recording functions - called directly from drivers and target
void record_io_start(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint8_t device_id);
void record_io_complete(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd);
void record_device_degraded(ublksrv_queue const* q, uint8_t device_id);

} // namespace ublkpp
