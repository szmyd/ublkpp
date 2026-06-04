#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include <sisl/metrics/metrics.hpp>

struct ublksrv_queue;

namespace ublkpp {

// Top-level I/O metrics - tracks queue depth and operation counts at the ublkpp target layer
//
// Constructor parameters:
//   uuid: The volume/target UUID for this ublkpp target instance.
struct UblkIOMetrics : public sisl::MetricsGroup {
    explicit UblkIOMetrics(std::string const& uuid);
    ~UblkIOMetrics();

    std::atomic< uint64_t > _queued_reads{0};
    std::atomic< uint64_t > _queued_writes{0};

    void record_queue_depth_change(ublksrv_queue const* q, uint8_t op, bool is_increment);

    // Returns true when both read and write counters are zero. Two separate loads — safe for
    // drain detection because the CAS on _device_reset_done prevents double-execution even if
    // the window between the two loads is observed non-atomically.
    bool all_idle() const {
        return _queued_reads.load(std::memory_order_acquire) == 0 &&
            _queued_writes.load(std::memory_order_acquire) == 0;
    }
};

} // namespace ublkpp
