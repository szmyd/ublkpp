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

    // Returns true when both read and write counters are zero. Two separate loads.
    //
    // Safety (x86): begin_shutdown() uses a seq_cst store (MFENCE) before reading the
    // counters. This drains the store buffer and creates a total order with each op's
    // lock xadd increment: either the increment is visible here (false positive avoided) or
    // the op's subsequent acquire load of _shutting_down sees true (op rejects, no device*
    // access). False negatives (stale non-zero from a not-yet-committed decrement) just defer
    // device.reset() to the last decrementer's own drain check — never lost. The CAS on
    // _device_reset_done prevents double-execution in both call sites.
    bool all_idle() const {
        return _queued_reads.load(std::memory_order_acquire) == 0 &&
            _queued_writes.load(std::memory_order_acquire) == 0;
    }
};

} // namespace ublkpp
