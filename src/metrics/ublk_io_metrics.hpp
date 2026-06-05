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
    // TOCTOU between the two loads: between reading _queued_reads and _queued_writes, a new
    // op could increment then decrement one of them (rejected at gate). This produces a false
    // negative (sees non-zero when both are effectively zero), not a false positive — the op
    // never touches device*. False negatives just defer device.reset() to the last
    // decrementer's own drain check; never lost. The CAS on _device_reset_done is the
    // real safeguard against double-execution.
    //
    // Memory ordering: counter operations use acq_rel (release makes the increment globally
    // visible; acquire pairs with begin_shutdown's seq_cst store). Together they ensure:
    // either the increment is visible to begin_shutdown's counter reads (no false drain) or
    // begin_shutdown's store is visible to the op's gate check (op rejects, no device* access).
    bool all_idle() const {
        return _queued_reads.load(std::memory_order_acquire) == 0 &&
            _queued_writes.load(std::memory_order_acquire) == 0;
    }
};

} // namespace ublkpp
