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
    // Safety on x86 (TSO): once _shutting_down=true is in the coherent cache, all cores see
    // it — no op can pass the gate and reach device* after that point. False negatives (stale
    // non-zero from another thread's not-yet-propagated relaxed decrement) just defer
    // device.reset() to the last decrementing thread's own drain check; never lost. The CAS
    // on _device_reset_done prevents double-execution.
    //
    // ARM note: would require paired seq_cst fences (increment→fence→gate in each op, and
    // fence→counter-read in begin_shutdown) to close the relaxed-increment / gate-check race.
    // Not implemented: we target x86 only and atomic_thread_fence is unsupported by TSan.
    bool all_idle() const {
        return _queued_reads.load(std::memory_order_acquire) == 0 &&
            _queued_writes.load(std::memory_order_acquire) == 0;
    }
};

} // namespace ublkpp
