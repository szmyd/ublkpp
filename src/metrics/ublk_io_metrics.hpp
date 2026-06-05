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
    // FLUSH note: UBLK_IO_OP_FLUSH is exempted from the gate and never increments these
    // counters (record_queue_depth_change is a no-op for op values other than 0/1). A FLUSH
    // completing while _shutting_down=true therefore cannot produce a spurious drain signal.
    //
    // TOCTOU between the two loads: between reading _queued_reads and _queued_writes, a new
    // op could increment then decrement one of them (rejected at gate). This produces a false
    // negative (sees non-zero when both are effectively zero), not a false positive — the op
    // never touches device*. False negatives just defer device.reset() to the last
    // decrementer's own drain check; never lost. The CAS on _device_reset_done is the
    // real safeguard against double-execution.
    //
    // Memory ordering: both the counter RMWs and these loads are seq_cst so all three
    // participate in the C++ total order S alongside begin_shutdown's seq_cst store.
    // Formal guarantee: if the op's increment precedes begin_shutdown's store in S, these
    // loads (sequenced after the store in begin_shutdown's thread) see the increment.
    // If the store precedes the increment in S, the gate check (sequenced after the increment)
    // sees _shutting_down=true and rejects without touching device*. No UAF in either case.
    bool all_idle() const {
        return _queued_reads.load(std::memory_order_seq_cst) == 0 &&
            _queued_writes.load(std::memory_order_seq_cst) == 0;
    }
};

} // namespace ublkpp
