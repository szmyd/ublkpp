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
    // Called from two sites with different safety arguments:
    //
    // begin_shutdown(): pairs with a seq_cst store+fence that form a total order S with each
    //   op's seq_cst fence (between its relaxed increment and its acquire gate check). In any
    //   execution, either the op's increment is visible here (false positive avoided) or the
    //   op's gate check sees _shutting_down=true (op rejects, never touches device*). No UAF.
    //
    // Drain check in __handle_io_async(): no explicit fence. False negatives (stale non-zero
    //   from another thread's not-yet-propagated relaxed decrement) just defer device.reset()
    //   to the last decrementing thread's own drain check — never lost. False positives (seeing
    //   zero while an op is between its increment and its fence) are still prevented by that
    //   op's seq_cst fence: the fence makes the op visible either to begin_shutdown's total-order
    //   read or ensures the op's gate check sees _shutting_down=true. The CAS on
    //   _device_reset_done prevents double-execution in both cases.
    bool all_idle() const {
        return _queued_reads.load(std::memory_order_acquire) == 0 &&
            _queued_writes.load(std::memory_order_acquire) == 0;
    }
};

} // namespace ublkpp
