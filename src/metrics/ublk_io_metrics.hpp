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
    // Tracks DISCARD (op=3) and WRITE_ZEROES (op=5) in-flight ops. These ops access device*
    // via async_iov just like reads/writes, but record_queue_depth_change is a no-op for their
    // op codes. Without this counter, all_idle() could return true while a DISCARD/WRITE_ZEROES
    // coroutine is suspended at co_await device->async_iov — causing a UAF when device.reset()
    // fires. Same seq_cst protocol as _queued_reads/_queued_writes.
    std::atomic< uint64_t > _queued_other{0};
    std::atomic< uint64_t > _read_bytes_total{0};
    std::atomic< uint64_t > _write_bytes_total{0};
    std::atomic< uint64_t > _read_errors{0};
    std::atomic< uint64_t > _write_errors{0};

    void record_queue_depth_change(ublksrv_queue const* q, uint8_t op, bool is_increment);
    void apply_op_for_test(uint8_t op, bool is_increment);
    void record_io_bytes(uint8_t op, uint32_t bytes);
    void record_io_latency(uint8_t op, uint64_t microseconds);
    void record_io_error(uint8_t op);

    // Returns true when no ops that access device* are in flight.
    //
    // Covered ops: READ (0), WRITE (1), DISCARD (3), WRITE_ZEROES (5). All four access
    // device* via async_iov. FLUSH (2) is excluded: it short-circuits before the else-branch
    // and never dereferences device*. Three separate seq_cst loads; the TOCTOU between them
    // produces only false negatives (deferred drain) never false positives (premature reset).
    // The CAS on _device_reset_done is the real guard against double-execution.
    //
    // Memory ordering: all counter RMWs and these loads are seq_cst, participating in the
    // C++ total order S alongside begin_shutdown's seq_cst store. Either an op's increment
    // precedes the store in S (begin_shutdown's counter reads see it → skip reset) or the
    // store precedes the increment in S (the gate check sees _shutting_down=true → reject).
    bool all_idle() const {
        return _queued_reads.load(std::memory_order_seq_cst) == 0 &&
            _queued_writes.load(std::memory_order_seq_cst) == 0 && _queued_other.load(std::memory_order_seq_cst) == 0;
    }
};

} // namespace ublkpp
