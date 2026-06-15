#include "ublk_io_metrics.hpp"

#include <ublksrv.h>

namespace ublkpp {

UblkIOMetrics::UblkIOMetrics(std::string const& uuid) : sisl::MetricsGroup{"ublk_io_metrics", uuid} {
    REGISTER_HISTOGRAM(ublk_read_queue_distribution, "Read queue depth distribution",
                       HistogramBucketsType(ExponentialOfTwoBuckets));
    REGISTER_HISTOGRAM(ublk_write_queue_distribution, "Write queue depth distribution",
                       HistogramBucketsType(ExponentialOfTwoBuckets));
    register_me_to_farm();
}

UblkIOMetrics::~UblkIOMetrics() { deregister_me_from_farm(); }

void UblkIOMetrics::record_queue_depth_change(ublksrv_queue const* q, uint8_t op, bool is_increment) {
    if (!q || !q->private_data) return;

    // UBLK_IO_OP_READ = 0, UBLK_IO_OP_WRITE = 1
    //
    // seq_cst: makes these RMWs participate in the C++ total order S alongside the seq_cst
    // store in begin_shutdown(). Under the C++ abstract machine, seq_cst operations on
    // different objects are formally ordered via S — either this increment precedes
    // begin_shutdown's store in S (begin_shutdown's counter read sees it) or the store
    // precedes the increment in S (the gate check that follows sees _shutting_down=true).
    // acq_rel alone does not participate in S and gives no formal cross-variable guarantee.
    // On x86, seq_cst compiles to the same lock xadd as acq_rel; no performance difference.
    if (op == 0) { // UBLK_IO_OP_READ
        if (is_increment) {
            auto const depth = _queued_reads.fetch_add(1, std::memory_order_seq_cst) + 1;
            HISTOGRAM_OBSERVE(*this, ublk_read_queue_distribution, depth);
        } else {
            _queued_reads.fetch_sub(1, std::memory_order_seq_cst);
        }
    } else if (op == 1) { // UBLK_IO_OP_WRITE
        if (is_increment) {
            auto const depth = _queued_writes.fetch_add(1, std::memory_order_seq_cst) + 1;
            HISTOGRAM_OBSERVE(*this, ublk_write_queue_distribution, depth);
        } else {
            _queued_writes.fetch_sub(1, std::memory_order_seq_cst);
        }
    } else if (op == 3 || op == 5) { // UBLK_IO_OP_DISCARD, UBLK_IO_OP_WRITE_ZEROES
        if (is_increment) {
            _queued_other.fetch_add(1, std::memory_order_seq_cst);
        } else {
            _queued_other.fetch_sub(1, std::memory_order_seq_cst);
        }
    }
}

} // namespace ublkpp
