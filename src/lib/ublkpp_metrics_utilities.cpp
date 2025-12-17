#include "ublkpp_metrics_utilities.hpp"

#include <sisl/metrics/metrics.hpp>
#include <ublksrv.h>

#include "target/ublkpp_tgt_impl.hpp"

namespace ublkpp {

// Thread-local timing map definition
thread_local std::map<std::pair<uint16_t, uint16_t>, io_timing> t_io_timings;

void record_io_start(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint8_t device_id) {
    if (!q || !q->private_data || !data) return;

    auto key = std::make_pair(static_cast<uint16_t>(data->tag), static_cast<uint16_t>(sub_cmd));
    t_io_timings[key] = io_timing{std::chrono::steady_clock::now(), device_id};
}

void record_io_complete(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) {
    if (!q || !q->private_data || !data) return;

    auto tgt = static_cast<ublkpp_tgt_impl*>(q->private_data);
    auto key = std::make_pair(static_cast<uint16_t>(data->tag), static_cast<uint16_t>(sub_cmd));

    if (auto it = t_io_timings.find(key); it != t_io_timings.end()) {
        auto const& timing = it->second;
        auto const end_time = std::chrono::steady_clock::now();
        auto const latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - timing.start_time).count();

        // Record to appropriate histogram
        if (timing.device_id == 0) {
            HISTOGRAM_OBSERVE(tgt->metrics, device0_latency_us, latency_us);
        } else if (timing.device_id == 1) {
            HISTOGRAM_OBSERVE(tgt->metrics, device1_latency_us, latency_us);
        }

        t_io_timings.erase(it);
    }
}

void record_device_degraded(ublksrv_queue const* q, uint8_t device_id) {
    if (!q || !q->private_data) return;

    auto tgt = static_cast<ublkpp_tgt_impl*>(q->private_data);

    if (device_id == 0) {
        HISTOGRAM_OBSERVE(tgt->metrics, device0_degraded_count, 1);
    } else if (device_id == 1) {
        HISTOGRAM_OBSERVE(tgt->metrics, device1_degraded_count, 1);
    }
}

void record_queue_depth_change(ublksrv_queue const* q, uint8_t op, bool is_increment) {
    if (!q || !q->private_data) return;

    auto tgt = static_cast<ublkpp_tgt_impl*>(q->private_data);

    // UBLK_IO_OP_READ = 0, UBLK_IO_OP_WRITE = 1
    if (op == 0) { // UBLK_IO_OP_READ
        if (is_increment) {
            auto const depth = tgt->_queued_reads.fetch_add(1, std::memory_order_relaxed) + 1;
            HISTOGRAM_OBSERVE(tgt->metrics, ublk_read_queue_distribution, depth);
        } else {
            tgt->_queued_reads.fetch_sub(1, std::memory_order_relaxed);
        }
    } else if (op == 1) { // UBLK_IO_OP_WRITE
        if (is_increment) {
            auto const depth = tgt->_queued_writes.fetch_add(1, std::memory_order_relaxed) + 1;
            HISTOGRAM_OBSERVE(tgt->metrics, ublk_write_queue_distribution, depth);
        } else {
            tgt->_queued_writes.fetch_sub(1, std::memory_order_relaxed);
        }
    }
}

} // namespace ublkpp
