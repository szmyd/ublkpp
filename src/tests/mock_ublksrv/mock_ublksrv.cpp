#include "mock_ublksrv.hpp"

extern "C" {
#include <unistd.h>
}

#include <stdexcept>

#include <ublksrv.h>

#include "ublkpp/lib/common.hpp"

namespace ublkpp {

// Matches ublkpp_tgt default max_io_buf_bytes (DEF_BUF_SIZE = 512 KiB)
static constexpr size_t k_max_io_size = DEF_BUF_SIZE;
static constexpr size_t k_sector_align = 512;

MockUblksrv::MockUblksrv(std::shared_ptr< UblkDisk > disk, int q_depth, int nr_queues) :
        _q_depth(q_depth), _disk(std::move(disk)), _tags(q_depth), _queues(nr_queues) {
    // q_depth * 4 gives headroom for RAID1 write amplification (2x replicas +
    // 2x bitmap SQEs per user write) without false "ring full" auto-submits.
    if (io_uring_queue_init(q_depth * 4, &_ring, 0) < 0) throw std::runtime_error("io_uring_queue_init failed");

    // Populate ublksrv_dev so disk code can reach tgt_data and ring
    _dev.tgt.tgt_data = _disk.get();
    _dev.tgt.dev_size = _disk->capacity();
    _dev.tgt.tgt_ring_depth = static_cast< unsigned >(q_depth);
    _dev.tgt.nr_fds = 1; // slot 0 reserved (mirrors ublkpp_tgt convention)

    // Simulate init_queue: call open_for_uring once per queue thread so the disk can count queues
    // and perform per-queue initialization (e.g. Raid1DiskImpl sets _nr_hw_queues and enables resync).
    for (int qi = 0; qi < nr_queues; ++qi) {
        for (auto const fd : _disk->open_for_uring(&_queues[qi], _dev.tgt.nr_fds)) {
            if (_dev.tgt.nr_fds < UBLKSRV_TGT_MAX_FDS) _dev.tgt.fds[_dev.tgt.nr_fds++] = fd;
        }
    }

    // Populate ublksrv_queue structs — the only fields disk code actually reads are
    // ring_ptr (for io_uring_get_sqe) and dev (for dev->tgt.tgt_data)
    for (int qi = 0; qi < nr_queues; ++qi) {
        _queues[qi].q_id = qi;
        _queues[qi].q_depth = q_depth;
        _queues[qi].ring_ptr = &_ring;
        _queues[qi].dev = &_dev;
        _queues[qi].private_data = nullptr;
    }

    // Wire up per-tag data.iod pointers
    for (int tag = 0; tag < q_depth; ++tag) {
        _tags[tag].data.tag = tag;
        _tags[tag].data.iod = &_tags[tag].iod;
        _tags[tag].data.private_data = nullptr;
    }

    // Allocate sector-aligned I/O buffers (one per tag)
    size_t const stride = k_max_io_size + k_sector_align;
    _io_buf_storage.resize(q_depth * stride);
    _io_buf_ptrs.resize(q_depth);
    for (int tag = 0; tag < q_depth; ++tag) {
        auto raw = reinterpret_cast< uintptr_t >(_io_buf_storage.data() + tag * stride);
        auto aligned = (raw + k_sector_align - 1) & ~(k_sector_align - 1);
        _io_buf_ptrs[tag] = reinterpret_cast< void* >(aligned);
    }
}

MockUblksrv::~MockUblksrv() {
    io_uring_queue_exit(&_ring);
    for (unsigned i = 1; i < _dev.tgt.nr_fds; ++i) {
        if (_dev.tgt.fds[i] > 0) close(_dev.tgt.fds[i]);
    }
}

io_result MockUblksrv::submit_io(int tag, uint8_t op, uint64_t start_sector, uint32_t nr_sectors, void* buf) {
    auto& ts = _tags[tag];
    ts.iod.op_flags = op;
    ts.iod.nr_sectors = nr_sectors;
    ts.iod.start_sector = start_sector;
    ts.iod.addr = reinterpret_cast< uint64_t >(buf);
    ts.sub_cmds_remaining = 0;
    ts.result = 0;

    auto res = _disk->queue_tgt_io(&_queues[0], &ts.data, 0);
    if (!res) return res;

    // Commit all SQEs queued by queue_tgt_io before we start polling
    io_uring_submit(&_ring);

    ts.sub_cmds_remaining = static_cast< int >(res.value());
    return res;
}

void MockUblksrv::process_cqe(io_uring_cqe* cqe, std::vector< Completion >& out) {
    int const tag = static_cast< int >(user_data_to_tag(cqe->user_data));
    auto const sub_cmd = static_cast< sub_cmd_t >(user_data_to_tgt_data(cqe->user_data));
    int const res = cqe->res;

    // Consume the CQE immediately so peek sees the next one
    io_uring_cqe_seen(&_ring, cqe);

    auto& ts = _tags[tag];
    _disk->on_io_complete(&ts.data, sub_cmd, res);

    if (is_internal(sub_cmd)) {
        // RAID1 bitmap completion — respond and potentially enqueue more SQEs
        ts.sub_cmds_remaining--;
        auto io_res = _disk->queue_internal_resp(&_queues[0], &ts.data, sub_cmd, res);
        if (io_res && io_res.value() > 0) {
            ts.sub_cmds_remaining += static_cast< int >(io_res.value());
            io_uring_submit(&_ring);
        }
    } else {
        ts.result += res;
        ts.sub_cmds_remaining--;
    }

    if (ts.sub_cmds_remaining == 0) { out.push_back({tag, ts.result}); }
}

std::vector< MockUblksrv::Completion > MockUblksrv::poll(int min_completions, std::chrono::milliseconds timeout) {
    std::vector< Completion > completions;
    auto const deadline = std::chrono::steady_clock::now() + timeout;

    while (static_cast< int >(completions.size()) < min_completions) {
        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) break;

        auto remaining_ms = std::chrono::duration_cast< std::chrono::milliseconds >(deadline - now);
        __kernel_timespec ts{.tv_sec = remaining_ms.count() / 1000,
                             .tv_nsec = (remaining_ms.count() % 1000) * 1'000'000LL};

        io_uring_cqe* cqe = nullptr;
        int r = io_uring_wait_cqe_timeout(&_ring, &cqe, &ts);
        if (r == -ETIME || r == -EINTR || r < 0 || cqe == nullptr) break;

        // Process this CQE then drain any additional ones that are ready
        do {
            process_cqe(cqe, completions);
        } while (io_uring_peek_cqe(&_ring, &cqe) == 0 && cqe != nullptr);
    }

    return completions;
}

void* MockUblksrv::io_buf(int tag) { return _io_buf_ptrs[tag]; }

uint64_t MockUblksrv::capacity_sectors() const noexcept { return _disk->capacity() >> SECTOR_SHIFT; }

} // namespace ublkpp
