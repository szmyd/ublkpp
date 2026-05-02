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
        _q_depth(q_depth),
        _disk(std::move(disk)),
        _tags(q_depth),
        _queues(nr_queues),
        _io_states(q_depth),
        _async_tasks(q_depth) {
    // q_depth * 4 gives headroom for RAID1 write amplification (2x replicas +
    // 2x bitmap SQEs per user write) without false "ring full" auto-submits.
    if (io_uring_queue_init(q_depth * 4, &_ring, 0) < 0) throw std::runtime_error("io_uring_queue_init failed");

    // Populate ublksrv_dev so disk code can reach tgt_data and ring
    _dev.tgt.tgt_data = _disk.get();
    _dev.tgt.dev_size = _disk->capacity();
    _dev.tgt.tgt_ring_depth = static_cast< unsigned >(q_depth);
    _dev.tgt.nr_fds = 1; // slot 0 reserved (mirrors ublkpp_tgt convention)

    // Populate ublksrv_queue structs before calling prepare so that any implementation
    // which reads ring_ptr or dev inside prepare sees valid values.
    for (int qi = 0; qi < nr_queues; ++qi) {
        _queues[qi].q_id = qi;
        _queues[qi].q_depth = q_depth;
        _queues[qi].ring_ptr = &_ring;
        _queues[qi].dev = &_dev;
        _queues[qi].private_data = nullptr;
    }

    // Simulate init_queue: call prepare once per queue thread so the disk can count queues
    // and perform per-queue initialization (e.g. Raid1DiskImpl sets _nr_hw_queues and enables resync).
    for (int qi = 0; qi < nr_queues; ++qi) {
        for (auto const fd : _disk->prepare(&_queues[qi], _dev.tgt.nr_fds)) {
            if (_dev.tgt.nr_fds < UBLKSRV_TGT_MAX_FDS) _dev.tgt.fds[_dev.tgt.nr_fds++] = fd;
        }
    }

    // Wire up per-tag data.iod pointers and async_io backing storage
    for (int tag = 0; tag < q_depth; ++tag) {
        _tags[tag].data.tag = tag;
        _tags[tag].data.iod = &_tags[tag].iod;
        _tags[tag].data.private_data = &_io_states[tag];
        _io_states[tag]._tag = tag;
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

    // Reset async_io state between IOs on the same tag slot
    _io_states[tag]._pool.clear();
    _async_tasks[tag].reset();

    if (op == UBLK_IO_OP_FLUSH) {
        auto flush_task = []() -> disk_task< int > { co_return 0; }();
        flush_task._coro.resume();
        _async_tasks[tag].emplace(std::move(flush_task));
        return io_result{0};
    }
    ts.iov.iov_base = reinterpret_cast< void* >(ts.iod.addr);
    ts.iov.iov_len = ts.iod.nr_sectors << SECTOR_SHIFT;
    auto task = _disk->async_iov(&_queues[0], &ts.data, &ts.iov, 1, ts.iod.start_sector << SECTOR_SHIFT);
    task._coro.resume(); // start lazy coroutine; runs until first co_await *state
    _async_tasks[tag].emplace(std::move(task));
    // Pool size == number of CqeStates registered (one per pending stripe SQE)
    return io_result{_io_states[tag]._pool.size()};
}

void MockUblksrv::process_cqe(io_uring_cqe* cqe, std::vector< Completion >& out) {
    auto* state = reinterpret_cast< cqe_state* >(cqe->user_data & ~(1ULL << 63));
    int const tag = state->_owner->_tag;
    int const res = cqe->res;

    // Consume the CQE immediately so peek sees the next one
    io_uring_cqe_seen(&_ring, cqe);

    state->_result = res;
    state->_result_ready = true;

    if (auto h = std::exchange(state->_waiter, {})) h.resume();
    auto& opt = _async_tasks[tag];
    if (opt && opt->_coro.done()) out.push_back({tag, opt->_coro.promise()._value});
}

std::vector< MockUblksrv::Completion > MockUblksrv::inject_cqe(int tag, int result) {
    std::vector< Completion > out;
    // Find the cqe_state currently suspended in the disk_task (_waiter is set)
    cqe_state* target = nullptr;
    for (auto& s : _io_states[tag]._pool) {
        if (s._waiter) {
            target = &s;
            break;
        }
    }
    auto& opt = _async_tasks[tag];
    if (!target) {
        // No suspended state — task may have completed synchronously (e.g. flush).
        // result is ignored; return the task's value if it finished.
        if (opt && opt->_coro.done()) out.push_back({tag, opt->_coro.promise()._value});
        return out;
    }
    target->_result = result;
    target->_result_ready = true;
    if (auto h = std::exchange(target->_waiter, {})) h.resume();
    if (opt && opt->_coro.done()) out.push_back({tag, opt->_coro.promise()._value});
    return out;
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

        // Match the production queue loop: submit pending SQEs and wait for at
        // least one CQE in a single syscall. Callers of async_iov rely on the
        // event loop to submit; using io_uring_wait_cqe_timeout here would leave
        // queued SQEs in the SQ forever and hang.
        io_uring_cqe* cqe = nullptr;
        int r = io_uring_submit_and_wait_timeout(&_ring, &cqe, 1, &ts, nullptr);
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
