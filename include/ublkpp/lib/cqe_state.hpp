#pragma once

#include <coroutine>
#include <cstdint>
#include <utility>
#include <vector>

#include <liburing.h>
#include <sisl/async/cqe_state.hpp> // shared managed-user-data encode/decode/is_managed helpers
#include <sisl/logging/logging.h>
#include <ublksrv.h>

namespace ublkpp {

// =============================================================================
// Queue io_uring integration contract for custom ublk_disk drivers.
//
// A custom driver that wants its CQEs routed back into a coroutine resumption
// (rather than running synchronously or off-thread) follows this protocol
// inside its async_iov override:
//
//   disk_task<int> MyDisk::async_iov(ublksrv_queue const* q, ublk_io_data const* d, ...) {
//       auto* sqe = next_sqe(q);
//       auto [state, user_data] = build_cqe_state_data(d);
//       io_uring_prep_*(sqe, ...);
//       io_uring_sqe_set_data64(sqe, user_data);
//       co_return co_await *state;
//   }
//
// The queue's CQE loop (run_queue_loop in src/target/ublkpp_tgt.cpp) inspects
// sisl::async::is_managed_user_data(cqe->user_data): if set, the bits decode (via
// sisl::async::decode_managed_user_data) to a cqe_state*; the loop writes _result, marks
// _result_ready, and resumes _waiter. If clear, the CQE is delegated to ublksrv.
//
// Two flavors of cqe_state:
//   - Per-IO (build_cqe_state_data path): allocated in async_io::_pool, owned
//     by the IO slot, _owner non-null so the loop can call ublksrv_complete_io
//     with -EIO if the coroutine throws.
//   - Stand-alone service loops: coroutine-frame-local, _owner = nullptr, and
//     callers handle their own errors. Encode the user_data manually with
//     sisl::async::encode_managed_user_data(state).
//
// The user_data managed-bit encoding (bit 63) is shared with sisl/iomgr via
// sisl::async::{encode,decode,is_managed}_user_data; probe-timeout CQEs encode a null pointer
// (encode_managed_user_data(nullptr)) and run_queue_loop checks state == nullptr to tell them
// apart from real I/O CQEs.
//
// Reference implementation: src/driver/fs_disk.cpp.
// =============================================================================

struct cqe_state;

// Per-IO state tracking one inflight request, owned by the ublksrv-allocated io_data slot.
//
// Lifetime: placement-new'd in init_queue for each tag slot; explicitly ~async_io() in
// deinit_queue. _pool is cleared at the start of each new I/O in __handle_io_async (the
// tgt C callback).
struct async_io {
    // Pre-reserved in init_queue to prepare_result::max_sqes_per_io. push_back never
    // reallocates when size < capacity, so cqe_state* pointers in SQE user_data stay stable.
    std::vector< cqe_state > _pool{};
    int _tag{-1}; // set in tgt __handle_io_async; read by run_queue_loop on error

    // Allocates a fresh cqe_state in the _pool and returns a stable pointer to it.
    cqe_state* next_state();
};

// Tracks a single inflight sub-operation. Stored in async_io::_pool (pre-reserved std::vector,
// so push_back is pointer-stable). _result and _result_ready are written by run_queue_loop before resuming
// _waiter. Implements the awaitable protocol directly: co_await *state suspends until the CQE
// arrives.
//
// _owner is nullable: per-IO cqe_states (build_cqe_state_data path) point at the slot's
// async_io so an exception on resume can be reported via ublksrv_complete_io. Stand-alone
// cqe_states set _owner = nullptr; callers handle their own errors.
struct cqe_state {
    async_io* _owner{nullptr};
    int _result{0};
    bool _result_ready{false};
    std::coroutine_handle<> _waiter{};

    bool await_ready() const noexcept { return _result_ready; }
    void await_suspend(std::coroutine_handle<> h) noexcept { _waiter = h; }
    int await_resume() const noexcept { return _result; }
};

inline cqe_state* async_io::next_state() {
    RELEASE_ASSERT_LT(_pool.size(), _pool.capacity(),
                      "cqe_state pool exhausted; prepare_result::max_sqes_per_io underestimated")
    _pool.push_back(cqe_state{._owner = this});
    return &_pool.back();
}

// Allocates a new cqe_state for this I/O and encodes it for SQE user_data. Returns
// {state*, encoded_user_data}. The caller co_awaits *state.
inline std::pair< cqe_state*, uint64_t > build_cqe_state_data(ublk_io_data const* data) {
    auto* state = reinterpret_cast< async_io* >(data->private_data)->next_state();
    return {state, sisl::async::encode_managed_user_data(state)};
}

// Acquires an SQE from the queue's io_uring, submitting any pending SQEs first if the ring
// is full. Returns nullptr only if the kernel cannot allocate one even after submission;
// callers should treat that as a transient back-pressure signal.
inline io_uring_sqe* next_sqe(ublksrv_queue const* q) {
    auto* r = q->ring_ptr;
    if (0 == io_uring_sq_space_left(r)) [[unlikely]]
        if (io_uring_submit(r) < 0) return nullptr;
    return io_uring_get_sqe(r);
}

} // namespace ublkpp
