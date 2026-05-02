#pragma once

#include <coroutine>
#include <cstdint>
#include <deque>
#include <utility>

#include <sisl/logging/logging.h>
#include <ublksrv.h>

namespace ublkpp {

struct cqe_state;

// Per-IO state tracking one inflight request, owned by the ublksrv-allocated io_data slot.
//
// Lifetime: placement-new'd in init_queue for each tag slot; explicitly ~async_io() in deinit_queue.
// _pool is cleared at the start of each new I/O in __handle_io_async (the tgt C callback).
//
// Dispatch protocol:
//   1. async_iov calls build_cqe_state_data() -> next_state() per SQE; _pool grows.
//   2. Coroutine suspends on co_await *state; state->_waiter is installed.
//   3. run_queue_loop decodes cqe_state*, sets _result + _result_ready, resumes state->_waiter.
//   4. cqe_state::await_resume returns state->_result directly.
struct async_io {
    std::deque< cqe_state > _pool{}; // stable addresses: push_back never invalidates pointers
    int _tag{-1};                    // set in tgt __handle_io_async; read by run_queue_loop on error

    // Allocates a fresh cqe_state in the _pool and returns a stable pointer to it.
    cqe_state* next_state();
};

// Tracks a single inflight sub-operation. Stored in async_io::_pool (std::deque, so push_back
// is pointer-stable). _result and _result_ready are written by run_queue_loop before resuming _waiter.
// Implements the awaitable protocol directly: co_await *state suspends until the CQE arrives.
//
// _owner is nullable: per-IO cqe_states allocated via build_cqe_state_data point at the slot's
// async_io so an exception on resume can be reported with ublksrv_complete_io. Long-lived
// stand-alone cqe_states (e.g. iSCSIDisk's POLL_ADD service loop) are coroutine-frame-local
// with no IO slot, and set _owner = nullptr; callers awaiting these states must handle their
// own errors.
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
    _pool.push_back(cqe_state{._owner = this});
    return &_pool.back();
}

// Allocates a new cqe_state for this I/O and encodes it for SQE user_data.
// Returns {state*, encoded_user_data}. The caller co_awaits *state.
//
// On ARM64/x86_64 canonical userspace addresses use <=48 bits, so bit 63 is always zero in any
// valid pointer — the OR is safe and reversible with & ~(1ULL << 63).
inline std::pair< cqe_state*, uint64_t > build_cqe_state_data(ublk_io_data const* data) {
    auto* state = reinterpret_cast< async_io* >(data->private_data)->next_state();
    return {state, reinterpret_cast< uint64_t >(state) | (1ULL << 63)};
}

} // namespace ublkpp
