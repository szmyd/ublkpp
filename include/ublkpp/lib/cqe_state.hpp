#pragma once

#include <coroutine>
#include <cstdint>
#include <deque>
#include <utility>

#include <sisl/logging/logging.h>
#include <ublksrv.h>

namespace ublkpp {

struct CqeState;

// Per-IO state tracking one inflight request, owned by the ublksrv-allocated io_data slot.
//
// Lifetime: placement-new'd in init_queue for each tag slot; explicitly ~async_io() in deinit_queue.
// pool is cleared at the start of each new I/O in __handle_io_async (the tgt C callback).
//
// Dispatch protocol:
//   1. async_iov calls build_cqe_state_data() -> next_state() per SQE; pool grows.
//   2. Coroutine suspends on co_await *state; state->waiter is installed.
//   3. run_queue_loop decodes CqeState*, sets result + result_ready, resumes state->waiter.
//   4. CqeState::await_resume returns state->result directly.
struct async_io {
    std::deque< CqeState > pool{}; // stable addresses: push_back never invalidates pointers
    int tag{-1};                   // set in tgt __handle_io_async; read by run_queue_loop on error

    // Allocates a fresh CqeState in the pool and returns a stable pointer to it.
    CqeState* next_state();
};

// Tracks a single inflight sub-operation. Stored in async_io::pool (std::deque, so push_back
// is pointer-stable). result and result_ready are written by run_queue_loop before resuming waiter.
// Implements the awaitable protocol directly: co_await *state suspends until the CQE arrives.
struct CqeState {
    async_io* owner;
    int result{0};
    bool result_ready{false};
    std::coroutine_handle<> waiter{};

    bool await_ready() const noexcept { return result_ready; }
    void await_suspend(std::coroutine_handle<> h) noexcept { waiter = h; }
    int await_resume() const noexcept { return result; }
};

inline CqeState* async_io::next_state() {
    pool.push_back(CqeState{.owner = this});
    return &pool.back();
}

// Allocates a new CqeState for this I/O and encodes it for SQE user_data.
// Returns {state*, encoded_user_data}. The caller co_awaits *state.
//
// On ARM64/x86_64 canonical userspace addresses use <=48 bits, so bit 63 is always zero in any
// valid pointer — the OR is safe and reversible with & ~(1ULL << 63).
inline std::pair< CqeState*, uint64_t > build_cqe_state_data(ublk_io_data const* data) {
    auto* state = reinterpret_cast< async_io* >(data->private_data)->next_state();
    return {state, reinterpret_cast< uint64_t >(state) | (1ULL << 63)};
}

} // namespace ublkpp
