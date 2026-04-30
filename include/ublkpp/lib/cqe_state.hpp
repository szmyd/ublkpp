#pragma once

#include <coroutine>
#include <cstdint>
#include <deque>
#include <utility>

#include <sisl/logging/logging.h>
#include <ublksrv.h>

#include <ublkpp/lib/sub_cmd.hpp>

namespace ublkpp {

struct CqeState;

// Per-IO state tracking one inflight request, owned by the ublksrv-allocated io_data slot.
//
// Lifetime: placement-new'd in init_queue for each tag slot; explicitly ~async_io() in deinit_queue.
// pool and completions are cleared at the start of each new I/O in __handle_io_async.
//
// Dispatch protocol:
//   1. queue_tgt_io calls build_cqe_state_data → ensure(sub_cmd) per SQE; pool grows.
//   2. run_queue_loop: CqeState* is decoded from user_data and push_completion is called.
//   3. push_completion resumes the CqeAwaiter-suspended coroutine if one is waiting.
//   4. CqeAwaiter::await_resume pops one entry from completions and returns it.
//   5. process_result inspects the state and may add to pending (retry / internal).
struct async_io {
    std::coroutine_handle<> waiter{};
    std::deque< CqeState > pool{};         // stable addresses: push_back never invalidates pointers
    std::deque< CqeState* > completions{}; // ordered queue of states ready for process_result
    uint32_t pending{0};
    int ret_val{0};
    int tag{-1}; // set in __handle_io_async; read by run_queue_loop and mock on error

    // Returns the CqeState for sub_cmd, creating it on first call. O(N) scan; N is small in practice.
    CqeState* ensure(sub_cmd_t sub_cmd);

    // Enqueues a completed state and resumes the coroutine if it is suspended on CqeAwaiter.
    void push_completion(CqeState* s) {
        completions.push_back(s);
        if (auto h = std::exchange(waiter, {})) h.resume();
    }
};

// Tracks a single inflight sub_cmd registered by build_cqe_state_data. Stored in async_io::pool
// (std::deque, so push_back is pointer-stable). result is written by tgt_io_done / handle_event
// before the state is enqueued in async_io::completions and the coroutine is resumed.
struct CqeState {
    async_io* owner;
    int result{0};
    sub_cmd_t sub_cmd{0};
};

inline CqeState* async_io::ensure(sub_cmd_t sub_cmd) {
    for (auto& s : pool)
        if (s.sub_cmd == sub_cmd) return &s;
    pool.push_back(CqeState{this, 0, sub_cmd});
    return &pool.back();
}

// Stores the CqeState* directly in the SQE user_data, OR'd with bit 63 to mark it as a target SQE.
// On ARM64/x86_64 canonical userspace addresses use ≤48 bits, so bit 63 is always zero in any
// valid pointer — the OR is safe and reversible with & ~(1ULL << 63).
inline uint64_t build_cqe_state_data(ublk_io_data const* data, uint64_t sub_cmd) {
    auto* io = reinterpret_cast< async_io* >(data->private_data);
    auto* state = io->ensure(static_cast< sub_cmd_t >(sub_cmd));
    return reinterpret_cast< uint64_t >(state) | (1ULL << 63);
}

} // namespace ublkpp
