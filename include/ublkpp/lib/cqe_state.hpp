#pragma once

#include <coroutine>
#include <cstdint>
#include <deque>
#include <utility>

#include <sisl/logging/logging.h>
#include <ublksrv.h>

#include <ublkpp/lib/sub_cmd.hpp>

namespace ublkpp {

// Encodes tag, op, and sub_cmd into the io_uring SQE user_data for target I/O.
// The high bit marks the entry as a target SQE so libublksrv routes the CQE to tgt_io_done.
inline uint64_t build_tgt_sqe_data(uint64_t tag, uint64_t op, uint64_t sub_cmd) {
    DEBUG_ASSERT_LE(tag, UINT16_MAX, "Tag too big: [{:#0x}]", tag)
    DEBUG_ASSERT_LE(op, UINT8_MAX, "Op too big: [{:#0x}]", op)
    DEBUG_ASSERT_LE(sub_cmd, UINT16_MAX, "SubCmd too big: [{:#0x}]", sub_cmd)
    return tag | (op << sqe_tag_width) | (sub_cmd << (sqe_tag_width + sqe_op_width)) |
        (static_cast< uint64_t >(0b1) << (sqe_tag_width + sqe_op_width + sqe_tgt_data_width + sqe_reserved_width));
}

struct CqeState;

// Per-IO state tracking one inflight request, owned by the ublksrv-allocated io_data slot.
//
// Lifetime: placement-new'd in init_queue for each tag slot; explicitly ~async_io() in deinit_queue.
// pool and completions are cleared at the start of each new I/O in __handle_io_async.
//
// Dispatch protocol:
//   1. queue_tgt_io calls build_cqe_state_data → ensure(sub_cmd) per SQE; pool grows.
//   2. tgt_io_done / handle_event: ensure(sub_cmd) finds the state, sets result, push_completion.
//   3. push_completion resumes the CqeAwaiter-suspended coroutine if one is waiting.
//   4. CqeAwaiter::await_resume pops one entry from completions and returns it.
//   5. process_result inspects the state and may add to pending (retry / internal).
struct async_io {
    std::coroutine_handle<> waiter{};
    std::deque< CqeState > pool{};         // stable addresses: push_back never invalidates pointers
    std::deque< CqeState* > completions{}; // ordered queue of states ready for process_result
    uint32_t pending{0};
    int ret_val{0};

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

// Drop-in replacement for build_tgt_sqe_data for disk drivers that submit SQEs through the target.
//
// Before encoding user_data for an SQE, this function pre-registers the sub_cmd in the per-IO
// CqeState pool (async_io::ensure). When the CQE arrives, tgt_io_done looks up the state by
// sub_cmd — avoiding a shared side-channel — sets the result, and resumes the coroutine via
// push_completion.
//
// The encoded bits returned are identical to build_tgt_sqe_data(tag, op, sub_cmd), so libublksrv
// routing (tag extraction, is_target_io check) is completely unchanged.
inline uint64_t build_cqe_state_data(ublk_io_data const* data, uint64_t tag, uint64_t op, uint64_t sub_cmd) {
    reinterpret_cast< async_io* >(data->private_data)->ensure(static_cast< sub_cmd_t >(sub_cmd));
    return build_tgt_sqe_data(tag, op, sub_cmd);
}

} // namespace ublkpp
