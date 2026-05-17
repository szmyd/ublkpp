#pragma once

#include "ublkpp/lib/cqe_state.hpp"
#include "ublkpp/lib/ublk_disk.hpp"

namespace ublkpp::test {

// Add a NOP SQE to q's ring with user_data pointing at the cqe_state that async_iov will
// next allocate via io->next_state().  Called only for resync slots (_tag == -1); normal IO
// slots use the inject_cqe path and must not have a NOP SQE queued here.
//
// The cqe_state address is stable because async_io::_pool is pre-reserved to capacity 1 and
// push_back never reallocates.  The coroutine suspends at co_await *state AFTER submit_iov
// returns, so _waiter is set by the time __process_cqe runs.
inline io_result submit_resync_nop(ublksrv_queue const* q, ublk_io_data const* data) {
    if (!data || !data->private_data || !q || !q->ring_ptr) return 1;
    auto* io_state = reinterpret_cast< async_io* >(data->private_data);
    if (io_state->_tag != -1) return 1; // normal IO: caller drives completion via inject_cqe
    if (io_state->_pool.size() >= io_state->_pool.capacity()) return 1; // shouldn't happen
    auto* future_state = io_state->_pool.data() + io_state->_pool.size();
    auto* sqe = next_sqe(q);
    if (!sqe) return 1;
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data64(sqe, reinterpret_cast< uint64_t >(future_state) | k_target_bit);
    return 1;
}

// Default submit_iov action.
// • Normal IO slots (_tag >= 0): returns 1 without queuing an SQE; inject_cqe() delivers the result.
// • Resync slots (_tag == -1): queues a NOP SQE so __process_cqe() can resume the cqe_state.
inline auto make_async_iov_action() {
    return [](ublksrv_queue const* q, ublk_io_data const* data, iovec*, uint32_t, uint64_t) -> io_result {
        return submit_resync_nop(q, data);
    };
}

} // namespace ublkpp::test
