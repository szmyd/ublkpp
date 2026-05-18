#pragma once

#include <exec/task.hpp>

#include <functional>
#include <mutex>
#include <vector>

#include <sisl/logging/logging.h>

namespace ublkpp {

// Thread-safe queue of pending RAID1 resync coroutine factories. I/O-queue threads
// call submit() from launch(); the per-volume run_resync_queue_loop drains it each tick
// via drain() and spawns the coroutines into its own exec::async_scope.
struct ResyncDispatcher {
    void submit(std::function< exec::task< void >() > f) {
        auto lk = std::scoped_lock(_mu);
        _pending.push_back(std::move(f));
    }

    // Atomically swap all pending factories into `out` under the lock so the caller can
    // spawn them without holding the lock. `out` is expected to be empty on entry.
    void drain(std::vector< std::function< exec::task< void >() > >& out) {
        DEBUG_ASSERT(out.empty(),
                     "ResyncDispatcher::drain: out is non-empty; pending factories would be silently dropped");
        auto lk = std::scoped_lock(_mu);
        out.swap(_pending);
    }

private:
    std::mutex _mu;
    std::vector< std::function< exec::task< void >() > > _pending;
};

} // namespace ublkpp
