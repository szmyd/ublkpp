#pragma once

#include <exec/task.hpp>

#include <functional>
#include <mutex>
#include <vector>

namespace ublkpp {

// Thread-safe queue of pending RAID1 resync coroutine factories. I/O-queue threads
// call submit() from launch(); the per-volume run_resync_queue_loop drains it each tick
// and spawns the coroutines into its own exec::async_scope.
struct ResyncDispatcher {
    std::mutex mu;
    std::vector< std::function< exec::task< void >() > > pending;

    void submit(std::function< exec::task< void >() > f) {
        auto lk = std::scoped_lock(mu);
        pending.push_back(std::move(f));
    }
};

} // namespace ublkpp
