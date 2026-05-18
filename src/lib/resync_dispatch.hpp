#pragma once

#include <exec/task.hpp>

#include <functional>
#include <vector>

#include <sisl/logging/logging.h>

namespace ublkpp {

// Single-slot handoff from I/O-queue threads (submit) to the per-volume resync thread (drain).
// Raid1ResyncTask::_launch_lock guarantees at most one factory is ever pending at a time,
// so this is an SPSC slot, not a queue — no mutex needed.
struct ResyncDispatcher {
    using Fn = std::function< exec::task< void >() >;

    // Called by an I/O-queue thread inside launch() while holding _launch_lock.
    // release ensures drain()'s acquire-exchange sees the fully-constructed Fn.
    void submit(Fn f) { _slot.store(new Fn(std::move(f)), std::memory_order_release); }

    // Called by run_resync_queue_loop on the resync thread every ~500 µs.
    // acquire pairs with submit()'s release; out is expected empty on entry.
    void drain(std::vector< Fn >& out) {
        DEBUG_ASSERT(out.empty(),
                     "ResyncDispatcher::drain: out is non-empty; pending factories would be silently dropped");
        if (auto* p = _slot.exchange(nullptr, std::memory_order_acquire)) {
            out.push_back(std::move(*p));
            delete p;
        }
    }

private:
    std::atomic< Fn* > _slot{nullptr};
};

} // namespace ublkpp
