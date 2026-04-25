#pragma once

#include <atomic>
#include <chrono>
#include <thread>
#include <functional>

#include <sisl/fds/atomic_status_counter.hpp>

#include "ublkpp/raid/raid1.hpp"
#include "metrics/ublk_raid_metrics.hpp"
#include "raid1_superblock.hpp"

namespace ublkpp::raid1 {

using namespace std::chrono_literals;
constexpr auto k_state_spin_time = 50us;

class Bitmap;
class MirrorDevice;

// State transitions:
//   IDLE → ACTIVE (launch())
//   ACTIVE ⟷ SLEEPING (yield for I/O)
//   SLEEPING → PAUSE (I/O started, block resync)
//   PAUSE → ACTIVE (I/O finished, resync can proceed)
//   any → STOPPING (shutdown requested)
//   STOPPING → IDLE (shutdown complete)
ENUM(resync_state, uint8_t, IDLE = 0, ACTIVE = 1, SLEEPING = 2, PAUSE = 3, STOPPING = 4);

// State transition actions for __transition_to helper
enum class transition_action : uint8_t {
    RETRY,            // Continue CAS loop immediately
    RETRY_WITH_SLEEP, // Sleep k_state_spin_time then retry
    SUCCESS,          // Exit loop (CAS succeeded or found target state)
    EARLY_EXIT        // Exit caller function immediately
};

// Result from state transition handler callback
struct transition_result {
    resync_state next_state; // State to expect on next retry
    transition_action action;
};

class Raid1ResyncTask {

    // Global counter for active resyncs across all RAID1 devices
    static inline std::atomic_uint32_t s_active_resyncs{0};

    std::shared_ptr< raid1::Bitmap > const _dirty_bitmap;
    std::shared_ptr< ublkpp::UblkRaidMetrics > const _metrics;

    // The smallest I/O both devices support (RAID logical block size)
    uint32_t const _io_size;
    // The largest I/O both devices support
    uint32_t const _max_size;
    // This is the offset we should copy the disks @ to avoid writing on the BITMAP itself.
    uint64_t const _offset;

    // Packs resync_state (status) + outstanding-write count (int32_t counter) into one 64-bit
    // word operated on with a single CAS. Key guarantee: dec_xchng_status_ifz() decrements the
    // counter and, only if it reaches zero AND status==PAUSE, atomically transitions to ACTIVE —
    // eliminating the window where a concurrent enqueue_write() could see count==0 while state
    // remains PAUSE, early-exit __pause(), and then race with a __resume() that fires afterward.
    sisl::atomic_status_counter< resync_state, resync_state::IDLE > _state_and_writes;
    std::mutex _launch_lock;
    std::thread _resync_task;

    // State access helpers to eliminate casting noise
    resync_state __load_state() const noexcept { return _state_and_writes.get_status(); }

    bool __cas_state(resync_state& expected, resync_state desired) noexcept {
        auto const exp = expected;
        return _state_and_writes.set_atomic_value([&](auto& /*cnt*/, auto& status) {
            if (status == exp) {
                status = desired;
                return true;
            }
            expected = status;
            return false;
        });
    }

    resync_state __run(auto& clean_mirror, auto& dirty_mirror, iovec* iov) noexcept;

    // Generic state transition helper - reduces duplication across launch/stop/pause
    template < typename StateHandler >
    bool __transition_to(resync_state initial, resync_state target, StateHandler&& handler) noexcept;

    // This most happen, so we wait till the resync job becomes sleeping, then move it quickly to
    // PAUSE to prevent any resync opeartions from continuing to run (will block in __yield)
    void __pause() noexcept;
    void _start(std::string str_uuid, std::shared_ptr< MirrorDevice >& clean_mirror,
                std::shared_ptr< MirrorDevice >& dirty_mirror, std::function< void() >&& complete);

    resync_state __yield(std::chrono::microseconds const yield_for, std::chrono::microseconds const spin_time) noexcept;

public:
    Raid1ResyncTask(std::shared_ptr< raid1::Bitmap >& bitmap, uint64_t offset, uint32_t io_size, uint32_t max_io,
                    std::shared_ptr< ublkpp::UblkRaidMetrics > metrics = nullptr);
    ~Raid1ResyncTask() noexcept;

    void clean_region(uint64_t addr, uint32_t len, MirrorDevice& clean_device);

    void launch(std::string const& str_uuid, std::shared_ptr< MirrorDevice > clean_mirror,
                std::shared_ptr< MirrorDevice > dirty_mirror, std::function< void() >&& complete);

    // Generic method to move Resync StateMachine to STOPPING
    uint32_t stop() noexcept; // Returns outstanding write count at time of stop

    inline void enqueue_write() noexcept {
        _state_and_writes.set_atomic_value([](auto& cnt, auto& /*status*/) {
            ++cnt;
            return true;
        });
        // Always call __pause() — not just on the first enqueue. A concurrent first enqueuer may
        // still be spinning inside __pause() when a second thread increments the counter; skipping
        // __pause() here would let the second write proceed before PAUSE is established, allowing
        // resync to overwrite it with stale data. __pause() is cheap when already PAUSE (O(1) CAS).
        __pause();
        DEBUG_ASSERT_LT(_state_and_writes.count(), INT32_MAX, "Outstanding Write Count Overflowed!");
    }

    inline void dequeue_write() noexcept {
        DEBUG_ASSERT_GT(_state_and_writes.count(), 0, "Outstanding Write Count Underflowed!");
        // Atomically decrement; if counter hits zero while state is PAUSE, transition to ACTIVE.
        // This single CAS closes the race where a concurrent enqueue could see old_val==0,
        // find state still PAUSE (from the prior write), and early-exit __pause() — only for
        // __resume() to then clear PAUSE before the new write's I/O is submitted.
        _state_and_writes.dec_xchng_status_ifz(resync_state::PAUSE, resync_state::ACTIVE);
    }
};
} // namespace ublkpp::raid1
