#pragma once

#include <atomic>
#include <chrono>
#include <thread>

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

    std::atomic_uint8_t _resync_state;
    std::atomic_uint32_t _outstanding_writes;
    std::thread _resync_task;

    // State access helpers to eliminate casting noise
    resync_state __load_state(std::memory_order order = std::memory_order_acquire) const noexcept {
        return static_cast< resync_state >(_resync_state.load(order));
    }

    bool __cas_state_weak(resync_state& expected, resync_state desired,
                          std::memory_order success = std::memory_order_seq_cst,
                          std::memory_order failure = std::memory_order_seq_cst) noexcept {
        auto expected_val = static_cast< uint8_t >(expected);
        bool result = _resync_state.compare_exchange_weak(expected_val, static_cast< uint8_t >(desired),
                                                           success, failure);
        expected = static_cast< resync_state >(expected_val);
        return result;
    }

    bool __cas_state_strong(resync_state& expected, resync_state desired,
                            std::memory_order success = std::memory_order_seq_cst,
                            std::memory_order failure = std::memory_order_seq_cst) noexcept {
        auto expected_val = static_cast< uint8_t >(expected);
        bool result = _resync_state.compare_exchange_strong(expected_val, static_cast< uint8_t >(desired),
                                                             success, failure);
        expected = static_cast< resync_state >(expected_val);
        return result;
    }

    resync_state __run(auto& clean_mirror, auto& dirty_mirror);

    // This most happen, so we wait till the resync job becomes sleeping, then move it quickly to
    // PAUSE to prevent any resync opeartions from continuing to run (will block in __yield)
    void __pause() noexcept;
    inline void __resume() noexcept {
        auto cur_state = resync_state::PAUSE;
        __cas_state_strong(cur_state, resync_state::ACTIVE);
    }
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
    uint32_t stop() noexcept; // Returns the _outstanding_writes cnt here

    inline void enqueue_write() noexcept {
        auto const old_val = _outstanding_writes.fetch_add(1, std::memory_order_release);
        if (0 == old_val) __pause();
        DEBUG_ASSERT_LT(old_val, UINT32_MAX, "Outstanding Write Count Overflowed!");
    }

    inline void dequeue_write() noexcept {
        auto const old_val = _outstanding_writes.fetch_sub(1, std::memory_order_acquire);
        if (1 == old_val) __resume();
        DEBUG_ASSERT_GT(old_val, 0, "Outstanding Write Count Underflowed!");
    }
};
} // namespace ublkpp::raid1
