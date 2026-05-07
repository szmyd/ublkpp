#pragma once

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include <sisl/logging/logging.h>

#include "lib/logging.hpp"

namespace ublkpp::raid1 {

// Lock-free bounded array tracking in-flight write LBA ranges.
// One slot is held per in-flight write (ResyncWriteGuard acquires one slot for the duration
// of the write regardless of how many replica legs it touches). Size at construction to at
// least queue_depth. Resync checks for overlap before and after each chunk copy.
class RegionTracker {
public:
    static constexpr uint64_t k_free = ~0ULL;

    explicit RegionTracker(uint32_t max_slots) : _slots(max_slots) {}

    // Register an in-flight write. CAS on lba (relaxed) claims the slot; len is published
    // with release ordering. There is a narrow window between those two stores where
    // slot_len reads as 0; overlaps() handles this conservatively — see overlaps() comment.
    void track(uint64_t lba, uint32_t len) noexcept {
        while (true) {
            for (auto& slot : _slots) {
                uint64_t expected = k_free;
                if (slot.lba.compare_exchange_weak(expected, lba, std::memory_order_relaxed,
                                                   std::memory_order_relaxed)) {
                    slot.len.store(len, std::memory_order_release);
                    return;
                }
            }
            // Slot exhaustion: should never happen if sized to at least queue_depth.
            TLOGE("RegionTracker slot exhaustion — spinning (lba={:#x} len={})", lba, len)
            std::this_thread::yield();
        }
    }

    // Deregister a completed write. Finds the first matching slot and clears it.
    // len is reset to 0 with release ordering before freeing lba so that any concurrent
    // overlaps() call that sees the slot mid-transition reads 0 and takes the conservative
    // path rather than seeing a stale non-zero value from a prior use.
    //
    // INVARIANT: block-device ordering guarantees no two concurrent writes to the same
    // LBA with different sizes, so (lba, len) uniquely identifies the slot and the
    // len-then-CAS sequence cannot accidentally free a slot belonging to a different write.
    void untrack(uint64_t lba, uint32_t len) noexcept {
        for (auto& slot : _slots) {
            if (slot.lba.load(std::memory_order_relaxed) != lba) continue;
            if (slot.len.load(std::memory_order_relaxed) != len) continue;
            uint64_t expected = lba;
            slot.len.store(0, std::memory_order_release);
            if (slot.lba.compare_exchange_strong(expected, k_free, std::memory_order_release,
                                                 std::memory_order_relaxed))
                return;
        }
        DEBUG_ASSERT(false, "RegionTracker: no slot found for lba={:#x} len={}", lba, len);
    }

    // Returns true if any in-flight write overlaps [lba, lba+len).
    //
    // Memory ordering: slot_lba is loaded with acquire, synchronizing-with the release store
    // in track() — but only when the load reads the stored value. Two transitional windows
    // exist where slot_len reads as 0:
    //   track():   between the relaxed lba CAS and the subsequent len.store(release)
    //   untrack(): between the len.store(0, release) and the lba CAS(release) that frees it
    //
    // In both windows, slot_len==0 on a non-free slot is treated as a potential conflict
    // when slot_lba falls inside [lba, lba+len). This is a false positive only — at most one
    // spurious resync yield per window, never a missed conflict.
    [[nodiscard]] bool overlaps(uint64_t lba, uint32_t len) const noexcept {
        for (auto const& slot : _slots) {
            auto const slot_lba = slot.lba.load(std::memory_order_acquire);
            if (k_free == slot_lba) continue;
            auto const slot_len = slot.len.load(std::memory_order_acquire);
            if (slot_lba < lba + len && (0 == slot_len || slot_lba + slot_len > lba)) return true;
        }
        return false;
    }

    // Returns true if no slots are occupied. Used as a post-stop integrity check.
    [[nodiscard]] bool all_free() const noexcept {
        for (auto const& slot : _slots)
            if (k_free != slot.lba.load(std::memory_order_relaxed)) return false;
        return true;
    }

private:
    // Each slot occupies a full cache line to prevent false sharing between the resync
    // reader (overlaps()) and I/O writers (track/untrack) on adjacent slots.
    struct alignas(64) Slot {
        std::atomic< uint64_t > lba{k_free};
        std::atomic< uint32_t > len{0};
    };
    static_assert(sizeof(Slot) == 64, "Slot must be cache-line sized");
    static_assert(std::atomic< uint64_t >::is_always_lock_free);
    static_assert(std::atomic< uint32_t >::is_always_lock_free);

    std::vector< Slot > _slots;
};

} // namespace ublkpp::raid1
