#pragma once

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include <sisl/logging/logging.h>

#include "lib/logging.hpp"

namespace ublkpp::raid1 {

// Lock-free bounded array tracking in-flight write LBA ranges.
// Sized to 2 × queue_depth to accommodate one slot per primary and one per replica leg.
// Resync checks for overlap before and after each copy to avoid racing with writes.
class RegionTracker {
public:
    static constexpr uint64_t k_free = ~0ULL;

    explicit RegionTracker(uint32_t max_slots) : _slots(max_slots) {}

    // Register an in-flight write. Called on the I/O thread before submitting async I/O.
    // CAS on lba (relaxed) claims the slot; len is then published with release ordering.
    // There is a narrow window between those two instructions where slot_len reads as 0.
    // overlaps() handles this conservatively: a claimed slot with len=0 whose LBA falls
    // within the queried range is treated as an overlap (see overlaps() comment).
    //
    // If both Phase 1 and Phase 2 in __run() observe len=0 (extremely unlikely — two
    // consecutive instructions), the resync copy writes stale data to the dirty mirror.
    // Correctness is still guaranteed: the in-flight write overwrites it on both mirrors
    // (success path) or dirties the bitmap via the error path.
    void register_write(uint64_t lba, uint32_t len) noexcept {
        while (true) {
            for (auto& slot : _slots) {
                uint64_t expected = k_free;
                if (slot.lba.compare_exchange_weak(expected, lba, std::memory_order_relaxed,
                                                   std::memory_order_relaxed)) {
                    slot.len.store(len, std::memory_order_release);
                    return;
                }
            }
            // Slot exhaustion: should never happen if sized to 2 × queue_depth.
            TLOGE("RegionTracker slot exhaustion — spinning (lba={:#x} len={})", lba, len)
            std::this_thread::yield();
        }
    }

    // Deregister a completed write. Finds the first matching slot and clears it.
    // Two registrations for the same (lba, len) — primary and replica legs — are
    // cleared independently, one per call.
    // len is reset to 0 before freeing lba so that any concurrent overlaps() call that
    // sees the slot mid-transition reads 0 and takes the conservative path rather than
    // seeing a stale non-zero value from a prior use.
    //
    // INVARIANT: block-device ordering guarantees no two concurrent writes to the same
    // LBA with different sizes, so (lba, len) uniquely identifies the slot pair and the
    // len-then-CAS sequence cannot accidentally free a slot belonging to a different write.
    void unregister_write(uint64_t lba, uint32_t len) noexcept {
        for (auto& slot : _slots) {
            if (slot.lba.load(std::memory_order_relaxed) != lba) continue;
            if (slot.len.load(std::memory_order_relaxed) != len) continue;
            uint64_t expected = lba;
            slot.len.store(0, std::memory_order_relaxed);
            if (slot.lba.compare_exchange_strong(expected, k_free, std::memory_order_release,
                                                 std::memory_order_relaxed))
                return;
            // CAS failed: another thread freed this slot; leave len=0, it will be
            // overwritten by the next register_write that claims the slot.
        }
        DEBUG_ASSERT(false, "RegionTracker: no slot found for lba={:#x} len={}", lba, len);
    }

    // Returns true if any in-flight write overlaps [lba, lba+len).
    //
    // Memory ordering: slot_lba is loaded with acquire, synchronizing-with the release store
    // in register_write — but only when the load actually reads the stored value. There is a
    // narrow window (between the relaxed CAS that claims a slot and the subsequent len.store)
    // where slot_len reads as 0. unregister_write also resets len to 0 before freeing lba,
    // creating a symmetric window on teardown.
    //
    // Both windows are handled conservatively: slot_len==0 on a non-free slot is treated as
    // a potential conflict (return true). This can cause at most one spurious resync yield;
    // it never causes a false negative, so no data corruption is possible.
    [[nodiscard]] bool overlaps(uint64_t lba, uint32_t len) const noexcept {
        for (auto const& slot : _slots) {
            auto const slot_lba = slot.lba.load(std::memory_order_acquire);
            if (k_free == slot_lba) continue;
            auto const slot_len = slot.len.load(std::memory_order_acquire);
            // slot_len==0: transitional window — real len not yet published.
            // Be conservative only when slot_lba is inside the query range; an unrelated
            // slot at a distant LBA is not a conflict regardless of its transitional state.
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
    struct alignas(16) Slot {
        std::atomic< uint64_t > lba{k_free};
        std::atomic< uint32_t > len{0};
        uint32_t _pad{0};
    };
    static_assert(std::atomic< uint64_t >::is_always_lock_free);
    static_assert(std::atomic< uint32_t >::is_always_lock_free);

    std::vector< Slot > _slots;
};

} // namespace ublkpp::raid1
