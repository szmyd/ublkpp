#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
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
    static constexpr uint64_t k_free = std::numeric_limits< uint64_t >::max();

    explicit RegionTracker(uint32_t max_slots) : _slots(max_slots), _shadow(4u * max_slots) {}

    // Register an in-flight write. CAS on lba (release) claims the slot; len is published
    // with release ordering. There is a narrow window between those two stores where
    // slot_len reads as 0; overlaps() handles this conservatively — see overlaps() comment.
    //
    // Scan starts from lba % size to distribute hot-slot pressure across threads.
    void track(uint64_t lba, uint32_t len) noexcept {
        while (true) {
            auto const start = lba % _slots.size();
            for (size_t i = 0; i < _slots.size(); ++i) {
                auto& slot = _slots[(start + i) % _slots.size()];
                uint64_t expected = k_free;
                // Release success ordering so overlaps()'s acquire load on slot.lba
                // synchronizes with this store on all architectures (not just TSO).
                if (slot.lba.compare_exchange_weak(expected, lba, std::memory_order_release,
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
    // Shadow log: records a completed write for Phase 2 detection via completed_since().
    // Slot reservation uses fetch_add(acq_rel) so multiple concurrent callers each claim
    // a distinct shadow slot — the previous relaxed-load+fetch_add sequence was not
    // multi-producer safe and could cause two threads to overwrite the same shadow entry,
    // silently dropping a completion and producing a false negative in completed_since().
    //
    // Protocol:
    //   1. fetch_add(acq_rel) atomically claims idx and bumps _shadow_head (gen is 0-based).
    //   2. Write lba/len to shadow[idx] with relaxed (ordered by seq release below).
    //   3. seq.store(gen+1, release) publishes the entry; completed_since() acquires on seq
    //      before reading lba/len, so it sees valid data once seq matches.
    //
    // If the seq is not yet set when completed_since() scans this entry, it returns true
    // conservatively (the completion happened but we don't know the range yet — safe).
    //
    // INVARIANT: block-device ordering guarantees no two concurrent writes to the same
    // LBA with different sizes, so (lba, len) uniquely identifies the slot and the
    // len-then-CAS sequence cannot accidentally free a slot belonging to a different write.
    void untrack(uint64_t lba, uint32_t len) noexcept {
        for (auto& slot : _slots) {
            // Acquire on lba synchronizes with the release-CAS in track() so the subsequent
            // len load is ordered correctly on weakly-ordered hardware (arm64).
            if (slot.lba.load(std::memory_order_acquire) != lba) continue;
            if (slot.len.load(std::memory_order_acquire) != len) continue;
            uint64_t expected = lba;

            // Atomically claim a shadow slot. Each concurrent caller gets a distinct gen,
            // so no two callers write to the same shadow entry.
            auto const gen = _shadow_head.fetch_add(1, std::memory_order_acq_rel);
            auto const idx = gen % _shadow.size();
            _shadow[idx].lba.store(lba, std::memory_order_relaxed);
            _shadow[idx].len.store(len, std::memory_order_relaxed);
            // Release publish: completed_since() acquires on seq before reading lba/len.
            _shadow[idx].seq.store(gen + 1, std::memory_order_release);

            slot.len.store(0, std::memory_order_release);
            if (slot.lba.compare_exchange_strong(expected, k_free, std::memory_order_release,
                                                 std::memory_order_relaxed))
                return;
            // CAS failed: a concurrent untrack() for the same lba freed this slot first,
            // which violates the block-device ordering invariant (no two concurrent writes to
            // the same LBA). The shadow entry we published is valid (records a true completion).
            // Continue scanning — our slot is a different entry with the same lba.
        }
        DLOGE("RegionTracker: no slot found for lba={:#x} len={} — block-device ordering invariant violated; "
              "resync will stall permanently for this range",
              lba, len)
        DEBUG_ASSERT(false, "RegionTracker: no slot found for lba={:#x} len={}", lba, len);
    }

    // Returns true if any in-flight write overlaps [lba, lba+len).
    //
    // Memory ordering: slot_lba is loaded with acquire, synchronizing-with the release-success
    // CAS in track() — so once a non-free lba is visible, the subsequent len.store(release)
    // is also ordered before our len.load(acquire). Two transitional windows exist where
    // slot_len reads as 0:
    //   track():   between the release CAS on lba and the subsequent len.store(release)
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

    // Snapshot the current completion generation. Call before Phase 1; pass to completed_since()
    // after the copy to detect writes that completed entirely between the two checks.
    [[nodiscard]] uint64_t snapshot_gen() const noexcept { return _shadow_head.load(std::memory_order_acquire); }

    // Returns true if any write whose range overlaps [lba, lba+len) completed (called untrack())
    // after gen_before was captured via snapshot_gen().
    //
    // Returns true conservatively in two cases:
    //   (a) The shadow ring has overflowed since gen_before (more completions than ring size).
    //   (b) A shadow entry within the scanned range has not yet been published by its producer
    //       (seq != expected): a completion happened but we don't know the range yet.
    //
    // Memory ordering: seq.load(acquire) synchronizes-with seq.store(release) in untrack(),
    // which is sequenced-after the stores to lba and len. So once seq matches, lba/len are
    // valid and can be read with relaxed ordering.
    [[nodiscard]] bool completed_since(uint64_t lba, uint32_t len, uint64_t gen_before) const noexcept {
        auto const head_now = _shadow_head.load(std::memory_order_acquire);
        if (head_now == gen_before) return false;
        if (head_now - gen_before > _shadow.size()) return true; // overflow: be conservative
        for (uint64_t i = gen_before; i < head_now; ++i) {
            auto const idx = i % _shadow.size();
            // If seq != i+1 the producer hasn't published lba/len yet. We don't know the range,
            // so return true conservatively rather than risk a false negative.
            if (_shadow[idx].seq.load(std::memory_order_acquire) != i + 1) return true;
            // Synchronized via seq acquire: lba and len are now valid.
            auto const sl = _shadow[idx].lba.load(std::memory_order_relaxed);
            if (sl == k_free) continue;
            auto const slen = _shadow[idx].len.load(std::memory_order_relaxed);
            if (sl < lba + len && sl + slen > lba) return true;
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

    // Shadow completion log: records recently completed writes for Phase 2 detection.
    // Not cache-line padded — only accessed by the resync reader (completed_since) and
    // I/O completion threads (untrack). seq provides the synchronization point: producers
    // write lba/len then store seq(release); the reader acquires on seq before reading
    // lba/len, so no additional padding is needed between the fields.
    //
    // Sized to 4× the main slot count so the ring overflows only when more than
    // 4×qdepth writes complete during a single __copy_region() call.
    struct ShadowEntry {
        std::atomic< uint64_t > lba{k_free};
        std::atomic< uint32_t > len{0};
        // seq == 0: slot never written. seq == gen+1: entry for generation gen is published.
        std::atomic< uint64_t > seq{0};
    };

    std::vector< Slot > _slots;
    std::vector< ShadowEntry > _shadow;
    // Wraps at 2^64. Unsigned wraparound is well-defined; completed_since() uses subtraction
    // (head_now - gen_before) which remains correct across a wrap. At 1M IOPS the counter
    // saturates in ~584,000 years.
    std::atomic< uint64_t > _shadow_head{0};
};

} // namespace ublkpp::raid1
