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
//
// Slot layout — single atomic uint64_t:
//   bits [63:32]  chunk index  = lba / chunk_size   (upper 32 bits)
//   bits [31:0]   chunk count  = len / chunk_size   (lower 32 bits)
//   sentinel      UINT64_MAX   = slot is free
//
// Packing both fields into one word eliminates the two-store transitional windows that the
// previous lba+len design required: track() and untrack() are each a single CAS, and
// overlaps() is a single load + plain range check with no special-casing.
class RegionTracker {
public:
    static constexpr uint64_t k_free = std::numeric_limits< uint64_t >::max();

    explicit RegionTracker(uint32_t max_slots, uint32_t chunk_size) :
            _slots(max_slots), _shadow(4u * max_slots), _chunk_size(chunk_size) {
        DEBUG_ASSERT(max_slots > 0, "RegionTracker requires at least one slot");
        DEBUG_ASSERT(chunk_size > 0, "RegionTracker chunk_size must be non-zero");
    }

    // Register an in-flight write. Single CAS atomically claims the slot and publishes
    // both chunk_idx and chunk_count — no transitional window.
    // Scan starts from chunk_idx % size to distribute hot-slot pressure across threads.
    void track(uint64_t lba, uint32_t len) noexcept {
        DEBUG_ASSERT(len > 0, "RegionTracker::track called with len=0 — zero-length writes are not valid");
        auto const packed = pack(lba, len);
        auto const start = (lba / _chunk_size) % _slots.size();
        while (true) {
            for (size_t i = 0; i < _slots.size(); ++i) {
                auto& slot = _slots[(start + i) % _slots.size()];
                uint64_t expected = k_free;
                if (slot.packed.compare_exchange_weak(expected, packed, std::memory_order_release,
                                                      std::memory_order_relaxed))
                    return;
            }
            // Slot exhaustion: should never happen if sized to at least queue_depth.
            TLOGE("RegionTracker slot exhaustion — spinning (lba={:#x} len={})", lba, len)
            std::this_thread::yield();
        }
    }

    // Deregister a completed write. Finds the matching slot and frees it with a single CAS.
    //
    // Shadow log: records a completed write for Phase 2 detection via completed_since().
    // Slot reservation uses fetch_add(acq_rel) so multiple concurrent callers each claim
    // a distinct shadow slot.
    //
    // Protocol:
    //   1. fetch_add(acq_rel) atomically claims idx and bumps _shadow_head (gen is 0-based).
    //   2. Write lba/len to shadow[idx] with relaxed (ordered by seq release below).
    //   3. seq.store(gen+1, release) publishes the entry; completed_since() acquires on seq
    //      before reading lba/len, so it sees valid data once seq matches.
    //
    // INVARIANT: block-device ordering guarantees no two concurrent writes to the same
    // LBA with different sizes, so (lba, len) uniquely identifies the slot.
    void untrack(uint64_t lba, uint32_t len) noexcept {
        auto const packed = pack(lba, len);
        for (auto& slot : _slots) {
            // relaxed: untrack() is always called by the owner of the ResyncWriteGuard that
            // called track(), or its destructor; the I/O completion provides the required ordering.
            if (slot.packed.load(std::memory_order_relaxed) != packed) continue;

            // Claim a shadow slot before freeing the main slot. This ensures _shadow_head
            // is visible to completed_since() before overlaps() can return false for this
            // range: if Phase 2 races here, it will see head_now > gen_before and scan for
            // the shadow entry (which seq guards until lba/len are written below).
            auto const gen = _shadow_head.fetch_add(1, std::memory_order_acq_rel);

            uint64_t expected = packed;
            if (!slot.packed.compare_exchange_strong(expected, k_free, std::memory_order_release,
                                                     std::memory_order_relaxed)) {
                // CAS failure here means the same packed value occupied two slots simultaneously
                // (duplicate-LBA tracking). This is a test-only scenario — block-device ordering
                // forbids concurrent writes to the same LBA in production. Write seq=0 for the
                // already-claimed shadow gen (conservative: completed_since() returns true), then
                // continue scanning for the second slot holding the same packed value.
                // Do not write seq here: leaving it unpublished (seq != gen+1) causes
                // completed_since() to return true conservatively — which is correct.
                // Writing seq=0 would be ambiguous when gen+1 == 0 (UINT64_MAX wrap).
                continue;
            }

            auto const idx = gen % _shadow.size();
            _shadow[idx].lba.store(lba, std::memory_order_relaxed);
            _shadow[idx].len.store(len, std::memory_order_relaxed);
            // Release publish: completed_since() acquires on seq before reading lba/len.
            // gen+1 == 0 would collide with the initial seq=0 sentinel; assert it never wraps.
            // At 1M IOPS this counter saturates in ~584,000 years.
            RELEASE_ASSERT(gen + 1 != 0, "RegionTracker shadow head wrapped at UINT64_MAX — unreachable in practice");
            _shadow[idx].seq.store(gen + 1, std::memory_order_release);
            return;
        }
        DLOGE("RegionTracker: no slot found for lba={:#x} len={} — block-device ordering invariant violated; "
              "resync will stall permanently for this range",
              lba, len)
        DEBUG_ASSERT(false, "RegionTracker: no slot found for lba={:#x} len={}", lba, len);
    }

    // Returns true if any in-flight write overlaps [lba, lba+len).
    // Single atomic load per slot — no transitional windows, no special-casing.
    [[nodiscard]] bool overlaps(uint64_t lba, uint32_t len) const noexcept {
        DEBUG_ASSERT(len > 0, "RegionTracker::overlaps called with len=0 — zero-length queries always return false");
        auto const q_start = lba / _chunk_size;
        auto const q_end = (lba + len + _chunk_size - 1) / _chunk_size; // exclusive, rounded up
        for (auto const& slot : _slots) {
            auto const val = slot.packed.load(std::memory_order_acquire);
            if (k_free == val) continue;
            auto const slot_start = val >> 32;
            auto const slot_end = slot_start + (val & 0xffff'ffffULL); // exclusive
            if (slot_start < q_end && slot_end > q_start) return true;
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
        if (head_now - gen_before >= _shadow.size()) return true; // overflow: be conservative
        for (uint64_t i = gen_before; i < head_now; ++i) {
            auto const idx = i % _shadow.size();
            // If seq != i+1 the producer hasn't published lba/len yet. We don't know the range,
            // so return true conservatively rather than risk a false negative.
            if (_shadow[idx].seq.load(std::memory_order_acquire) != i + 1) return true;
            // Synchronized via seq acquire: lba and len are now valid.
            auto const sl = _shadow[idx].lba.load(std::memory_order_relaxed);
            // Defensive: sl == k_free would mean the lba store was skipped, which cannot happen
            // once seq matches (seq release is sequenced-after the lba store). Included for safety.
            if (sl == k_free) continue;
            auto const slen = _shadow[idx].len.load(std::memory_order_relaxed);
            if (sl < lba + len && sl + slen > lba) return true;
        }
        return false;
    }

    // Returns true if no slots are occupied. Used as a post-stop integrity check.
    [[nodiscard]] bool all_free() const noexcept {
        for (auto const& slot : _slots)
            if (k_free != slot.packed.load(std::memory_order_relaxed)) return false;
        return true;
    }

private:
    // Each slot occupies a full cache line to prevent false sharing between the resync
    // reader (overlaps()) and I/O writers (track/untrack) on adjacent slots.
    // Single atomic uint64_t: upper 32 bits = chunk index, lower 32 bits = chunk count.
    struct alignas(64) Slot {
        std::atomic< uint64_t > packed{k_free};
    };
    static_assert(sizeof(Slot) == 64, "Slot must be cache-line sized");
    static_assert(std::atomic< uint64_t >::is_always_lock_free);

    // Shadow completion log: records recently completed writes for Phase 2 detection.
    // Not cache-line padded — slots (16 KiB) + padded shadow (32 KiB) would exceed L1D
    // on 32 KiB cores; keeping shadow entries dense (24 bytes) holds the combined working
    // set within L1D on all server-class microarchitectures.
    //
    // Sized to 4× the main slot count so the ring overflows only when more than
    // 4×qdepth writes complete during a single async copy slot's READ window.
    struct ShadowEntry {
        std::atomic< uint64_t > lba{k_free};
        std::atomic< uint32_t > len{0};
        // seq == 0: slot never written. seq == gen+1: entry for generation gen is published.
        std::atomic< uint64_t > seq{0};
    };

    [[nodiscard]] uint64_t pack(uint64_t lba, uint32_t len) const noexcept {
        auto const chunk_idx = lba / _chunk_size;
        // chunk_idx must fit in 32 bits: upper 32 bits of the packed slot hold chunk_idx,
        // so volumes requiring chunk_idx >= 2^32 would silently truncate and produce false
        // negatives (resync proceeds through a conflicting range). At the default 32 KiB
        // chunk_size the limit is ~128 TiB.
        RELEASE_ASSERT(chunk_idx < (1ULL << 32),
                       "RegionTracker: chunk_idx overflow — volume too large for RegionTracker slot encoding; "
                       "at 32 KiB chunk_size the limit is ~128 TiB");
        // Use ceiling to compute end chunk: a sub-chunk write (len < chunk_size) must occupy
        // at least 1 chunk; a write spanning a chunk boundary occupies 2+. Floor division
        // would give chunk_count=0 for sub-chunk writes, making overlaps() return false even
        // with a slot occupied — causing both Phase 1 and Phase 2 to miss the conflict.
        auto const chunk_end = (lba + len + _chunk_size - 1) / _chunk_size;
        return (chunk_idx << 32) | (chunk_end - chunk_idx);
    }

    std::vector< Slot > _slots;
    std::vector< ShadowEntry > _shadow;
    // Wraps at 2^64. Unsigned wraparound is well-defined; completed_since() uses subtraction
    // (head_now - gen_before) which remains correct across a wrap. At 1M IOPS the counter
    // saturates in ~584,000 years.
    std::atomic< uint64_t > _shadow_head{0};
    uint32_t _chunk_size;
};

} // namespace ublkpp::raid1
