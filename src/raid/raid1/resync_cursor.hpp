#pragma once

#include <algorithm>
#include <cstdint>
#include <tuple>

namespace ublkpp::raid1 {

class Bitmap;

// Cursor state for the resync copy loop (_run_resync_loop).
// Encapsulates dirty-chunk navigation, progress tracking, and the skip-hint that prevents
// low-LBA regions from starving higher ones under sustained write pressure.
struct ResyncCursor {
    uint64_t lba;
    uint32_t sz;
    uint64_t skip_from{0}; // hint for next sweep if this sweep made no progress
    bool any_copy{false};  // true if any chunk was attempted in the current dirty region

    ResyncCursor(Bitmap& bm, uint64_t hint) noexcept;

    uint32_t chunk_len(uint32_t max_sz) const noexcept { return std::min(sz, max_sz); }

    // Skip a Phase-1-conflicting chunk. Returns true if the caller must break the inner loop.
    bool skip(uint32_t len, Bitmap& bm) noexcept;

    // Skip a chunk already covered by an in-flight slot.
    void skip_inflight(uint32_t len, Bitmap& bm) noexcept;

    // Advance after a successfully submitted or completed copy (or after a Phase-2 skip).
    void advance(uint32_t len, Bitmap& bm) noexcept;
};

} // namespace ublkpp::raid1
