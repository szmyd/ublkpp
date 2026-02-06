#pragma once

#include <cstddef>
#include <cstdint>

namespace ublkpp::raid1 {

// SuperBitmap: Fast bitmap page tracking (1 bit per bitmap page)
// Stored on disk in SuperBlock.superbitmap_reserved field
// Does NOT own memory - operates directly on the SuperBlock.superbitmap_reserved field
//
// CONCURRENCY: set_bit(), clear_bit(), and test_bit() are thread-safe and lock-free.
// They use atomic operations (std::atomic_ref) to safely modify individual bits.
// clear_all() is NOT thread-safe - caller must ensure exclusive access.
//
// LIMITATION: With 4022 bytes (32,176 bits), can track at most 32,176 bitmap pages.
// With minimum chunk_size of 32KiB, this limits disk size to ~31.4TB.

class SuperBitmap {
private:
    static constexpr size_t k_size_bytes = 4022;  // Size of SuperBlock.superbitmap_reserved
    static constexpr size_t k_size_bits = k_size_bytes * 8;  // 32,176 bits max
    uint8_t* const _bits;  // Const pointer to SuperBlock.superbitmap_reserved (cannot be reassigned, NOT owned)

public:
    // Constructor takes pointer to SuperBlock.superbitmap_reserved field
    SuperBitmap(uint8_t* superblock_reserved_field);

    // Set bit for a bitmap page (mark as dirty)
    void set_bit(uint32_t page_idx);

    // Clear bit for a bitmap page (mark as clean)
    void clear_bit(uint32_t page_idx);

    // Test if bitmap page is dirty
    bool test_bit(uint32_t page_idx) const;

    // Clear all bits (mark all pages clean)
    void clear_all();

    // Get raw data pointer (points into SuperBlock.superbitmap_reserved)
    uint8_t* data();
    const uint8_t* data() const;
    size_t size() const;
};

} // namespace ublkpp::raid1
