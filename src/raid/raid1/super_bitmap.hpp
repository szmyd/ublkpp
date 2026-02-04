#pragma once

#include <cstddef>
#include <cstdint>

namespace ublkpp::raid1 {

// SuperBitmap: Fast bitmap page tracking (1 bit per bitmap page)
// Stored on disk in SuperBlock.superbitmap_reserved field
// Does NOT own memory - operates directly on the SuperBlock.superbitmap_reserved field
//
// LIMITATION: With 4033 bytes (32,264 bits), can track at most 32,264 bitmap pages.
// With minimum chunk_size of 32KiB, this limits disk size to ~31.5TB.

class SuperBitmap {
private:
    static constexpr size_t k_size_bytes = 4033;  // Size of SuperBlock.superbitmap_reserved
    static constexpr size_t k_size_bits = k_size_bytes * 8;  // 32,264 bits max
    uint8_t* _bits;  // Pointer to SuperBlock.superbitmap_reserved (NOT owned)

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

    // Set all bits (mark all pages dirty)
    void set_all();

    // Get raw data pointer (points into SuperBlock.superbitmap_reserved)
    uint8_t* data();
    const uint8_t* data() const;
    size_t size() const;
};

} // namespace ublkpp::raid1
