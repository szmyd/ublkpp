#pragma once

#include <cstddef>
#include <cstdint>

#include "raid1_superblock.hpp"

namespace ublkpp::raid1 {

class SuperBitmap {
private:
    uint8_t* const _bits;  // Const pointer to SuperBlock.superbitmap_reserved (cannot be reassigned, NOT owned)

public:
    // Constructor takes pointer to SuperBlock.superbitmap_reserved field
    SuperBitmap(uint8_t* superblock_reserved_field);

    // Set bit for a bitmap page (mark as dirty)
    void set_bit(uint32_t page_idx) noexcept;

    // Clear bit for a bitmap page (mark as clean)
    void clear_bit(uint32_t page_idx) noexcept;

    // Test if bitmap page is dirty
    bool test_bit(uint32_t page_idx) const noexcept;

    // Clear all bits (mark all pages clean)
    void clear_all() noexcept;

    // Get raw data pointer (points into SuperBlock.superbitmap_reserved)
    uint8_t* data() noexcept;
    const uint8_t* data() const noexcept;
};

} // namespace ublkpp::raid1
