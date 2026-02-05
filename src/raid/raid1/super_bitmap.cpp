#include "super_bitmap.hpp"

#include <cstring>

#include "lib/logging.hpp"

namespace ublkpp::raid1 {

SuperBitmap::SuperBitmap(uint8_t* superblock_reserved_field)
    : _bits(superblock_reserved_field) {
    // NOTE: We do NOT clear_all() here because the superblock may contain
    // existing bitmap state that was loaded from disk. The caller should
    // explicitly call clear_all() if they want to initialize a new bitmap.
}

void SuperBitmap::set_bit(uint32_t page_idx) {
    DEBUG_ASSERT_LT(page_idx, k_size_bits, "SuperBitmap page_idx out of bounds");
    auto const byte_idx = page_idx / 8;
    auto const bit_idx = page_idx % 8;
    _bits[byte_idx] |= (1U << bit_idx);
}

void SuperBitmap::clear_bit(uint32_t page_idx) {
    DEBUG_ASSERT_LT(page_idx, k_size_bits, "SuperBitmap page_idx out of bounds");
    auto const byte_idx = page_idx / 8;
    auto const bit_idx = page_idx % 8;
    _bits[byte_idx] &= ~(1U << bit_idx);
}

bool SuperBitmap::test_bit(uint32_t page_idx) const {
    DEBUG_ASSERT_LT(page_idx, k_size_bits, "SuperBitmap page_idx out of bounds");
    auto const byte_idx = page_idx / 8;
    auto const bit_idx = page_idx % 8;
    return (_bits[byte_idx] & (1U << bit_idx)) != 0;
}

void SuperBitmap::clear_all() {
    if (_bits) memset(_bits, 0x00, k_size_bytes);
}

void SuperBitmap::set_all() {
    if (_bits) memset(_bits, 0xFF, k_size_bytes);
}

uint8_t* SuperBitmap::data() {
    return _bits;
}

const uint8_t* SuperBitmap::data() const {
    return _bits;
}

size_t SuperBitmap::size() const {
    return k_size_bytes;
}

} // namespace ublkpp::raid1
