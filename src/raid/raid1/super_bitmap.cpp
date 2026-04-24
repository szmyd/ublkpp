#include "super_bitmap.hpp"

#include <atomic>
#include <bit>

#include "lib/logging.hpp"

// SuperBitmap implementation uses lock-free atomic operations for thread safety.
// Individual bit operations (set_bit, clear_bit, test_bit) use std::atomic_ref
// to perform atomic read-modify-write operations on individual bytes.
// This allows concurrent modifications to different bits in the same byte without races.

namespace ublkpp::raid1 {

SuperBitmap::SuperBitmap(uint8_t* superblock_reserved_field) : _bits(superblock_reserved_field) {
    // NOTE: We do NOT clear_all() here because the superblock may contain
    // existing bitmap state that was loaded from disk. The caller should
    // explicitly call clear_all() if they want to initialize a new bitmap.
}

void SuperBitmap::set_bit(uint32_t page_idx) noexcept {
    DEBUG_ASSERT_LT(page_idx, k_superbitmap_bits, "SuperBitmap page_idx out of bounds");
    auto const byte_idx = page_idx / 8;
    auto const bit_idx = page_idx % 8;
    // Use atomic fetch_or to safely set the bit without racing with other bit operations
    std::atomic_ref< uint8_t >(_bits[byte_idx]).fetch_or(1U << bit_idx, std::memory_order_relaxed);
}

void SuperBitmap::clear_bit(uint32_t page_idx) noexcept {
    DEBUG_ASSERT_LT(page_idx, k_superbitmap_bits, "SuperBitmap page_idx out of bounds");
    auto const byte_idx = page_idx / 8;
    auto const bit_idx = page_idx % 8;
    // Use atomic fetch_and to safely clear the bit without racing with other bit operations
    std::atomic_ref< uint8_t >(_bits[byte_idx]).fetch_and(~(1U << bit_idx), std::memory_order_relaxed);
}

bool SuperBitmap::test_bit(uint32_t page_idx) const noexcept {
    DEBUG_ASSERT_LT(page_idx, k_superbitmap_bits, "SuperBitmap page_idx out of bounds");
    auto const byte_idx = page_idx / 8;
    auto const bit_idx = page_idx % 8;
    // Use atomic load to safely read the byte without seeing torn reads
    auto const byte_val = std::atomic_ref< const uint8_t >(_bits[byte_idx]).load(std::memory_order_relaxed);
    return (byte_val & (1U << bit_idx)) != 0;
}

void SuperBitmap::clear_all() noexcept {
    if (!_bits) return;
    // Use byte-by-byte atomic stores so concurrent set_bit() calls (from write I/Os that arrive
    // during a device swap) don't produce UB via memset's potentially-wider stores racing with
    // the per-byte atomic_ref operations on the same memory.
    for (size_t i = 0; i < k_superbitmap_size; ++i)
        std::atomic_ref< uint8_t >(_bits[i]).store(0, std::memory_order_relaxed);
}

uint32_t SuperBitmap::next_set_bit(uint32_t start_page) const noexcept {
    auto byte_idx = start_page / 8;
    auto const start_bit = start_page % 8;

    // Handle the partial first byte: mask off bits below start_bit (LSB-first layout).
    // When start_bit == 0, the mask is ~0 so no bits are cleared.
    if (byte_idx < k_superbitmap_size) {
        auto byte_val = std::atomic_ref< const uint8_t >(_bits[byte_idx]).load(std::memory_order_acquire);
        byte_val &= ~static_cast< uint8_t >((1U << start_bit) - 1);
        if (byte_val != 0) { return byte_idx * 8 + std::countr_zero(byte_val); }
        ++byte_idx;
    }

    for (; byte_idx < k_superbitmap_size; ++byte_idx) {
        auto const byte_val = std::atomic_ref< const uint8_t >(_bits[byte_idx]).load(std::memory_order_acquire);
        if (byte_val != 0) { return byte_idx * 8 + std::countr_zero(byte_val); }
    }
    return k_superbitmap_bits;
}

uint8_t* SuperBitmap::data() noexcept { return _bits; }

const uint8_t* SuperBitmap::data() const noexcept { return _bits; }

} // namespace ublkpp::raid1
