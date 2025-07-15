#pragma once

extern "C" {
#include <endian.h>
}
#include <tuple>

#include <sisl/logging/logging.h>

namespace ublkpp {

namespace raid1 {
constexpr uint64_t reserved_size = 128 * Mi;
constexpr uint64_t reserved_sectors = reserved_size >> SECTOR_SHIFT;

// Each bit in the BITMAP represents a single "Chunk" of size chunk_size
inline auto calc_bitmap_region(uint64_t addr, uint32_t len, uint32_t page_size, uint32_t chunk_size) {
    static auto const bits_in_byte = 8UL;
    static auto const bits_in_uint64 = bits_in_byte * sizeof(uint64_t);
    auto const page_size_bits =
        chunk_size * page_size * bits_in_byte;       // Number of bytes represented by a single page (block)
    auto const page = addr / page_size_bits;          // Which page does this address land in
    auto const page_off = (addr % page_size_bits);    // Bytes within the page
    auto const page_bit = (page_off / chunk_size);    // Bit within the page
    return std::make_tuple(page,                      // Page that address references
                           page_bit / bits_in_uint64, // Word in the page
                           bits_in_uint64 - (page_bit % bits_in_uint64) - 1,    // Shift within the Word
                           std::min((uint64_t)len, (page_size_bits - page_off)) // Tail size of the page
    );
}

#ifdef __LITTLE_ENDIAN
struct __attribute__((__packed__)) SuperBlock {
    static constexpr auto SIZE = 4 * Ki;
    struct {
        uint8_t magic[16]; // This is a unconsumed set of 128bits to confirm existing superblock
        uint16_t version;
        uint8_t uuid[16];
    } header;
    struct {
        uint8_t clean_unmount : 1, : 0; // was cleanly unmounted
        struct {
            uint32_t chunk_size; // Number of bytes each bit represents
            uint8_t dirty : 1, : 0;
            uint64_t age;
        } bitmap;
    } fields;
    uint8_t _reserved[SIZE - (sizeof(header) + sizeof(fields))];
};
static_assert(SuperBlock::SIZE == sizeof(SuperBlock), "Size of raid1::SuperBlock does not match SIZE!");
#else
#error "Big Endian not supported!"
#endif

extern std::pair< SuperBlock*, read_route > pick_superblock(SuperBlock* dev_a, raid1::SuperBlock* dev_b);

} // namespace raid1

} // namespace ublkpp
