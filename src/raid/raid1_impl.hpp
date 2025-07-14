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

static inline auto calc_bitmap_region(uint64_t addr, uint32_t len, uint32_t block_size, uint32_t chunk_size) {
    auto const page_size_bits = chunk_size * block_size * 8;
    auto const page = addr / page_size_bits;
    DEBUG_ASSERT_LT(page, UINT32_MAX, "Page too big: {}", page) // LCOV_EXCL_LINE
    auto const page_offset = addr % page_size_bits;
    DEBUG_ASSERT_LT(page_offset, UINT32_MAX, "Pageoffset too big: {}", page_offset) // LCOV_EXCL_LINE
    uint32_t const chunk_offset = page_offset / (chunk_size * 8);
    auto const word_offset = chunk_offset % (sizeof(uint64_t) * 8);
    auto const shift_offset = (sizeof(uint64_t) * 8) - word_offset;
    auto const sz = std::min(len, ((uint32_t)(page_size_bits - page_offset)) * chunk_size);
    return std::make_tuple(page, word_offset, shift_offset - 1, sz);
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
