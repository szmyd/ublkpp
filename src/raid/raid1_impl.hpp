#pragma once

extern "C" {
#include <endian.h>
}
#include <map>
#include <tuple>

#include <sisl/logging/logging.h>
#include "ublkpp/raid/raid1.hpp"

namespace ublkpp {

namespace raid1 {
constexpr auto const k_bits_in_byte = 8UL;
//  Cap some array parameters so we can make simple assumptions later
constexpr auto k_max_dev_size = 32 * Ti;
constexpr auto k_min_chunk_size = 32 * Ki;
constexpr auto k_max_bitmap_chunks = k_max_dev_size / k_min_chunk_size;
// Use a single bit to represent each chunk
constexpr auto k_max_bitmap_size = k_max_bitmap_chunks / k_bits_in_byte;
constexpr auto k_page_size = 4 * Ki;

class Bitmap {
    using map_type_t = std::map< uint32_t, std::shared_ptr< uint64_t > >;

    uint32_t _align;
    uint32_t _chunk_size;
    map_type_t _page_map;

    uint64_t* __get_page(uint64_t offset, bool creat = false);

public:
    Bitmap(uint32_t chunk_size, uint32_t align = k_page_size) : _align(align), _chunk_size(chunk_size) {}

    auto page_size() const { return k_page_size; }

    bool is_dirty(uint64_t addr, uint32_t len);

    // Tuple of form [page*, page_offset, size_consumed (max len)]
    std::tuple< uint64_t*, uint32_t, uint32_t > dirty_page(uint64_t addr, uint32_t len);

    // Each bit in the BITMAP represents a single "Chunk" of size chunk_size
    static std::tuple< uint32_t, uint32_t, uint32_t, uint64_t > calc_bitmap_region(uint64_t addr, uint32_t len,
                                                                                   uint32_t chunk_size);
};

#ifdef __LITTLE_ENDIAN
struct __attribute__((__packed__)) SuperBlock {
    static constexpr auto SIZE = k_page_size;
    struct {
        uint8_t magic[16]; // This is a static set of 128bits to confirm existing superblock
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
    uint8_t _reserved[k_page_size - (sizeof(header) + sizeof(fields))];
};
static_assert(k_page_size == sizeof(SuperBlock), "Size of raid1::SuperBlock does not match SIZE!");
#else
#error "Big Endian not supported!"
#endif

constexpr uint64_t reserved_size = sizeof(SuperBlock) + k_max_bitmap_size;

extern std::pair< SuperBlock*, read_route > pick_superblock(SuperBlock* dev_a, raid1::SuperBlock* dev_b);

} // namespace raid1

} // namespace ublkpp
