#pragma once

extern "C" {
#include <endian.h>
}

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
