#pragma once

extern "C" {
#include <endian.h>
}
#include <tuple>

namespace ublkpp {

namespace raid1 {
constexpr uint64_t reserved_size = 128 * Mi;
constexpr uint64_t reserved_sectors = reserved_size >> SECTOR_SHIFT;

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
