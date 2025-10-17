#pragma once

extern "C" {
#include <endian.h>
}

#include <boost/uuid/uuid.hpp>
#include <sisl/logging/logging.h>
#include <sisl/utility/enum.hpp>

#include "ublkpp/lib/ublk_disk.hpp"

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

ENUM(read_route, uint8_t, EITHER = 0, DEVA = 1, DEVB = 2);

#ifdef __LITTLE_ENDIAN
struct __attribute__((__packed__)) SuperBlock {
    struct {
        uint8_t magic[16]; // This is a static set of 128bits to confirm existing superblock
        uint16_t version;
        uint8_t uuid[16]; // This is a user UUID that is assigned when the array is created
    } header;
    struct {
        // was cleanly unmounted, position in RAID1 and current Healthy device
        uint8_t clean_unmount : 1, read_route : 2, device_b : 1, : 0;
        struct {
            uint8_t _reserved[16]; // Unused
            uint32_t chunk_size;   // Number of bytes each bit represents
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

extern SuperBlock* pick_superblock(SuperBlock* dev_a, raid1::SuperBlock* dev_b);
extern io_result write_superblock(UblkDisk& device, raid1::SuperBlock* sb, bool device_b);
extern folly::Expected< std::pair< raid1::SuperBlock*, bool >, std::error_condition >
load_superblock(UblkDisk& device, boost::uuids::uuid const& uuid, uint32_t const chunk_size);

} // namespace raid1
} // namespace ublkpp
