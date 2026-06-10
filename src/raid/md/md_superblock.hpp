#pragma once

extern "C" {
#include <endian.h>
}

#include <cstddef>
#include <cstdint>

#include "lib/common.hpp"

namespace ublkpp::md {

constexpr auto k_page_size = 4 * Ki;
constexpr uint16_t k_sb_version = 1;
constexpr size_t k_magic_size = 16;

// 128-bit magic identifying an MdDisk-stamped leg. Distinct from raid0::magic_bytes
// and raid1::magic_bytes; if any of those collide we want a clean parse failure rather
// than misidentifying the wrapper.
extern uint8_t const k_magic[k_magic_size];

#ifdef __LITTLE_ENDIAN
struct __attribute__((__packed__)) SuperBlock {
    static constexpr size_t SIZE = k_page_size;
    struct {
        uint8_t magic[k_magic_size];
        uint16_t version;        // BE16
        uint8_t md_set_uuid[16]; // md set_uuid; cross-leg sanity
    } header;                    // 34 bytes
    struct {
        uint64_t data_offset;      // BE64. bytes (md sectors << 9)
        uint32_t md_chunk_size;    // BE32. bytes (0 for md raid 1)
        uint16_t raid_disks;       // BE16
        uint16_t dev_role;         // BE16. this leg's md dev_role[N]
        uint8_t layout_near;       // near=2 for md raid 10; 0 for md raid 1
        uint8_t layout_far;        // 0
        uint8_t layout_far_offset; // 0
        uint8_t md_level;          // 1 or 10. Was _pad0 in MdDisk SB v1; legacy v1 arrays
                                   // read 0 here and are treated as level 10 (the only
                                   // level v1 ever accepted).
        uint64_t events_at_import; // BE64. diagnostic
    } fields;                      // 32 bytes
    uint8_t _reserved[k_page_size - sizeof(header) - sizeof(fields)];
};
static_assert(k_page_size == sizeof(SuperBlock), "Size of md::SuperBlock must equal k_page_size");
static_assert(sizeof(SuperBlock::header) == 34, "md::SuperBlock::header size mismatch");
static_assert(sizeof(((SuperBlock*)nullptr)->fields) == 32, "md::SuperBlock::fields size mismatch");
#else
#error "Big Endian not supported!"
#endif

} // namespace ublkpp::md
