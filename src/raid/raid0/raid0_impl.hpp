#pragma once

extern "C" {
#include <endian.h>
}

#include <algorithm>
#include <map>
#include <tuple>

namespace ublkpp::raid0 {

constexpr auto k_page_size = 4 * Ki;

inline auto next_subcmd(uint32_t const stride_width, uint32_t const stripe_size, uint64_t const addr,
                        uint32_t const len) {
    // If single disk, nothing needed
    if (stride_width == stripe_size) [[unlikely]]
        return std::make_tuple(0U, addr, len);
    auto const chunk_num = addr / stride_width; // Which stride
    auto const offset_in_stride = addr % stride_width;
    uint32_t const device_off = offset_in_stride / stripe_size;     // Which disk
    auto const chunk_off = offset_in_stride % stripe_size;          // Offset in stripe
    auto const logical_off = (chunk_num * stripe_size) + chunk_off; // Logical offset
    auto const sz = std::min(len, static_cast< uint32_t >(stripe_size - chunk_off));
    return std::make_tuple(device_off, logical_off, sz);
}

// For operations that don't require a buffer being passed (e.g. Discard) we can optimize by merging I/Os that would
// access the same device after wrapping around the stride.
inline auto merged_subcmds(uint32_t const stride_width, uint32_t const stripe_size, uint64_t addr, uint64_t const len) {
    auto ret = std::map< uint32_t, std::pair< uint64_t, uint64_t > >();
    // If single disk, no splitting needed
    if (stride_width == stripe_size) {
        ret[0] = std::make_pair(addr, len);
        return ret;
    }
    for (auto cur = 0UL; len > cur;) {
        auto const [device_off, logical_off, sz] = next_subcmd(stride_width, stripe_size, addr, len - cur);
        if (auto it = ret.find(device_off); ret.end() != it) {
            it->second.second += sz;
        } else {
            ret[device_off] = std::make_pair(logical_off, sz);
        }
        cur += sz;
        addr += sz;
    }
    return ret;
}

#ifdef __LITTLE_ENDIAN
struct __attribute__((__packed__)) SuperBlock {
    struct {
        uint8_t magic[16]; // This is a unconsumed set of 128bits to confirm existing superblock
        uint16_t version;
        uint8_t uuid[16];
    } header;
    struct {
        uint16_t stripe_off;  // Position within the array
        uint32_t stripe_size; // Number of bytes before rotating devices
    } fields;
    uint8_t _reserved[k_page_size - (sizeof(header) + sizeof(fields))];
};
static_assert(k_page_size == sizeof(SuperBlock), "Size of raid0::SuperBlock does not match SIZE!");
#else
#error "Big Endian not supported!"
#endif

} // namespace ublkpp::raid0
