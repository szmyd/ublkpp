#pragma once

#include <map>
#include <memory>
#include <tuple>

#include "raid1_impl.hpp"

namespace ublkpp::raid1 {

class Bitmap {
    using map_type_t = std::map< uint32_t, std::shared_ptr< uint64_t > >;

    uint64_t _data_size;
    uint32_t _chunk_size;
    uint32_t _align;
    map_type_t _page_map;

    uint32_t const _page_width_bits; // Number of bytes represented by a single page (block)
    uint32_t const _num_pages;

    uint64_t* __get_page(uint64_t offset, bool creat = false);

public:
    Bitmap(uint64_t data_size, uint32_t chunk_size, uint32_t align) :
            _data_size(data_size),
            _chunk_size(chunk_size),
            _align(align),
            _page_width_bits(_chunk_size * k_page_size * k_bits_in_byte),
            _num_pages(_data_size / _page_width_bits + ((0 == _data_size % _page_width_bits) ? 0 : 1)) {}

    auto page_size() const { return k_page_size; }

    bool is_dirty(uint64_t addr, uint32_t len);

    // Tuple of form [page*, page_offset, size_consumed (max len)]
    std::tuple< uint64_t*, uint32_t, uint32_t > dirty_page(uint64_t addr, uint32_t len);

    // Each bit in the BITMAP represents a single "Chunk" of size chunk_size
    static std::tuple< uint32_t, uint32_t, uint32_t, uint64_t > calc_bitmap_region(uint64_t addr, uint32_t len,
                                                                                   uint32_t chunk_size);

    void init_to(UblkDisk& device);
    void load_from(UblkDisk& device);
};
} // namespace ublkpp::raid1
