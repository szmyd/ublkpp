#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <tuple>

#include "raid1_superblock.hpp"

namespace ublkpp::raid1 {

static_assert(sizeof(uint64_t) == sizeof(std::atomic_uint64_t), "BITMAP Cannot be ATOMIC!");
class Bitmap {
public:
    using word_t = std::atomic_uint64_t;
    using map_type_t = std::map< uint32_t, std::shared_ptr< word_t > >;

private:
    uint64_t _data_size;
    uint32_t _chunk_size;
    uint32_t _align;
    map_type_t _page_map;
    std::shared_ptr< word_t > _clean_page;

    uint32_t const _page_width; // Number of bytes represented by a single page (block)
    uint32_t const _num_pages;

    word_t* __get_page(uint64_t offset, bool creat = false);

public:
    Bitmap(uint64_t data_size, uint32_t chunk_size, uint32_t align);

    auto page_size() const { return k_page_size; }
    size_t dirty_pages();

    bool is_dirty(uint64_t addr, uint32_t len);

    // Tuple of form [page*, page_offset, size_consumed (max len)]
    std::tuple< word_t*, uint32_t, uint32_t > dirty_page(uint64_t addr, uint64_t len);
    std::tuple< word_t*, uint32_t, uint32_t > clean_page(uint64_t addr, uint32_t len);
    std::pair< uint64_t, uint32_t > next_dirty();

    // Each bit in the BITMAP represents a single "Chunk" of size chunk_size
    static std::tuple< uint32_t, uint32_t, uint32_t, uint32_t, uint64_t >
    calc_bitmap_region(uint64_t addr, uint64_t len, uint32_t chunk_size);

    void init_to(UblkDisk& device);
    io_result sync_to(UblkDisk& device);
    void load_from(UblkDisk& device);
};
} // namespace ublkpp::raid1
