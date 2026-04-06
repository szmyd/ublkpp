#pragma once

#include <atomic>
#include <memory>
#include <tuple>
#include <vector>

#include "ublkpp/lib/ublk_disk.hpp"
#include "super_bitmap.hpp"

namespace ublkpp::raid1 {

static_assert(sizeof(uint64_t) == sizeof(std::atomic_uint64_t), "BITMAP Cannot be ATOMIC!");
class Bitmap {
public:
    using word_t = std::atomic_uint64_t;

    struct PageData {
        // Null shared_ptr = page is clean (no memory allocated). Lazy-allocated on first dirty_region.
        // atomic<shared_ptr> allows lock-free CAS during concurrent lazy allocation.
        std::atomic< std::shared_ptr< word_t > > page{nullptr};
        std::atomic< bool > loaded_from_disk{false}; // true = loaded unchanged, false = modified/new

        PageData() = default;

        // Move constructor/assignment are needed for vector construction only (single-threaded).
        // Not safe to move a PageData that is concurrently accessed.
        PageData(PageData&& other) noexcept :
                page(other.page.load(std::memory_order_relaxed)),
                loaded_from_disk(other.loaded_from_disk.load(std::memory_order_relaxed)) {}

        PageData& operator=(PageData&& other) noexcept {
            if (this != &other) {
                page.store(other.page.load(std::memory_order_relaxed), std::memory_order_relaxed);
                loaded_from_disk.store(other.loaded_from_disk.load(std::memory_order_relaxed),
                                       std::memory_order_relaxed);
            }
            return *this;
        }

        PageData(PageData const&) = delete;
        PageData& operator=(PageData const&) = delete;
    };

private:
    std::string const _id;
    uint64_t _data_size;
    uint32_t _chunk_size;
    uint32_t _align;
    // Pre-allocated to _num_pages at construction. Slots never inserted or erased after init,
    // eliminating all structural modifications and the need for a mutex.
    std::vector< PageData > _page_map;
    std::shared_ptr< word_t > _clean_page;

    uint32_t const _page_width; // Number of bytes represented by a single page (block)
    size_t const _num_pages;
    std::atomic_uint64_t _dirty_chunks_est{0};
    SuperBitmap _super_bitmap;

private:
    PageData* __get_page(uint64_t offset, bool creat = false);
    static size_t max_pages_per_tx(const UblkDisk& device);

public:
    Bitmap(uint64_t data_size, uint32_t chunk_size, uint32_t align, uint8_t* superbitmap_reserved,
           std::string const& id = "");

    static uint64_t page_size();
    size_t dirty_pages();
    uint64_t dirty_data_est() const;

    bool is_dirty(uint64_t addr, uint32_t len);

    // Tuple of form [page*, page_offset, size_consumed (max len)]
    void dirty_region(uint64_t addr, uint64_t len);
    std::tuple< word_t*, uint32_t, uint32_t > clean_region(uint64_t addr, uint32_t len);
    std::pair< uint64_t, uint32_t > next_dirty();

    // Each bit in the BITMAP represents a single "Chunk" of size chunk_size
    static std::tuple< uint32_t, uint32_t, uint32_t, uint32_t, uint64_t >
    calc_bitmap_region(uint64_t addr, uint64_t len, uint32_t chunk_size);

    void init_to(std::shared_ptr< UblkDisk > device);
    io_result sync_to(UblkDisk& device, uint64_t offset = 0UL);
    void load_from(UblkDisk& device);
};

} // namespace ublkpp::raid1
