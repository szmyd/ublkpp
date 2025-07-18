#include "raid1_impl.hpp"

#include <isa-l/mem_routines.h>
#include <ublk_cmd.h>

#include "lib/logging.hpp"

namespace ublkpp::raid1 {
constexpr auto bits_in_uint64 = k_bits_in_byte * sizeof(uint64_t);

struct free_page {
    void operator()(void* x) { free(x); }
};

// We use uint64_t pointers to access the allocated pages. calc_bitmap_region will return:
//      * page_offset  : The page key in our page map (generated if we have a hole currently)
//      * word_offset  : The uint64_t offset within the raid1::page_size byte array
//      * shift_offset : The bits to begin setting within the word indicated
//      * sz           : The number of bytes to represent as "dirty" from this index
std::tuple< uint32_t, uint32_t, uint32_t, uint64_t > Bitmap::calc_bitmap_region(uint64_t addr, uint32_t len,
                                                                                uint32_t chunk_size) {
    auto const page_width_bits =
        chunk_size * k_page_size * k_bits_in_byte;    // Number of bytes represented by a single page (block)
    auto const page = addr / page_width_bits;         // Which page does this address land in
    auto const page_off = (addr % page_width_bits);   // Bytes within the page
    auto const page_bit = (page_off / chunk_size);    // Bit within the page
    return std::make_tuple(page,                      // Page that address references
                           page_bit / bits_in_uint64, // Word in the page LCOV_EXCL_LINE
                           bits_in_uint64 - (page_bit % bits_in_uint64) - 1,     // Shift within the Word
                           std::min((uint64_t)len, (page_width_bits - page_off)) // Tail size of the page
    );
}

void Bitmap::init_to(UblkDisk& dev_a, UblkDisk& dev_b) {
    // TODO we should be able to use discard if supported here. Need to add support in the Drivers first in sync_iov
    // call
    RLOGI("Initializing RAID-1 Bitmaps on \n\tDeviceA:[{}]\n\t\tand \n\tDeviceB:[{}]", dev_a, dev_b);
    auto iov = iovec{.iov_base = nullptr, .iov_len = k_page_size};
    if (auto err = ::posix_memalign(&iov.iov_base, std::max(dev_a.block_size(), dev_b.block_size()), k_page_size);
        0 != err || nullptr == iov.iov_base) [[unlikely]] { // LCOV_EXCL_START
        if (EINVAL == err) RLOGE("Invalid Argument while initializing superblock!")
        throw std::runtime_error("OutOfMemory");
    } // LCOV_EXCL_STOP
    memset(iov.iov_base, 0, k_page_size);
    for (auto pg_idx = 0UL; _num_pages > pg_idx; ++pg_idx) {
        auto res_a = dev_a.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, k_page_size + (pg_idx * k_page_size));
        auto res_b = dev_b.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, k_page_size + (pg_idx * k_page_size));
        if (!res_a || !res_b) {
            free(iov.iov_base);
            throw std::runtime_error(
                fmt::format("Failed to read: {}", !res_a ? res_a.error().message() : res_b.error().message()));
        }
    }
    free(iov.iov_base);
}

void Bitmap::load_from(UblkDisk& device) {
    // We read each page from the Device into memory if it is not ZERO'd out
    auto iov = iovec{.iov_base = nullptr, .iov_len = k_page_size};
    for (auto pg_idx = 0UL; _num_pages > pg_idx; ++pg_idx) {
        RLOGT("Loading page: {} of {} page(s)", pg_idx + 1, _num_pages);
        if (auto err = ::posix_memalign(&iov.iov_base, device.block_size(), k_page_size);
            0 != err || nullptr == iov.iov_base) [[unlikely]] { // LCOV_EXCL_START
            if (EINVAL == err) RLOGE("Invalid Argument while initializing superblock!")
            throw std::runtime_error("OutOfMemory");
        } // LCOV_EXCL_STOP
        if (auto res = device.sync_iov(UBLK_IO_OP_READ, &iov, 1, k_page_size + (pg_idx & k_page_size)); !res) {
            free(iov.iov_base);
            throw std::runtime_error(fmt::format("Failed to read: {}", res.error().message()));
        }
        // If page is empty; leave a hole
        if (0 == isal_zero_detect(iov.iov_base, k_page_size)) continue;
        RLOGT("Page: {} is *DIRTY*!", pg_idx + 1);

        // Insert new dirty page into page map
        auto [it, _] = _page_map.emplace(std::make_pair(pg_idx, nullptr));
        if (_page_map.end() == it) throw std::runtime_error("Could not insert new page"); // LCOV_EXCL_LINE
        it->second.reset(reinterpret_cast< uint64_t* >(iov.iov_base), free_page());
        iov.iov_base = nullptr;
    }
}

uint64_t* Bitmap::__get_page(uint64_t offset, bool creat) {
    if (!creat) {
        if (auto it = _page_map.find(offset); _page_map.end() == it)
            return nullptr;
        else
            return it->second.get();
    }

    auto [it, happened] = _page_map.emplace(std::make_pair(offset, nullptr));
    if (happened) {
        void* new_page{nullptr};
        if (auto err = ::posix_memalign(&new_page, _align, k_page_size); err) return nullptr; // LCOV_EXCL_LINE
        memset(new_page, 0, k_page_size);
        it->second.reset(reinterpret_cast< uint64_t* >(new_page), free_page());
    }
    return it->second.get();
}

bool Bitmap::is_dirty(uint64_t addr, uint32_t len) {
    auto [page_offset, word_offset, shift_offset, sz] = calc_bitmap_region(addr, len, _chunk_size);
    // Check for a dirty page
    auto cur_page = __get_page(page_offset);
    if (!cur_page) return false;

    auto cur_word = cur_page + word_offset;

    // If our offset does not align on chunk boundary, then we need to add a bit as we've written over into the
    // next word, it's unexpected that this will require writing into a third word
    uint32_t nr_bits = (sz / _chunk_size) + ((0 < (sz % _chunk_size)) ? 1 : 0);
    if ((sz > _chunk_size) && (0 != addr) % _chunk_size) ++nr_bits;

    // Handle update crossing multiple words (optimization potential?)
    for (auto bits_left = nr_bits; 0 < bits_left;) {
        auto const bits_to_write = std::min(shift_offset + 1, bits_left);
        auto const bits_to_set =
            htobe64((((uint64_t)0b1 << bits_to_write) - 1) << (shift_offset - (bits_to_write - 1)));
        bits_left -= bits_to_write;
        if (0 != (*cur_word & bits_to_set)) return true;
        ++cur_word;
        shift_offset = 63; // Word offset back to the beginning
    }
    return false;
}

// Returns:
//      * page         : Pointer to the page
//      * page_offset  : Page index
//      * sz           : The number of bytes from the provided `len` that fit in this page
std::tuple< uint64_t*, uint32_t, uint32_t > Bitmap::dirty_page(uint64_t addr, uint32_t len) {
    // Since we can require updating multiple pages on a page boundary write we need to loop here with a cursor
    // Calculate the tuple mentioned above
    auto [page_offset, word_offset, shift_offset, sz] = calc_bitmap_region(addr, len, _chunk_size);

    // Get/Create a Page
    auto cur_page = __get_page(page_offset, k_page_size);
    if (!cur_page) return std::make_tuple(cur_page, page_offset, sz);
    auto cur_word = cur_page + word_offset;
    // If our offset does not align on chunk boundary, then we need to add a bit as we've written over into the next
    // word, it's unexpected that this will require writing into a third word
    uint32_t nr_bits = (sz / _chunk_size) + ((0 < (sz % _chunk_size)) ? 1 : 0);

    // Handle update crossing multiple words (optimization potential?)
    bool updated{false};
    for (auto bits_left = nr_bits; 0 < bits_left;) {
        auto const bits_to_write = std::min(shift_offset + 1, bits_left);
        auto const bits_to_set = htobe64(64 == bits_to_write ? UINT64_MAX
                                                             : (((uint64_t)0b1 << bits_to_write) - 1)
                                                 << (shift_offset - (bits_to_write - 1)));
        bits_left -= bits_to_write;
        if ((*cur_word & bits_to_set) == bits_to_set) continue; // These chunks are already dirty!
        updated = true;
        (*cur_word) |= bits_to_set;
        ++cur_word;
        shift_offset = 63; // Word offset back to the beginning
    }
    if (!updated) cur_page = nullptr;
    return std::make_tuple(cur_page, page_offset, sz);
}
} // namespace ublkpp::raid1
