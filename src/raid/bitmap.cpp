#include "raid1_impl.hpp"

namespace ublkpp::raid1 {
struct free_page {
    void operator()(void* x) { free(x); }
};

// Each bit in the BITMAP represents a single "Chunk" of size chunk_size
std::tuple< uint32_t, uint32_t, uint32_t, uint64_t > Bitmap::calc_bitmap_region(uint64_t addr, uint32_t len,
                                                                                uint32_t chunk_size) {
    static auto const bits_in_uint64 = k_bits_in_byte * sizeof(uint64_t);
    auto const page_width_bits =
        chunk_size * k_page_size * k_bits_in_byte;    // Number of bytes represented by a single page (block)
    auto const page = addr / page_width_bits;         // Which page does this address land in
    auto const page_off = (addr % page_width_bits);   // Bytes within the page
    auto const page_bit = (page_off / chunk_size);    // Bit within the page
    return std::make_tuple(page,                      // Page that address references
                           page_bit / bits_in_uint64, // Word in the page
                           bits_in_uint64 - (page_bit % bits_in_uint64) - 1,     // Shift within the Word
                           std::min((uint64_t)len, (page_width_bits - page_off)) // Tail size of the page
    );
}

uint64_t* Bitmap::get_page(uint64_t offset, bool creat) {
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
        memset(new_page, 0, raid1::k_page_size);
        it->second.reset(reinterpret_cast< uint64_t* >(new_page), free_page());
    }
    return it->second.get();
}

bool Bitmap::is_dirty(uint64_t addr, uint32_t len) {
    auto [page_offset, word_offset, shift_offset, sz] = calc_bitmap_region(addr, len, _chunk_size);
    // Check for a dirty page
    if (auto cur_page = get_page(page_offset); cur_page) {
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
    }
    return false;
}

// We use uint64_t pointers to access the allocated pages. calc_bitmap_region will return:
//      * page_offset  : The page key in our page map (generated if we have a hole currently)
//      * word_offset  : The uint64_t offset within the raid1::page_size byte array
//      * shift_offset : The bits to begin setting within the word indicated
//      * sz           : The number of bytes to represent as "dirty" from this index
std::tuple< uint64_t*, uint32_t, uint32_t > Bitmap::dirty_page(uint64_t addr, uint32_t len) {
    // Since we can require updating multiple pages on a page boundary write we need to loop here with a cursor
    // Calculate the tuple mentioned above
    auto [page_offset, word_offset, shift_offset, sz] = calc_bitmap_region(addr, len, _chunk_size);

    // Get/Create a Page
    auto cur_page = get_page(page_offset, k_page_size);
    if (!cur_page) return std::make_tuple(cur_page, page_offset, sz);
    auto cur_word = cur_page + word_offset;
    uint32_t nr_bits = (sz / _chunk_size) + ((0 < (sz % _chunk_size)) ? 1 : 0);

    // If our offset does not align on chunk boundary, then we need to add a bit as we've written over into the next
    // word, it's unexpected that this will require writing into a third word
    if ((sz > _chunk_size) && (0 != addr) % _chunk_size) ++nr_bits;

    // Handle update crossing multiple words (optimization potential?)
    bool updated{false};
    for (auto bits_left = nr_bits; 0 < bits_left;) {
        auto const bits_to_write = std::min(shift_offset + 1, bits_left);
        auto const bits_to_set =
            htobe64((((uint64_t)0b1 << bits_to_write) - 1) << (shift_offset - (bits_to_write - 1)));
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
