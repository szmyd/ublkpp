#include "bitmap.hpp"

#include <isa-l/mem_routines.h>
#include <ublk_cmd.h>

#include "lib/logging.hpp"

namespace ublkpp::raid1 {
constexpr auto bits_in_word = k_bits_in_byte * sizeof(Bitmap::word_t);

struct free_page {
    void operator()(void* x) { free(x); }
};

Bitmap::Bitmap(uint64_t data_size, uint32_t chunk_size, uint32_t align) :
        _data_size(data_size),
        _chunk_size(chunk_size),
        _align(align),
        _page_width(_chunk_size * k_page_size * k_bits_in_byte),
        _num_pages(_data_size / _page_width + ((0 == _data_size % _page_width) ? 0 : 1)) {
    void* new_page{nullptr};
    if (auto err = ::posix_memalign(&new_page, _align, k_page_size); err)
        throw std::runtime_error("OutOfMemory"); // LCOV_EXCL_LINE
    memset(new_page, 0, k_page_size);
    _clean_page.reset(reinterpret_cast< word_t* >(new_page), free_page());
}

// We use uint64_t pointers to access the allocated pages. calc_bitmap_region will return:
//      * page_offset  : The page key in our page map (generated if we have a hole currently)
//      * word_offset  : The uint64_t offset within the raid1::page_size byte array
//      * shift_offset : The bits to begin setting within the word indicated
//      * nr_bits      : The number of bits fit in this word
//      * sz           : The number of bytes to represent as "dirty" from this index
std::tuple< uint32_t, uint32_t, uint32_t, uint32_t, uint64_t > Bitmap::calc_bitmap_region(uint64_t addr, uint64_t len,
                                                                                          uint32_t chunk_size) {
    auto const page_width_bits =
        chunk_size * k_page_size * k_bits_in_byte;  // Number of bytes represented by a single page (block)
    auto const page = addr / page_width_bits;       // Which page does this address land in
    auto const page_off = (addr % page_width_bits); // Bytes within the page
    auto const page_bit = (page_off / chunk_size);  // Bit within the page
    auto const sz = std::min(len, (page_width_bits - page_off));

    // If our offset does not align on chunk boundary, then we need to add a bit as we've written over into the
    // next word, it's unexpected that this will require writing into a third word
    auto const alignment = addr % chunk_size;
    auto const left_hand = std::min(chunk_size - alignment, sz);
    auto const right_hand = (sz - left_hand) % chunk_size;
    auto const middle = sz - (left_hand + right_hand);
    uint32_t const nr_bits = (left_hand ? 1 : 0) + ((middle) / chunk_size) + (right_hand ? 1 : 0);

    return std::make_tuple(page,                                         // Page that address references
                           page_bit / bits_in_word,                      // Word in the page LCOV_EXCL_LINE
                           bits_in_word - (page_bit % bits_in_word) - 1, // Shift within the Word
                           nr_bits,
                           sz); // Tail size of the page
}

void Bitmap::init_to(UblkDisk& device) {
    // TODO should be able to use discard if supported here. Need to add support in the Drivers first in sync_iov call
    RLOGD("Initializing RAID-1 Bitmaps on: [{}]", device);
    auto iov = iovec{.iov_base = _clean_page.get(), .iov_len = k_page_size};
    for (auto pg_idx = 0UL; _num_pages > pg_idx; ++pg_idx) {
        auto res = device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, k_page_size + (pg_idx * k_page_size));
        if (!res) { throw std::runtime_error(fmt::format("Failed to write: {}", res.error().message())); }
    }
}

io_result Bitmap::sync_to(UblkDisk& device) {
    auto iov = iovec{.iov_base = nullptr, .iov_len = k_page_size};
    for (auto& [pg_offset, page] : _page_map) {
        if (0 == isal_zero_detect(page.get(), k_page_size)) continue;
        RLOGD("Syncing Bitmap page: {} to [{}]", pg_offset, device)
        iov.iov_base = page.get();
        auto page_addr = (k_page_size * pg_offset) + k_page_size;
        if (auto res = device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, page_addr); !res) return res;
    }
    return 0;
}

void Bitmap::load_from(UblkDisk& device) {
    // We read each page from the Device into memory if it is not ZERO'd out
    auto iov = iovec{.iov_base = nullptr, .iov_len = k_page_size};
    for (auto pg_idx = 0UL; _num_pages > pg_idx; ++pg_idx) {
        RLOGT("Loading page: {} of {} page(s)", pg_idx + 1, _num_pages);
        if (nullptr == iov.iov_base) {
            if (auto err = ::posix_memalign(&iov.iov_base, device.block_size(), k_page_size);
                0 != err || nullptr == iov.iov_base) [[unlikely]] { // LCOV_EXCL_START
                if (EINVAL == err) RLOGE("Invalid Argument while initializing superblock!")
                throw std::runtime_error("OutOfMemory");
            } // LCOV_EXCL_STOP
        }
        if (auto res = device.sync_iov(UBLK_IO_OP_READ, &iov, 1, k_page_size + (pg_idx * k_page_size)); !res) {
            free(iov.iov_base);
            throw std::runtime_error(fmt::format("Failed to read: {}", res.error().message()));
        }
        // If page is empty; leave a hole
        if (0 == isal_zero_detect(iov.iov_base, k_page_size)) continue;
        RLOGT("Page: {} is *DIRTY*!", pg_idx + 1)

        // Insert new dirty page into page map
        auto [it, _] = _page_map.emplace(std::make_pair(pg_idx, nullptr));
        if (_page_map.end() == it) throw std::runtime_error("Could not insert new page"); // LCOV_EXCL_LINE
        it->second.reset(reinterpret_cast< word_t* >(iov.iov_base), free_page());
        iov.iov_base = nullptr;
    }
    if (nullptr != iov.iov_base) free(iov.iov_base);
}

Bitmap::word_t* Bitmap::__get_page(uint64_t offset, bool creat) {
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
        it->second.reset(reinterpret_cast< word_t* >(new_page), free_page());
    }
    return it->second.get();
}

bool Bitmap::is_dirty(uint64_t addr, uint32_t len) {
    for (auto off = 0U; len > off;) {
        auto [page_offset, word_offset, shift_offset, nr_bits, sz] =
            calc_bitmap_region(addr + off, len - off, _chunk_size);
        off += sz;
        // Check for a dirty page
        auto cur_page = __get_page(page_offset);
        if (!cur_page) continue;

        auto cur_word = cur_page + word_offset;

        // Handle update crossing multiple words (optimization potential?)
        for (auto bits_left = nr_bits; 0 < bits_left;) {
            auto const bits_to_read = std::min(shift_offset + 1, bits_left);
            auto const bits_to_check = htobe64(64 == bits_to_read ? UINT64_MAX
                                                                  : (((uint64_t)0b1 << bits_to_read) - 1)
                                                       << (shift_offset - (bits_to_read - 1)));
            bits_left -= bits_to_read;
            if (0 != (cur_word->load(std::memory_order_acquire) & bits_to_check)) return true;
            ++cur_word;
            shift_offset = bits_in_word - 1; // Word offset back to the beginning
        }
    }
    return false;
}

size_t Bitmap::dirty_pages() {
    auto cnt =
        std::erase_if(_page_map, [](const auto& it) { return (0 == isal_zero_detect(it.second.get(), k_page_size)); });
    if (0 < cnt) { RLOGD("Dropped {} page(s) from the Bitmap", cnt); }
    return _page_map.size();
}

std::tuple< Bitmap::word_t*, uint32_t, uint32_t > Bitmap::clean_page(uint64_t addr, uint32_t len) {
    // Since we can require updating multiple pages on a page boundary write we need to loop here with a cursor
    // Calculate the tuple mentioned above
    auto [page_offset, word_offset, shift_offset, nr_bits, sz] = calc_bitmap_region(addr, len, _chunk_size);

    // Get/Create a Page
    auto const cur_page = __get_page(page_offset);
    DEBUG_ASSERT_NOTNULL(cur_page, "Expected to find dirty page!")
    if (!cur_page) return std::make_tuple(cur_page, page_offset, sz);
    auto cur_word = cur_page + word_offset;

    // Handle update crossing multiple words (optimization potential?)
    for (auto bits_left = nr_bits; 0 < bits_left;) {
        auto const bits_to_write = std::min(shift_offset + 1, bits_left);
        bits_left -= bits_to_write;
        auto const bits_to_clear = htobe64(64 == bits_to_write ? UINT64_MAX
                                                               : (((uint64_t)0b1 << bits_to_write) - 1)
                                                   << (shift_offset - (bits_to_write - 1)));
        cur_word->fetch_and(~bits_to_clear, std::memory_order_release);
        ++cur_word;
        shift_offset = bits_in_word - 1; // Word offset back to the beginning
    }
    // Only return clean pages
    if (0 == isal_zero_detect(cur_page, k_page_size)) return std::make_tuple(_clean_page.get(), page_offset, sz);
    return std::make_tuple(nullptr, page_offset, sz);
}

std::pair< uint64_t, uint32_t > Bitmap::next_dirty() {
    auto it = _page_map.begin();
    // Find the first dirty word
    if (_page_map.end() == it) return std::make_pair(0, 0);
    uint64_t logical_off = static_cast< uint64_t >(_page_width) * it->first;

    // Find the first dirty word
    auto word = 0UL;
    for (auto word_off = 0U; (k_page_size / sizeof(word_t)) > word_off; ++word_off) {
        word = be64toh((it->second.get() + word_off)->load(std::memory_order_relaxed));
        if (0 == word) continue;
        logical_off += (word_off * bits_in_word * _chunk_size); // Adjust for word
        break;
    }

    // How long does the dirt stretch?
    uint32_t sz = 0;
    if (0 != word) {
        auto set_bit = __builtin_clzl(word);
        logical_off += set_bit * _chunk_size; // Adjust for bit within word
        RLOGT("addr: {:0x} word: {:064b}", logical_off, word);
        // Consume as many consecutive set-bits as we can in the rest of the word
        while ((static_cast< int >(bits_in_word) > set_bit) && ((word >> (bits_in_word - (set_bit++) - 1)) & 0b1)) {
            sz += _chunk_size;
        }
    }
    if (_data_size < (logical_off + sz)) sz = (_data_size - logical_off);
    return std::make_pair(logical_off, sz);
}

// Returns:
//      * page         : Pointer to the page
//      * page_offset  : Page index
//      * sz           : The number of bytes from the provided `len` that fit in this page
uint64_t Bitmap::dirty_page(uint64_t addr, uint64_t len) {
    // Since we can require updating multiple pages on a page boundary write we need to loop here with a cursor
    // Calculate the tuple mentioned above
    auto [page_offset, word_offset, shift_offset, nr_bits, sz] = calc_bitmap_region(addr, len, _chunk_size);

    // Get/Create a Page
    auto cur_page = __get_page(page_offset, true);
    if (!cur_page) throw std::runtime_error("Could not insert new page");
    auto cur_word = cur_page + word_offset;
    // Handle update crossing multiple words (optimization potential?)
    bool updated{false};
    for (auto bits_left = nr_bits; 0 < bits_left;) {
        auto const bits_to_write = std::min(shift_offset + 1, bits_left);
        auto const bits_to_set = htobe64(64 == bits_to_write ? UINT64_MAX
                                                             : (((uint64_t)0b1 << bits_to_write) - 1)
                                                 << (shift_offset - (bits_to_write - 1)));
        bits_left -= bits_to_write;
        auto const was = cur_word->fetch_or(bits_to_set, std::memory_order_release);
        ++cur_word;
        shift_offset = bits_in_word - 1;                  // Word offset back to the beginning
        if ((was & bits_to_set) == bits_to_set) continue; // These chunks are already dirty!
        updated = true;
    }
    if (!updated) cur_page = nullptr;
    return sz;
}
} // namespace ublkpp::raid1
