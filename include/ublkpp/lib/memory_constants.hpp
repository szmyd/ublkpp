#pragma once
#include <cstdint>

namespace ublkpp {

// OS thread stack size (Linux default)
constexpr uint64_t k_thread_stack_size = 8 * 1024 * 1024; // 8 MiB

// Typical async_io structure size (from target/ublkpp_tgt.cpp)
constexpr uint64_t k_async_io_size = 40; // ~40 bytes

// Page size constant
constexpr uint64_t k_page_size = 4096; // 4 KiB

// PageData structure overhead (from raid1/bitmap.hpp)
constexpr uint64_t k_page_data_overhead = 16; // atomic + shared_ptr

} // namespace ublkpp
