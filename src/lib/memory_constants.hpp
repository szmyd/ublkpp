#pragma once
#include <cstdint>

namespace ublkpp {

// OS thread stack size (Linux default)
constexpr uint64_t k_thread_stack_size = 8 * 1024 * 1024; // 8 MiB

// Page size constant
constexpr uint64_t k_page_size = 4096; // 4 KiB

} // namespace ublkpp
