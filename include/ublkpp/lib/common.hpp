#pragma once

extern "C" {
#include <sys/uio.h>
}

#include <bit>

namespace ublkpp {
template < typename T >
constexpr T ilog2(T x) {
    return sizeof(T) * 8 - 1 - std::countl_zero(x);
}

[[maybe_unused]] constexpr auto Ki = 1024UL;
[[maybe_unused]] constexpr auto Mi = Ki * Ki;
[[maybe_unused]] constexpr auto Gi = Mi * Ki;

constexpr auto SECTOR_SIZE = 512UL;
constexpr auto SECTOR_SHIFT = ilog2(SECTOR_SIZE);
constexpr auto DEFAULT_BLOCK_SIZE = 4 * Ki;
constexpr auto DEFAULT_BS_SHIFT = ilog2(DEFAULT_BLOCK_SIZE);

inline auto __iovec_len(iovec const* begin, iovec const* end) {
    auto res = decltype(iovec::iov_len){0};
    while (begin != end)
        res += (begin++)->iov_len;
    return res;
}

} // namespace ublkpp
