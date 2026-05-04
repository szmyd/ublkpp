#pragma once

#include <sys/uio.h>

#include <algorithm>
#include <bit>
#include <ranges>

namespace ublkpp {
template < typename T >
constexpr T ilog2(T x) {
    return sizeof(T) * 8 - 1 - std::countl_zero(x);
}

constexpr auto Ki = 1024UL;
constexpr auto Mi = Ki * Ki;
constexpr auto Gi = Mi * Ki;
constexpr auto Ti = Gi * Ki;

constexpr auto SECTOR_SIZE = 512UL;
constexpr auto SECTOR_SHIFT = ilog2(SECTOR_SIZE);
constexpr auto DEFAULT_BLOCK_SIZE = 4 * Ki;
constexpr auto DEFAULT_BS_SHIFT = ilog2(DEFAULT_BLOCK_SIZE);

inline auto iovec_len(iovec const* begin, iovec const* end) {
    return std::ranges::fold_left(std::ranges::subrange{begin, end}, decltype(iovec::iov_len){0},
                                  [](auto acc, iovec const& v) { return acc + v.iov_len; });
}

} // namespace ublkpp
