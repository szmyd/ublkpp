#pragma once

#include <cstdint>

#include <fmt/format.h>

namespace ublkpp {

using sub_cmd_t = uint16_t;
constexpr auto sqe_tag_width = 16U;
constexpr auto sqe_op_width = 8U;
constexpr auto sqe_tgt_data_width = sizeof(sub_cmd_t) * 8U;
constexpr auto sqe_is_tgt_width = 1U;
constexpr auto sqe_reserved_width = 64U - (sqe_tag_width + sqe_op_width + sqe_tgt_data_width + sqe_is_tgt_width);

// SubCmd routing
constexpr auto _route_width = sqe_tgt_data_width;
constexpr auto _route_mask = (1U << _route_width) - 1U;

inline sub_cmd_t shift_route(sub_cmd_t sub_cmd, sub_cmd_t const shift) { return (_route_mask & sub_cmd) << shift; }

inline auto to_string(sub_cmd_t const& sub_cmd) { return fmt::format("{:016b}", sub_cmd); }
} // namespace ublkpp
