#pragma once

#include <sisl/utility/enum.hpp>

namespace ublkpp {

using sub_cmd_t = uint16_t;
constexpr auto sqe_tag_width = 16U;
constexpr auto sqe_op_width = 8U;
constexpr auto sqe_tgt_data_width = sizeof(sub_cmd_t) * 8U;
constexpr auto sqe_is_tgt_width = 1U;
constexpr auto sqe_reserved_width = 64U - (sqe_tag_width + sqe_op_width + sqe_tgt_data_width + sqe_is_tgt_width);

// Device Specific Flags
constexpr auto _flag_width = 8U;
constexpr auto _route_width = sqe_tgt_data_width - _flag_width;
ENUM(sub_cmd_flags, sub_cmd_t, NONE = 0, REPLICATED = 1, RETRIED = 2, INTERNAL = 4);

inline sub_cmd_t set_flags(sub_cmd_t sub_cmd, sub_cmd_flags const flags) {
    return sub_cmd | (static_cast< sub_cmd_t >(flags) << _route_width);
}

inline sub_cmd_t unset_flags(sub_cmd_t sub_cmd, sub_cmd_flags const flags) {
    return sub_cmd & ~(static_cast< sub_cmd_t >(flags) << _route_width);
}

inline bool test_flags(sub_cmd_t sub_cmd, sub_cmd_flags const flags) {
    return 0 < ((sub_cmd >> _route_width) & static_cast< sub_cmd_t >(flags));
}

inline bool is_retry(sub_cmd_t sub_cmd) { return test_flags(sub_cmd, sub_cmd_flags::RETRIED); }

// SubCmd routing for Error Handling
constexpr auto _route_mask = (1U << _route_width) - 1U;

inline sub_cmd_t shift_route(sub_cmd_t sub_cmd, sub_cmd_t const shift) { return (_route_mask & sub_cmd) << shift; }
} // namespace ublkpp
