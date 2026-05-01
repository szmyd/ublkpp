#pragma once

#include "ublkpp/lib/ublk_disk.hpp"

namespace ublkpp::test {

// Default submit_iov action: return 1 to signal "submitted"; inject_cqe() delivers the result.
inline auto make_async_iov_action() {
    return [](ublksrv_queue const*, ublk_io_data const*, iovec*, uint32_t, uint64_t) -> io_result { return 1; };
}

} // namespace ublkpp::test
