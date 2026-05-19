#pragma once

#include <linux/time_types.h>

namespace ublkpp::raid1 {

// Timeout for io_uring_submit_and_wait_timeout() in _run_resync_loop().
// stop() returns within approximately one tick after STOPPING is set.
inline constexpr __kernel_timespec k_resync_tick{.tv_sec = 0, .tv_nsec = 500'000};

} // namespace ublkpp::raid1
