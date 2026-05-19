#pragma once

#include <chrono>
#include <linux/time_types.h>

namespace ublkpp::raid1 {

// Timeout for io_uring_submit_and_wait_timeout() in _run_resync_loop().
// stop() returns within approximately one tick after STOPPING is set.
inline constexpr __kernel_timespec k_resync_tick{.tv_sec = 0, .tv_nsec = 500'000};

// Watchdog deadline for the STOPPING drain. If in-flight SQEs are not resolved within this
// window we force-clear all slots to unblock stop(). Sized at 30 s as a conservative upper
// bound: block-device kernel timeouts are typically 30–120 s, and IORING_ASYNC_CANCEL_ANY
// (submitted at the start of STOPPING) should resolve SQEs within milliseconds on reachable
// devices. The watchdog fires only when a device is truly hung beyond that window.
inline constexpr auto k_watchdog_timeout = std::chrono::seconds(30);

} // namespace ublkpp::raid1
