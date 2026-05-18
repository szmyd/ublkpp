#pragma once

#include <linux/time_types.h>

namespace ublkpp::raid1 {

// Timeout used by both sleep_tick() (coroutine path) and the run_resync_queue_loop() outer
// wait (target path). stop() returns within approximately one tick after signalling STOPPING.
inline constexpr __kernel_timespec k_resync_tick{.tv_sec = 0, .tv_nsec = 500'000};

} // namespace ublkpp::raid1
