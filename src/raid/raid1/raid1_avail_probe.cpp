#include "raid1_avail_probe.hpp"

#include <condition_variable>
#include <mutex>

#include <sisl/options/options.h>
#include <ublksrv.h>

#include "lib/logging.hpp"
#include "raid1_impl.hpp"

namespace ublkpp::raid1 {

// Probes mirror reachability with a single synchronous page read.
// Returns true  — device is reachable; unavail flag has been cleared.
// Returns false — device failed the probe; unavail flag has been set.
bool probe_mirror(MirrorDevice& mirror, uint64_t reserved_size) noexcept {
    alignas(k_page_size) uint8_t probe_buf[k_page_size];
    auto iov = iovec{.iov_base = probe_buf, .iov_len = k_page_size};
    if (auto res = mirror.disk->sync_iov(UBLK_IO_OP_READ, &iov, 1, reserved_size); res) {
        mirror.unavail.clear(std::memory_order_release);
        return true;
    }
    mirror.unavail.test_and_set(std::memory_order_acquire);
    return false;
}

void Raid1AvailProbeTask::launch(std::shared_ptr< MirrorDevice > mirror, uint64_t rs) {
    _probe = std::jthread([mirror = std::move(mirror), rs](std::stop_token st) {
        // Background probe only recovers UNAVAIL devices; CLEAN devices are not probed here
        // (the immediate_probe in idle_transition handles the synchronous entry-time check).
        // Enforce a minimum 1s between background probes so avail_delay=0 doesn't spin.
        auto const raw = SISL_OPTIONS["avail_delay"].as< uint32_t >();
        auto const delay = std::chrono::seconds(std::max(raw, 1u));
        std::mutex m;
        std::condition_variable_any cv;
        while (!st.stop_requested()) {
            {
                std::unique_lock lk{m};
                cv.wait_for(lk, st, delay, [] { return false; });
            }
            if (st.stop_requested()) break;
            if (!mirror->unavail.test(std::memory_order_acquire)) continue;
            if (!probe_mirror(*mirror, rs)) { RLOGD("Idle probe: device still unavailable: {}", *mirror->disk) }
        }
    });
}

} // namespace ublkpp::raid1
