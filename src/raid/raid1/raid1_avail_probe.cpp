#include "raid1_avail_probe.hpp"

#include <condition_variable>
#include <mutex>

#include <sisl/options/options.h>
#include <ublksrv.h>

#include "lib/logging.hpp"
#include "raid1_impl.hpp"

namespace ublkpp::raid1 {

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
        auto const delay = std::chrono::seconds(SISL_OPTIONS["avail_delay"].as< uint32_t >());
        std::mutex m;
        std::condition_variable_any cv;
        while (!st.stop_requested()) {
            {
                std::unique_lock lk{m};
                cv.wait_for(lk, st, delay, [] { return false; });
            }
            if (st.stop_requested()) break;
            bool const was_unavail = mirror->unavail.test(std::memory_order_acquire);
            if (!probe_mirror(*mirror, rs) && !was_unavail) {
                RLOGW("Idle probe: device failed during idle: {}", *mirror->disk)
            }
        }
    });
}

} // namespace ublkpp::raid1
