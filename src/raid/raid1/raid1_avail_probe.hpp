#pragma once

#include <memory>
#include <thread>

namespace ublkpp::raid1 {

struct MirrorDevice;

class Raid1AvailProbeTask {
    std::jthread _probe;

public:
    void launch(std::shared_ptr< MirrorDevice > mirror, uint64_t reserved_size);
    void stop() noexcept { _probe.request_stop(); }
    bool is_running() const noexcept { return _probe.joinable(); }
};

} // namespace ublkpp::raid1
