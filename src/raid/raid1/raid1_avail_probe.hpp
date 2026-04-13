#pragma once

#include <cstdint>
#include <memory>
#include <thread>

namespace ublkpp::raid1 {

struct MirrorDevice;

// Probe a mirror device: reads at reserved_size, clears unavail on success,
// sets unavail on failure. Returns true if device is available.
bool probe_mirror(MirrorDevice& mirror, uint64_t reserved_size) noexcept;

class Raid1AvailProbeTask {
    std::jthread _probe;

public:
    void launch(std::shared_ptr< MirrorDevice > mirror, uint64_t reserved_size);
    void stop() noexcept { _probe.request_stop(); }
};

} // namespace ublkpp::raid1
