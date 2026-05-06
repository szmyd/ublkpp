#pragma once

#include <cstdint>
#include <memory>

namespace ublkpp::raid1 {

struct MirrorDevice;

// Probe a mirror device: reads at reserved_size, clears unavail on success,
// sets unavail on failure. Returns true if device is available.
bool probe_mirror(MirrorDevice& mirror, uint64_t reserved_size) noexcept;

} // namespace ublkpp::raid1
