#pragma once

#include <string>

#include <sisl/metrics/metrics.hpp>

namespace ublkpp {

struct UblkDiskMetrics : public sisl::MetricsGroupWrapper {
    explicit UblkDiskMetrics(std::string const& device_name);
    ~UblkDiskMetrics();
};

} // namespace ublkpp
