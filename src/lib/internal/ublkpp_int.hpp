#pragma once

#include "ublkpp/lib/ublk_disk.hpp"

namespace ublkpp::detail {

// Access broker for the raw ublk_params owned by ublk_disk. Restricted to ublksrv handshake
// code in src/target; not exposed in the installed public headers. For a composite disk
// (RAID0/1/10) this returns the aggregated params that the composite's ctor built from its
// leaves' public getters, so the kernel sees one geometry for the whole stack.
struct params_access {
    static ublk_params const* of(ublk_disk const& d) noexcept;
};

} // namespace ublkpp::detail
