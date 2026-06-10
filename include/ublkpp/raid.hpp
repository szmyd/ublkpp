#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <sisl/utility/enum.hpp>

namespace ublkpp {

class ublk_disk;
using disk_handle = std::shared_ptr< ublk_disk >;

// Construct a RAID0 stripe set. `disks` becomes the array (ownership consumed).
// Throws std::runtime_error on bad geometry / superblock probe failure.
disk_handle make_raid0_disk(boost::uuids::uuid const& uuid, uint32_t stripe_size_bytes,
                            std::vector< disk_handle >&& disks);

// Construct a 2-way RAID1 mirror from `dev_a` + `dev_b`. `parent_id` is woven into the metrics
// labels; pass empty if metrics correlation is not needed.
// Throws std::runtime_error on bad geometry / superblock probe failure.
disk_handle make_raid1_disk(boost::uuids::uuid const& uuid, disk_handle dev_a, disk_handle dev_b,
                            std::string const& parent_id = "");

// Construct a RAID10 (RAID0 of RAID1 pairs) from 2N raw leaves. Legs are paired in order:
// legs[0]/legs[1] form pair 0, legs[2]/legs[3] form pair 1, etc. Each pair K is assigned a
// partition UUID derived from `uuid` via name_generator("partition_K").
// `parent_id` is threaded into each per-pair RAID1 for metrics correlation.
// Throws std::runtime_error if legs.size() < 4 or is odd.
disk_handle make_raid10_disk(boost::uuids::uuid const& uuid, uint32_t stripe_size_bytes,
                             std::vector< disk_handle >&& legs, std::string const& parent_id = "");

// Construct a placeholder disk representing a missing mirror leg. All I/O fails; is_missing()
// returns true. Pass to make_raid1_disk() when a leg is unavailable and awaiting hot-swap.
disk_handle make_missing_disk();

namespace raid0 {

// Returns the stripe leg at `stripe_offset`, or nullptr if out of range / not a Raid0 disk.
disk_handle get_device(ublk_disk const& disk, uint32_t stripe_offset) noexcept;

} // namespace raid0

namespace md {

// Result of a non-destructive superblock probe on a raw block device.
enum class disk_type {
    md_none,    // no md or MdDisk superblock found
    md_native,  // clean md 1.2 SB at byte 4096; ready for first import
    md_stamped, // MdDisk SB at byte 0; ready for transparent reattach
    md_dirty,   // md 1.2 SB at byte 4096 but array is not clean (resync/reshape active)
};

// Open the device backing `leaf` (via leaf->id()), read ~8 KiB, and return which
// superblock is present. Never writes to the device. `leaf` must not be missing.
// Throws std::system_error if the device cannot be opened or read. Callers should
// probe the first non-missing leg of a volume; the factory functions validate all
// legs regardless.
disk_type probe(disk_handle const& leaf);

// One-way migration of a clean md-raid 1.2 array onto ublkpp's RAID stack. User data is
// preserved in-place; ublkpp metadata occupies the dead zone md left before data_offset.
// Each leg is wrapped in a per-leg shim (4 KiB SB at byte 0) that survives reboots so
// subsequent attaches are transparent. `uuid` must equal the array's md set_uuid.
// Throws std::runtime_error if the array is dirty, the level/layout is unsupported, cross-leg
// topology mismatches, or `uuid` doesn't match.

// Accepts md level 1 (2-way mirror) or level 10 (2-leg near=2). Legs must have complementary
// dev_roles (0 and 1).
disk_handle make_md_raid1_disk(boost::uuids::uuid const& uuid, std::pair< disk_handle, disk_handle > legs,
                               std::string const& parent_id = "");

// Accepts 2N legs from a near=2 md-raid 10 array in any order. Pairs by dev_role / 2.
// RAID0 stripe size taken from md's chunk_size.
disk_handle make_md_raid10_disk(boost::uuids::uuid const& uuid, std::vector< disk_handle >&& legs,
                                std::string const& parent_id = "");

} // namespace md

namespace raid1 {

ENUM(replica_state, uint8_t, CLEAN = 0, SYNCING = 1, ERROR = 2, UNAVAIL = 3);

// Default-constructed value is a reserved sentinel: the implementation never produces both
// legs in ERROR simultaneously (the active leg is always CLEAN or UNAVAIL), so wrong-type
// queries are distinguishable from any valid Raid1 state.
struct array_state {
    replica_state device_a{replica_state::ERROR};
    replica_state device_b{replica_state::ERROR};
    uint64_t bytes_to_sync{0};
};

// Replace one leg of the mirror identified by `old_device_id` with `new_device`. On success
// returns the displaced leg. On rejection (no matching id, geometry mismatch, would-degrade-
// active-leg, etc.) returns `new_device` unchanged so the caller can identify rejection by
// pointer-equality. Aborts if `disk` is not a Raid1 mirror (programmer error).
disk_handle swap_device(ublk_disk& disk, std::string const& old_device_id, disk_handle new_device);

// Returns the per-leg replica state + bytes-to-sync. If `disk` is not a Raid1 mirror, returns
// the default-constructed value (see array_state).
array_state replica_states(ublk_disk const& disk) noexcept;

// Returns both legs of the mirror, or {nullptr, nullptr} if `disk` is not a Raid1 mirror.
std::pair< disk_handle, disk_handle > replicas(ublk_disk const& disk) noexcept;

} // namespace raid1

} // namespace ublkpp
