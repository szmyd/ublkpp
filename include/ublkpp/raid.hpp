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

// Construct a placeholder disk representing a missing mirror leg. All I/O fails; is_missing()
// returns true. Pass to make_raid1_disk() when a leg is unavailable and awaiting hot-swap.
disk_handle make_missing_disk();

namespace raid0 {

// Returns the stripe leg at `stripe_offset`, or nullptr if out of range / not a Raid0 disk.
disk_handle get_device(ublk_disk const& disk, uint32_t stripe_offset) noexcept;

} // namespace raid0

namespace md {

// One-way, in-place migration of an md-raid 10 array (near=2 layout, superblock 1.2) onto
// ublkpp's RAID-10 stack (RAID0 over N RAID1 mirrors). User data is preserved byte-for-byte
// at md's data_offset on each leg; ublkpp metadata is written into the dead zone md left in
// front of the data area.
//
// Each leaf is wrapped in a thin per-leg shim that persists across reboots (its own 4 KiB SB
// at leaf byte 0). Subsequent attaches walk the shim transparently.
//
// Validation (throws std::runtime_error on any failure):
//   - Each leg must carry a valid md 1.2 SB at byte 4 KiB (or a previously-stamped MdDisk SB
//     at byte 0 if the array was already migrated).
//   - Array must be clean: resync_offset == MaxSector, no reshape/recovery active.
//   - Layout must be near=2 (far/offset rejected).
//   - Cross-leg: set_uuid, md_chunk_size, data_offset, raid_disks, layout, events all match.
//   - dev_roles complementary: pair K covers roles 2K and 2K+1 for K in [0, raid_disks/2).
//
// The first call on a fresh import overwrites the md superblock at byte 4 KiB so the kernel
// will not auto-assemble it on next boot; after that call returns successfully, the array
// is no longer recognizable to mdadm.

// Wrap a single leaf in an MdDisk shim. Useful for testing or manual composition; production
// code should prefer the higher-level make_md_raid1_disk / make_md_raid10_disk factories.
disk_handle make_md_disk(disk_handle leaf);

// Construct a 2-way RAID1 mirror from a near=2 md-raid 10 leg pair. Both legs must carry the
// same md set_uuid and have complementary dev_roles (2K and 2K+1).
disk_handle make_md_raid1_disk(boost::uuids::uuid const& uuid, std::pair< disk_handle, disk_handle > legs,
                               std::string const& parent_id = "");

// Construct an N-pair RAID-10 (RAID0 of RAID1 mirrors) from a near=2 md-raid 10 array. Accepts
// 2N raw leaves (any order). Pairs are reconstructed by dev_role / 2; mirrors are then composed
// in ascending pair-K order so md's chunk-to-pair mapping is preserved byte-identical. The
// RAID0 stripe size is taken from md's chunk_size.
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
