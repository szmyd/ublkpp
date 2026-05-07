#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <boost/uuid/uuid.hpp>

#include "ublkpp/lib/ublk_disk.hpp"

#include "md_superblock.hpp"

namespace ublkpp::md {

// Topology captured from md's 1.2 superblock at first migration. Persisted in MdDisk's own
// SB so subsequent attaches can re-derive the address translation without re-parsing md
// metadata (which RAID1 above will have already overwritten).
struct DiscoveredTopology {
    boost::uuids::uuid set_uuid; // md array set_uuid
    uint64_t data_offset_bytes;  // md data_offset, in bytes (md sectors << 9)
    uint32_t md_chunk_size;      // md chunksize, in bytes
    uint16_t raid_disks;         // md raid_disks
    uint16_t dev_role;           // md dev_role[N] for this leg
    uint8_t layout_near;         // 2 (near=2 only)
    uint8_t layout_far;          // 0
    uint8_t layout_far_offset;   // 0
    uint64_t events;             // md events counter at import time
};

// MdDisk wraps a leaf and applies a piecewise address translation so user data continues to
// live at md's data_offset on the leaf. Inserted only between leaves and RAID1 for legs
// imported from md-raid; fresh ublkpp arrays do not use this wrapper.
//
// Per-leg layout (after migration):
//   [0,                    4 KiB)              MdDisk SB
//   [4 KiB,                8 KiB)              upper RAID1 SB (lands here via head-zone shift)
//   [8 KiB,                8 KiB + bmp)        upper RAID1 bitmap
//   [8 KiB + bmp,          12 KiB + bmp + pad) upper RAID0 SB (member offset 0 in RAID0)
//   [..., data_offset_md)                      DEAD ZONE (alignment slack)
//   [data_offset_md, leaf_size)                user data, byte-identical to md
class MdDisk : public ublk_disk {
public:
    explicit MdDisk(disk_handle leaf);
    ~MdDisk() override;

    DiscoveredTopology const& topology() const noexcept { return _topo; }
    disk_handle const& leaf() const noexcept { return _leaf; }
    // Upper-layer offset where the user-data band begins. Anything below this is the
    // RAID1 metadata band; anything at/above maps into md's user data on the leaf.
    uint64_t data_band_threshold() const noexcept { return _data_band_threshold; }
    // Byte width of the user-data band (rounded down for max_sectors alignment; may be
    // up to max_sectors_bytes - 1 less than `leaf_capacity - data_offset_md`).
    uint64_t data_band_size() const noexcept { return _data_band_size; }

    std::string id() const noexcept override { return _id; }
    prepare_result prepare(ublksrv_queue const* q, int const iouring_device_start) override;
    void probe_tick(ublksrv_queue const* q) noexcept override;
    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept override;

private:
    disk_handle _leaf;
    DiscoveredTopology _topo{};
    std::string _id;

    // Boundary between RAID1's metadata band (uses head-zone shift) and the user-data band
    // (uses data-band shift). Equals what RAID1 will compute as its own _reserved_size.
    uint64_t _data_band_threshold{0};
    // Constant: skip past MdDisk's own SB (one page at leaf byte 0).
    static constexpr uint64_t k_head_zone_shift = k_page_size;
    // Adds the dead-zone width so user data lands exactly at md's data_offset.
    uint64_t _data_band_shift{0};
    // Effective data-band size (= leaf_size - data_offset_md, possibly aligned-down).
    uint64_t _data_band_size{0};

    void __probe_and_init();
    void __populate_params();
    static uint64_t __compute_raid1_reserved(uint64_t presented_size, uint64_t max_sectors_bytes) noexcept;
};

} // namespace ublkpp::md
