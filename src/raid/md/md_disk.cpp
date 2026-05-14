#include "md_disk.hpp"

extern "C" {
#include <endian.h>
}

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <stdexcept>
#include <vector>

#include <boost/uuid/name_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <fmt/format.h>
#include <ublk_cmd.h>
#include <ublksrv.h>

#include "ublkpp/raid.hpp"

#include "lib/common.hpp"
#include "lib/logging.hpp"
#include "raid/raid1/raid1_superblock.hpp" // for sizeof(raid1::SuperBlock) and k_min_chunk_size

namespace ublkpp::md {

namespace {

// md superblock 1.2 lives at byte 4 KiB on each leg. Field offsets per kernel
// include/uapi/linux/raid/md_p.h. Subset relevant to our parser.
constexpr uint64_t k_md_sb_offset = 4 * Ki;
constexpr uint32_t k_md_magic = 0xa92b4efc;
constexpr uint32_t k_md_major_version = 1;
constexpr uint32_t k_md_feature_reshape_active = 0x4;
constexpr uint32_t k_md_feature_recovery_offset = 0x10;
constexpr uint32_t k_md_level_raid1 = 1;
constexpr uint32_t k_md_level_raid10 = 10;
constexpr uint64_t k_md_sector_clean = ~uint64_t{0}; // MaxSector

constexpr size_t k_md_off_magic = 0x00;
constexpr size_t k_md_off_major_version = 0x04;
constexpr size_t k_md_off_feature_map = 0x08;
constexpr size_t k_md_off_set_uuid = 0x10;
constexpr size_t k_md_off_level = 0x48;
constexpr size_t k_md_off_layout = 0x4C;
constexpr size_t k_md_off_chunksize = 0x58;
constexpr size_t k_md_off_raid_disks = 0x5C;
constexpr size_t k_md_off_data_offset = 0x80;
constexpr size_t k_md_off_dev_number = 0xA0;
constexpr size_t k_md_off_events = 0xC8;
constexpr size_t k_md_off_resync_offset = 0xD0;
constexpr size_t k_md_off_dev_roles = 0x100;

// RAID1 aligns _reserved_size to the logical block size of its members (so user-data offsets
// stay block-aligned for O_DIRECT). MdDisk reports the leaf's block_size upward, so the
// alignment we need to reproduce here is the leaf's block_size.

// LE field readers - md SB is stored little-endian on disk.
inline uint16_t le16_at(uint8_t const* p, size_t off) noexcept {
    uint16_t v;
    std::memcpy(&v, p + off, sizeof(v));
    return le16toh(v);
}
inline uint32_t le32_at(uint8_t const* p, size_t off) noexcept {
    uint32_t v;
    std::memcpy(&v, p + off, sizeof(v));
    return le32toh(v);
}
inline uint64_t le64_at(uint8_t const* p, size_t off) noexcept {
    uint64_t v;
    std::memcpy(&v, p + off, sizeof(v));
    return le64toh(v);
}

struct AlignedBuf {
    void* ptr{nullptr};
    explicit AlignedBuf(uint32_t align, size_t sz) {
        if (auto err = ::posix_memalign(&ptr, align, sz); err || !ptr)
            throw std::runtime_error(fmt::format("MdDisk: posix_memalign({}, {}) failed: {}", align, sz, err));
        std::memset(ptr, 0, sz);
    }
    ~AlignedBuf() {
        if (ptr) std::free(ptr);
    }
    AlignedBuf(AlignedBuf const&) = delete;
    AlignedBuf& operator=(AlignedBuf const&) = delete;
};

// Read `len` bytes at leaf byte offset `off` directly from the leaf, bypassing any
// translation that MdDisk would apply through its own sync_iov.
io_result leaf_pread(ublk_disk& leaf, uint64_t off, void* buf, size_t len) noexcept {
    auto iov = iovec{.iov_base = buf, .iov_len = len};
    return leaf.sync_iov(UBLK_IO_OP_READ, &iov, 1, static_cast< off_t >(off));
}

io_result leaf_pwrite(ublk_disk& leaf, uint64_t off, void const* buf, size_t len) noexcept {
    auto iov = iovec{.iov_base = const_cast< void* >(buf), .iov_len = len};
    return leaf.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, static_cast< off_t >(off));
}

// Parse + validate md 1.2 SB at leaf byte 4 KiB. Throws std::runtime_error on any rejection
// (not an md array, wrong level/layout, dirty, reshape/recovery active, etc.).
DiscoveredTopology discovered_topo_from_md(ublk_disk& leaf) {
    AlignedBuf raw(leaf.block_size(), k_page_size);
    if (auto r = leaf_pread(leaf, k_md_sb_offset, raw.ptr, k_page_size); !r)
        throw std::runtime_error(fmt::format("MdDisk: failed to read md SB candidate: {}", r.error().message()));
    auto const* sb = static_cast< uint8_t const* >(raw.ptr);

    if (le32_at(sb, k_md_off_magic) != k_md_magic)
        throw std::runtime_error("MdDisk: leaf has no md superblock magic at byte 4096 (not an md 1.2 array)");
    if (le32_at(sb, k_md_off_major_version) != k_md_major_version)
        throw std::runtime_error("MdDisk: md superblock is not version 1 (only 1.2 supported)");

    auto const feature_map = le32_at(sb, k_md_off_feature_map);
    if (feature_map & k_md_feature_reshape_active)
        throw std::runtime_error("MdDisk: md array has reshape active; cannot import");
    if (feature_map & k_md_feature_recovery_offset)
        throw std::runtime_error("MdDisk: md array has recovery_offset set; cannot import");

    auto const md_level = le32_at(sb, k_md_off_level);
    if (md_level != k_md_level_raid1 && md_level != k_md_level_raid10)
        throw std::runtime_error(fmt::format("MdDisk: md array level {} is not supported (require 1 or 10)", md_level));

    auto const layout = le32_at(sb, k_md_off_layout);
    auto const near_copies = static_cast< uint8_t >(layout & 0xff);
    auto const far_copies = static_cast< uint8_t >((layout >> 8) & 0xff);
    auto const far_offset = static_cast< uint8_t >((layout >> 16) & 1);
    // For md raid 10 (level 10), layout field is meaningful. mdadm's default for near=2 is
    // 0x00010002 (far_copies=0, far_offset=1; the kernel ignores far_offset when far_copies<=1).
    // For md raid 1 (level 1), layout is unused by md and may be anything; don't validate.
    if (md_level == k_md_level_raid10 && (near_copies != 2 || far_copies > 1))
        throw std::runtime_error(
            fmt::format("MdDisk: only near=2 RAID-10 supported (layout=0x{:08x}: near={}, far={}, far_offset={})",
                        layout, near_copies, far_copies, far_offset));

    if (le64_at(sb, k_md_off_resync_offset) != k_md_sector_clean)
        throw std::runtime_error("MdDisk: md array is not in sync (resync_offset != MaxSector)");

    auto const raid_disks = le32_at(sb, k_md_off_raid_disks);
    if (md_level == k_md_level_raid10) {
        if (raid_disks < 4 || raid_disks > 256 || (raid_disks & 1U) != 0)
            throw std::runtime_error(
                fmt::format("MdDisk: unsupported md-raid 10 raid_disks={} (require even, >=4, <=256)", raid_disks));
    } else {
        // md raid 1: ublkpp RAID1 is strictly 2-way; reject N-way mirrors.
        if (raid_disks != 2)
            throw std::runtime_error(
                fmt::format("MdDisk: md-raid 1 with raid_disks={} is unsupported (ublkpp RAID1 is 2-way)", raid_disks));
    }

    auto const dev_number = le32_at(sb, k_md_off_dev_number);
    if (dev_number >= raid_disks)
        throw std::runtime_error(fmt::format("MdDisk: dev_number {} >= raid_disks {}", dev_number, raid_disks));

    auto const dev_role = le16_at(sb, k_md_off_dev_roles + 2 * dev_number);
    if (dev_role >= raid_disks)
        throw std::runtime_error(
            fmt::format("MdDisk: dev_role {} >= raid_disks {} (spare or missing slot)", dev_role, raid_disks));

    DiscoveredTopology t{};
    t.md_chunk_size = le32_at(sb, k_md_off_chunksize) << SECTOR_SHIFT;
    t.data_offset_bytes = le64_at(sb, k_md_off_data_offset) << SECTOR_SHIFT;
    t.raid_disks = static_cast< uint16_t >(raid_disks);
    t.dev_role = dev_role;
    t.layout_near = near_copies;
    t.layout_far = far_copies;
    t.layout_far_offset = far_offset;
    t.md_level = static_cast< uint8_t >(md_level);
    t.events = le64_at(sb, k_md_off_events);
    std::memcpy(t.set_uuid.data, sb + k_md_off_set_uuid, sizeof(t.set_uuid.data));

    // chunk_size only matters for md raid 10 (it becomes the RAID0 stripe size in the import
    // stack). md raid 1 has no striping, so chunk_size is meaningless and may be 0.
    if (md_level == k_md_level_raid10 && (t.md_chunk_size == 0 || t.md_chunk_size < raid1::k_min_chunk_size))
        throw std::runtime_error(fmt::format("MdDisk: md chunk_size {} is below ublkpp k_min_chunk_size {}",
                                             t.md_chunk_size, raid1::k_min_chunk_size));
    if (t.data_offset_bytes == 0)
        throw std::runtime_error("MdDisk: md data_offset is zero (1.0 superblock or unset; not supported)");

    DLOGI("MdDisk: discovered md-raid {} topology on {}: set_uuid={}, raid_disks={}, dev_role={}, "
          "chunk={}KiB, data_offset={}MiB, events={}",
          t.md_level, leaf, boost::uuids::to_string(t.set_uuid), t.raid_disks, t.dev_role, t.md_chunk_size / Ki,
          t.data_offset_bytes / Mi, t.events);

    return t;
}

} // namespace

uint64_t MdDisk::__compute_raid1_reserved(uint64_t presented_size, uint64_t max_sectors_bytes) noexcept {
    // Mirror raid1.cpp:137-145. k_min_chunk_size and sizeof(SuperBlock) come from raid1::.
    uint64_t const bitmap_size = (presented_size / raid1::k_min_chunk_size) / 8;
    uint64_t reserved = sizeof(raid1::SuperBlock) + bitmap_size;
    reserved += (presented_size - reserved) % max_sectors_bytes;
    return reserved;
}

MdDisk::MdDisk(disk_handle leaf, boost::uuids::uuid const& uuid) : ublk_disk(), _leaf(std::move(leaf)) {
    if (!_leaf) throw std::runtime_error("MdDisk: leaf is null");
    if (_leaf->is_missing()) throw std::runtime_error("MdDisk: leaf is missing");

    __probe_and_init(uuid);
    __populate_params();
    _id = fmt::format("MdDisk[{}@{}MiB]", _leaf->id(), _topo.data_offset_bytes / Mi);
}

MdDisk::~MdDisk() = default;

void MdDisk::__probe_and_init(boost::uuids::uuid const& expected_uuid) {
    // Look for our own superblock at leaf byte 0 (bypass any translation; this is BEFORE
    // _data_band_threshold/_data_band_shift are set).
    AlignedBuf own(_leaf->block_size(), k_page_size);
    if (auto r = leaf_pread(*_leaf, 0, own.ptr, k_page_size); !r)
        throw std::runtime_error(fmt::format("MdDisk: failed to read leaf SB candidate: {}", r.error().message()));
    auto* mdsb = static_cast< SuperBlock* >(own.ptr);

    if (std::memcmp(mdsb->header.magic, k_magic, k_magic_size) == 0) {
        if (be16toh(mdsb->header.version) > k_sb_version)
            throw std::runtime_error(fmt::format("MdDisk: unsupported MdDisk SB version {} (max {})",
                                                 be16toh(mdsb->header.version), k_sb_version));
        _topo.data_offset_bytes = be64toh(mdsb->fields.data_offset);
        _topo.md_chunk_size = be32toh(mdsb->fields.md_chunk_size);
        _topo.raid_disks = be16toh(mdsb->fields.raid_disks);
        _topo.dev_role = be16toh(mdsb->fields.dev_role);
        _topo.layout_near = mdsb->fields.layout_near;
        _topo.layout_far = mdsb->fields.layout_far;
        _topo.layout_far_offset = mdsb->fields.layout_far_offset;
        // md_level field was added after the initial MdDisk SB layout (where its byte was
        // _pad0=0). A zero on disk means a legacy v1 wrapper that only ever accepted level 10,
        // so back-fill that interpretation here.
        _topo.md_level = (mdsb->fields.md_level == 0) ? 10 : mdsb->fields.md_level;
        _topo.events = be64toh(mdsb->fields.events_at_import);
        std::memcpy(_topo.set_uuid.data, mdsb->header.md_set_uuid, sizeof(mdsb->header.md_set_uuid));

        // Verify the stored set_uuid matches the caller-provided volume uuid. Refuses to
        // attach a leg that belongs to a different array than the caller expects.
        if (_topo.set_uuid != expected_uuid)
            throw std::runtime_error(fmt::format(
                "MdDisk: stored set_uuid {} does not match expected volume uuid {} on {}",
                boost::uuids::to_string(_topo.set_uuid), boost::uuids::to_string(expected_uuid), _leaf->id()));

        DLOGI("MdDisk: re-attached existing wrapper on {}: set_uuid={}, dev_role={}, data_offset={}MiB", _leaf->id(),
              boost::uuids::to_string(_topo.set_uuid), _topo.dev_role, _topo.data_offset_bytes / Mi);
    } else {
        // Fresh import path: parse md SB, write our own SB, scrub md SB to prevent kernel auto-assemble.
        _topo = discovered_topo_from_md(*_leaf);

        // Verify md's set_uuid matches the caller-provided volume uuid. Production md arrays
        // are created with the volume uuid as the array set_uuid, so this check refuses to
        // import legs that don't belong to the expected volume.
        if (_topo.set_uuid != expected_uuid)
            throw std::runtime_error(fmt::format(
                "MdDisk: md array set_uuid {} does not match expected volume uuid {} on {}",
                boost::uuids::to_string(_topo.set_uuid), boost::uuids::to_string(expected_uuid), _leaf->id()));

        std::memset(own.ptr, 0, k_page_size);
        std::memcpy(mdsb->header.magic, k_magic, k_magic_size);
        mdsb->header.version = htobe16(k_sb_version);
        std::memcpy(mdsb->header.md_set_uuid, _topo.set_uuid.data, sizeof(mdsb->header.md_set_uuid));
        mdsb->fields.data_offset = htobe64(_topo.data_offset_bytes);
        mdsb->fields.md_chunk_size = htobe32(_topo.md_chunk_size);
        mdsb->fields.raid_disks = htobe16(_topo.raid_disks);
        mdsb->fields.dev_role = htobe16(_topo.dev_role);
        mdsb->fields.layout_near = _topo.layout_near;
        mdsb->fields.layout_far = _topo.layout_far;
        mdsb->fields.layout_far_offset = _topo.layout_far_offset;
        mdsb->fields.md_level = _topo.md_level;
        mdsb->fields.events_at_import = htobe64(_topo.events);
        if (auto w = leaf_pwrite(*_leaf, 0, own.ptr, k_page_size); !w)
            throw std::runtime_error(fmt::format("MdDisk: failed to write own SB: {}", w.error().message()));

        // Scrub md SB at byte 4 KiB so the kernel will not auto-assemble it on next boot.
        std::memset(own.ptr, 0, k_page_size);
        if (auto w = leaf_pwrite(*_leaf, k_md_sb_offset, own.ptr, k_page_size); !w)
            throw std::runtime_error(fmt::format("MdDisk: failed to scrub md SB: {}", w.error().message()));
        DLOGI("MdDisk: stamped wrapper SB on {}; md superblock at byte {} scrubbed", _leaf->id(), k_md_sb_offset);
    }

    // Compute the address translation. We solve for R = RAID1's _reserved_size that RAID1
    // will compute when MdDisk presents capacity = D + md_chunk_size + R. RAID1 aligns its
    // user-data area to its members' block_size for O_DIRECT, so (D + md_chunk_size) must be
    // a multiple of block_size. md_chunk_size is always a multiple of 512 (sectors) and md
    // aligns data_offset to >= 4 KiB, so on real arrays the residue is zero. We still trim
    // defensively, bounded by block_size - 1 (typically <= 4095 bytes).
    auto const leaf_size = _leaf->capacity();
    if (leaf_size <= _topo.data_offset_bytes)
        throw std::runtime_error(
            fmt::format("MdDisk: leaf capacity {} <= data_offset {}", leaf_size, _topo.data_offset_bytes));
    auto const align = static_cast< uint64_t >(_leaf->block_size());
    auto const D_raw = leaf_size - _topo.data_offset_bytes;
    auto const D_plus_chunk_aligned = ((D_raw + _topo.md_chunk_size) / align) * align;
    if (D_plus_chunk_aligned <= _topo.md_chunk_size)
        throw std::runtime_error(fmt::format("MdDisk: leaf too small after alignment (D_raw={}, md_chunk={}, align={})",
                                             D_raw, _topo.md_chunk_size, align));
    uint64_t const D = D_plus_chunk_aligned - _topo.md_chunk_size;
    _data_band_size = D;

    uint64_t R = sizeof(raid1::SuperBlock); // initial estimate
    for (int iter = 0; iter < 16; ++iter) {
        auto const presented = D + _topo.md_chunk_size + R;
        auto const new_R = __compute_raid1_reserved(presented, align);
        if (new_R == R) break;
        R = new_R;
    }
    if (auto const final_check = __compute_raid1_reserved(D + _topo.md_chunk_size + R, align); final_check != R) {
        throw std::runtime_error(
            fmt::format("MdDisk: failed to converge on RAID1 reserved size (R={}, recomputed={})", R, final_check));
    }

    _data_band_threshold = R + _topo.md_chunk_size;
    if (_topo.data_offset_bytes < k_head_zone_shift + _data_band_threshold)
        throw std::runtime_error(
            fmt::format("MdDisk: md data_offset ({} bytes) is too small for ublkpp metadata (need >= {})",
                        _topo.data_offset_bytes, k_head_zone_shift + _data_band_threshold));
    _data_band_shift = _topo.data_offset_bytes - _data_band_threshold;
}

void MdDisk::__populate_params() {
    auto& our_params = *params();

    // Inherit physical / logical block size and direct-io capability from the leaf.
    our_params.basic.logical_bs_shift = static_cast< uint8_t >(ilog2(_leaf->block_size()));
    our_params.basic.physical_bs_shift = static_cast< uint8_t >(ilog2(_leaf->physical_block_size()));
    our_params.basic.max_sectors = _leaf->max_tx() >> SECTOR_SHIFT;
    if (_leaf->can_discard())
        our_params.types |= UBLK_PARAM_TYPE_DISCARD;
    else
        our_params.types &= ~UBLK_PARAM_TYPE_DISCARD;
    _direct_io = _leaf->direct_io();

    // presented = D + md_chunk + R, where R = _data_band_threshold - md_chunk.
    auto const presented_size = _data_band_size + _data_band_threshold;
    our_params.basic.dev_sectors = presented_size >> SECTOR_SHIFT;
    if (can_discard())
        our_params.discard.discard_granularity = std::max(our_params.discard.discard_granularity, _leaf->block_size());
}

ublk_disk::prepare_result MdDisk::prepare(ublksrv_queue const* q, int const iouring_device_start) {
    return _leaf->prepare(q, iouring_device_start);
}

void MdDisk::probe_tick(ublksrv_queue const* q) noexcept { _leaf->probe_tick(q); }

disk_task< int > MdDisk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                                   uint64_t addr) {
    auto const op = ublksrv_get_op(data->iod);
    if (op == UBLK_IO_OP_FLUSH) co_return co_await _leaf->async_iov(q, data, iovecs, nr_vecs, addr).start();

    auto const len = static_cast< uint64_t >(iovec_len(iovecs, iovecs + nr_vecs));
    uint64_t leaf_addr;
    if (addr + len <= _data_band_threshold) {
        leaf_addr = addr + k_head_zone_shift;
    } else if (addr >= _data_band_threshold) {
        leaf_addr = addr + _data_band_shift;
    } else {
        // I/O straddling the head zone / data band boundary. Should not happen in practice
        // because RAID1 metadata writes are aligned to k_page_size and never cross _reserved_size,
        // and RAID0/RAID1 user-data writes are aligned to chunk_size. Defensive failure.
        DLOGE("MdDisk: I/O straddles head-zone boundary [addr={:#x}, len={:#x}, threshold={:#x}]", addr, len,
              _data_band_threshold);
        co_return -EINVAL;
    }
    co_return co_await _leaf->async_iov(q, data, iovecs, nr_vecs, leaf_addr).start();
}

io_result MdDisk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    auto const len = iovec_len(iovecs, iovecs + nr_vecs);
    auto const uaddr = static_cast< uint64_t >(addr);
    off_t leaf_addr;
    if (uaddr + len <= _data_band_threshold) {
        leaf_addr = static_cast< off_t >(uaddr + k_head_zone_shift);
    } else if (uaddr >= _data_band_threshold) {
        leaf_addr = static_cast< off_t >(uaddr + _data_band_shift);
    } else {
        DLOGE("MdDisk: sync_iov straddles head-zone boundary [addr={:#x}, len={:#x}, threshold={:#x}]", uaddr, len,
              _data_band_threshold);
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    return _leaf->sync_iov(op, iovecs, nr_vecs, leaf_addr);
}

// ---------------------------------------------------------------------------------------------
// Factories
// ---------------------------------------------------------------------------------------------

namespace {

// Validate two MdDisks (a, b) form a mirror pair. For md raid 10 (near=2): pair is roles
// 2K and 2K+1 for some K. For md raid 1: pair is roles 0 and 1 (2-way mirror).
// `a` MUST have lower dev_role than `b`; caller is responsible for ordering.
void __validate_pair_topology(DiscoveredTopology const& a, DiscoveredTopology const& b) {
    if (a.set_uuid != b.set_uuid)
        throw std::runtime_error(fmt::format("MdImport: set_uuid mismatch ({} vs {})",
                                             boost::uuids::to_string(a.set_uuid), boost::uuids::to_string(b.set_uuid)));
    if (a.md_level != b.md_level)
        throw std::runtime_error(fmt::format("MdImport: md level mismatch ({} vs {})", a.md_level, b.md_level));
    if (a.md_chunk_size != b.md_chunk_size)
        throw std::runtime_error(
            fmt::format("MdImport: md_chunk_size mismatch ({} vs {})", a.md_chunk_size, b.md_chunk_size));
    if (a.data_offset_bytes != b.data_offset_bytes)
        throw std::runtime_error(
            fmt::format("MdImport: data_offset mismatch ({} vs {})", a.data_offset_bytes, b.data_offset_bytes));
    if (a.raid_disks != b.raid_disks)
        throw std::runtime_error(fmt::format("MdImport: raid_disks mismatch ({} vs {})", a.raid_disks, b.raid_disks));
    if (a.layout_near != b.layout_near || a.layout_far != b.layout_far || a.layout_far_offset != b.layout_far_offset)
        throw std::runtime_error("MdImport: layout fields differ between legs");
    if (a.events != b.events)
        throw std::runtime_error(fmt::format(
            "MdImport: events counter mismatch ({} vs {}); array was not cleanly stopped", a.events, b.events));
    // a.dev_role < b.dev_role enforced by caller; verify pair index match. For md raid 10
    // pair K is roles {2K, 2K+1}; for md raid 1 the only pair is {0, 1}. Both check the same
    // way: roles differ by 1 and share the same pair index (role/2).
    if ((a.dev_role / 2) != (b.dev_role / 2) || a.dev_role + 1 != b.dev_role)
        throw std::runtime_error(
            fmt::format("MdImport: dev_roles {} and {} are not a mirror pair", a.dev_role, b.dev_role));
}

} // namespace

disk_handle make_md_raid1_disk(boost::uuids::uuid const& uuid, std::pair< disk_handle, disk_handle > legs,
                               std::string const& parent_id) {
    auto md_a = std::make_shared< MdDisk >(std::move(legs.first), uuid);
    auto md_b = std::make_shared< MdDisk >(std::move(legs.second), uuid);
    auto topo_a = md_a->topology();
    auto topo_b = md_b->topology();

    // Order so that the leg with the smaller dev_role is "a".
    if (topo_a.dev_role > topo_b.dev_role) {
        std::swap(md_a, md_b);
        std::swap(topo_a, topo_b);
    }
    __validate_pair_topology(topo_a, topo_b);

    return make_raid1_disk(uuid, std::move(md_a), std::move(md_b), parent_id);
}

disk_handle make_md_raid10_disk(boost::uuids::uuid const& uuid, std::vector< disk_handle >&& legs,
                                std::string const& parent_id) {
    if (legs.size() < 4 || (legs.size() & 1U) != 0)
        throw std::runtime_error(fmt::format("MdRaid10 import: legs.size()={} (require even, >= 4)", legs.size()));

    // Wrap each leg in MdDisk; each MdDisk verifies the caller's uuid against its md /
    // wrapper SB. A leg from another array will throw here.
    std::vector< std::shared_ptr< MdDisk > > wrapped;
    wrapped.reserve(legs.size());
    for (auto& leg : legs)
        wrapped.emplace_back(std::make_shared< MdDisk >(std::move(leg), uuid));
    legs.clear();

    // Reference topology = first leg's. All others must match (other than dev_role).
    auto const& ref = wrapped.front()->topology();
    if (ref.md_level != 10)
        throw std::runtime_error(fmt::format(
            "MdRaid10 import: source array is md level {}, not 10 (use make_md_raid1_disk for level 1)", ref.md_level));
    if (ref.raid_disks != wrapped.size())
        throw std::runtime_error(
            fmt::format("MdRaid10 import: md raid_disks={} but {} legs were provided", ref.raid_disks, wrapped.size()));
    for (size_t i = 1; i < wrapped.size(); ++i) {
        auto const& t = wrapped[i]->topology();
        if (t.set_uuid != ref.set_uuid || t.md_level != ref.md_level || t.md_chunk_size != ref.md_chunk_size ||
            t.data_offset_bytes != ref.data_offset_bytes || t.raid_disks != ref.raid_disks ||
            t.layout_near != ref.layout_near || t.layout_far != ref.layout_far ||
            t.layout_far_offset != ref.layout_far_offset || t.events != ref.events)
            throw std::runtime_error(fmt::format("MdRaid10 import: leg {} topology disagrees with leg 0", i));
    }

    // Group legs by dev_role / 2 into pairs. Each pair must contain roles 2K and 2K+1.
    auto const num_pairs = static_cast< size_t >(ref.raid_disks / 2);
    std::map< uint16_t, std::pair< std::shared_ptr< MdDisk >, std::shared_ptr< MdDisk > > > pairs;
    for (auto& md : wrapped) {
        auto const role = md->topology().dev_role;
        auto const pair_k = static_cast< uint16_t >(role / 2);
        auto& slot = pairs[pair_k];
        if ((role & 1U) == 0) {
            if (slot.first) throw std::runtime_error(fmt::format("MdRaid10 import: duplicate role {}", role));
            slot.first = std::move(md);
        } else {
            if (slot.second) throw std::runtime_error(fmt::format("MdRaid10 import: duplicate role {}", role));
            slot.second = std::move(md);
        }
    }
    if (pairs.size() != num_pairs)
        throw std::runtime_error(
            fmt::format("MdRaid10 import: found {} pair(s), expected {}", pairs.size(), num_pairs));

    // Build the RAID1 mirrors in ascending pair-K order so md's chunk-to-pair mapping
    // (kernel uses chunk_index % (raid_disks / 2)) is preserved byte-identical. The pair
    // UUIDs are derived from the caller-supplied volume uuid (== md set_uuid after MdDisk
    // verification), which is what the analogous fresh-create raid10 would have produced.
    auto name_gen = boost::uuids::name_generator(uuid);
    std::vector< disk_handle > mirrors;
    mirrors.reserve(num_pairs);
    for (uint16_t k = 0; k < num_pairs; ++k) {
        auto it = pairs.find(k);
        if (it == pairs.end() || !it->second.first || !it->second.second)
            throw std::runtime_error(fmt::format("MdRaid10 import: missing legs for pair index {}", k));
        auto pair_uuid = name_gen(fmt::format("partition_{}", k));
        mirrors.emplace_back(
            make_raid1_disk(pair_uuid, std::move(it->second.first), std::move(it->second.second), parent_id));
    }

    // raid0 SB carries the volume uuid; verified on every reattach.
    return make_raid0_disk(uuid, ref.md_chunk_size, std::move(mirrors));
}

} // namespace ublkpp::md
