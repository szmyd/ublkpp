#include "ublkpp/raid.hpp"

#include <optional>
#include <set>

#include <boost/uuid/uuid_io.hpp>
#include <ublksrv.h>
#include <ublksrv_utils.h>
#include <sisl/options/options.h>

#include "bitmap.hpp"
#include "raid1_impl.hpp"
#include "raid1_resync_task.hpp"
#include "lib/logging.hpp"
#include "target/ublkpp_tgt_impl.hpp"
#include "metrics/ublk_raid_metrics.hpp"

SISL_OPTION_GROUP(raid1,
                  (chunk_size, "", "chunk_size", "The desired chunk_size for new Raid1 devices",
                   cxxopts::value< std::uint32_t >()->default_value("32768"), "<io_size>"),
                  (resync_level, "", "resync_level", "Resync prioritization level (1-32)",
                   cxxopts::value< std::uint32_t >()->default_value("4"), "<io_size>"),
                  (resync_delay, "", "resync_delay", "Delay between I/O and Resync context switches",
                   cxxopts::value< std::uint32_t >()->default_value("300"), "<microseconds> (us)"),
                  (avail_delay, "", "avail_delay", "Seconds between idle device availability probes",
                   cxxopts::value< std::uint32_t >()->default_value("5"), "<seconds>"))

namespace ublkpp {

namespace raid1 {

// Min page-resolution (how much does the smallest page cover?)
constexpr auto k_min_page_depth = k_min_chunk_size * k_page_size * k_bits_in_byte; // 1GiB from above
// > 1: new_device detection threshold in __load_and_select_superblock. 16: matches the bumps used
// in __swap_device and the existing new_device / unclean-degraded paths so all sites are comparable.
constexpr uint64_t k_age_bump = 16;

// Max user-data size
constexpr uint64_t k_max_user_data =
    (unsigned __int128)(k_min_page_depth - k_page_size) * (UINT64_MAX - sizeof(SuperBlock)) / k_min_page_depth;

MirrorDevice::MirrorDevice(boost::uuids::uuid const& uuid, std::shared_ptr< ublk_disk > device) :
        disk(std::move(device)) {
    auto chunk_size = SISL_OPTIONS["chunk_size"].as< uint32_t >();
    if (k_min_chunk_size > chunk_size) {
        RLOGE("Invalid chunk_size: {}KiB [min:{}KiB]", chunk_size / Ki, k_min_chunk_size / Ki) // LCOV_EXCL_START
        throw std::runtime_error("Invalid Chunk Size");
    } // LCOV_EXCL_STOP
    if (0 == SISL_OPTIONS["resync_level"].as< uint32_t >()) {
        RLOGE("Invalid resync_level: 0 [min:1] — use 1-32")
        throw std::runtime_error("resync_level must be at least 1");
    }

    // It is not a failure to be able to load the superblock from a missing-leg placeholder
    if (disk->is_missing()) return;

    auto read_super = load_superblock(*disk, uuid, chunk_size);
    if (!read_super) {
        throw std::runtime_error(fmt::format("Could not read superblock! {}", read_super.error().message()));
    } else {
        new_device = read_super.value().second;
        sb = std::shared_ptr< SuperBlock >(read_super.value().first, [](void* x) { free(x); });
    }
}

// Route state capture
//
// The destructor is marked no_sanitize_thread in debug builds to balance the hidden
// shared_ptr refcount increments performed by __capture_route_state() (also no_sanitize_thread).
// Without this, TSAN sees N visible decrements without corresponding visible increments, causing
// its shadow counter to diverge and falsely report heap-use-after-free on the last release.
struct RouteState {
    std::shared_ptr< MirrorDevice > active_dev;
    std::shared_ptr< MirrorDevice > backup_dev;
    raid1::read_route route;
    bool is_degraded;

#ifndef NDEBUG
    __attribute__((no_sanitize_thread, no_sanitize("address"))) ~RouteState() {
        active_dev.reset();
        backup_dev.reset();
    }
#endif
};

Raid1Disk::Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< ublk_disk > dev_a,
                     std::shared_ptr< ublk_disk > dev_b, std::string const& parent_id) :
        ublk_disk(), _uuid(uuid), _str_uuid(boost::uuids::to_string(uuid)) {
    // At least one device has to be "real"
    if (dev_a->is_missing() && dev_b->is_missing())
        throw std::runtime_error("Can not run with both devices missing"); // LCOV_EXCL_LINE

    // Create metrics with parent_id for correlation
    if (!parent_id.empty()) _raid_metrics = std::make_unique< UblkRaidMetrics >(parent_id, _str_uuid);

    // Load devices and select best superblock first so __init_params can read _sb->header.version.
    __load_and_select_superblock(uuid, std::move(dev_a), std::move(dev_b), parent_id);

    // Discover parameters and calculate reserved space (uses _device_a/_device_b/_sb).
    __init_params();

    // Initialize bitmap and handle initial degradation based on route determination
    __init_bitmap_and_degraded_route();

    // Initialize resync_task; slot_count = 2×qdepth so the tracker can hold every
    // in-flight write at peak depth. Fall back to 256 (= 2×128 default) when the
    // ublkpp_tgt option group is not loaded (unit test context).
    uint32_t const resync_slots = SISL_OPTIONS.count("qdepth") ? 2u * SISL_OPTIONS["qdepth"].as< uint16_t >() : 256u;
    _resync_task = std::make_shared< Raid1ResyncTask >(_dirty_bitmap, _reserved_size, block_size(),
                                                       params()->basic.max_sectors << SECTOR_SHIFT, resync_slots,
                                                       be32toh(_sb->fields.bitmap.chunk_size), _raid_metrics);

    // Write the up-to-date superblocks and mark devices as in use
    __become_active();
}

void Raid1Disk::__init_params() {
    DEBUG_ASSERT(_sb, "__init_params called before __load_and_select_superblock populated _sb")
    RLOGI("Initializing RAID-1 [uuid:{}] from devices {} and {}", _str_uuid, _device_a->disk, _device_b->disk)

    _direct_io = true; // RAID-1 prefers DIO; downgraded below if any member doesn't support it

    // Discover overall Device parameters
    auto& our_params = *params();
    our_params.types |= UBLK_PARAM_TYPE_DISCARD;
    our_params.basic.io_opt_shift = ilog2(k_min_chunk_size);

    // Set largest underlying user-data size we support as starting point
    our_params.basic.dev_sectors = k_max_user_data >> SECTOR_SHIFT;

    // Now find the what size we should actually set based on the smallest provided device
    for (auto device_array = std::set< std::shared_ptr< ublk_disk > >{_device_a->disk, _device_b->disk};
         auto const& device : device_array) {
        if (!device->direct_io()) {
            RLOGW("Device {} does not support O_DIRECT - RAID-1 will use buffered I/O (backend caching not bypassed!)",
                  device)
            _direct_io = false; // LCOV_EXCL_LINE
        }
        our_params.basic.dev_sectors =
            std::min< uint64_t >(our_params.basic.dev_sectors, device->capacity() >> SECTOR_SHIFT);
        our_params.basic.logical_bs_shift =
            std::max(our_params.basic.logical_bs_shift, static_cast< uint8_t >(ilog2(device->block_size())));
        our_params.basic.physical_bs_shift =
            std::max(our_params.basic.physical_bs_shift, static_cast< uint8_t >(ilog2(device->physical_block_size())));

        if (!device->can_discard()) our_params.types &= ~UBLK_PARAM_TYPE_DISCARD;
    }

    auto const sb_version = be16toh(_sb->header.version);
    if (sb_version < 2) {
        // v1: capacity-proportional reserved region — reconstruct the exact on-disk layout.
        auto const bitmap_size = ((our_params.basic.dev_sectors << SECTOR_SHIFT) / k_min_chunk_size) / k_bits_in_byte;
        _reserved_size = sizeof(SuperBlock) + bitmap_size;
    } else {
        // v2+: fixed maximum, leaves headroom for future resize.
        _reserved_size = sizeof(SuperBlock) + (k_superbitmap_bits * k_page_size);
    }

    // Pad _reserved_size for alignment. Policy depends on SB version:
    //  v1: pad to max_sectors_bytes (preserves legacy on-disk layout exactly).
    //  v2+: pad to logical_bs only — sufficient for O_DIRECT, frees the ~511 KiB tail loss v1 imposed.
    auto const align = (sb_version >= 2) ? (static_cast< uint64_t >(1) << our_params.basic.logical_bs_shift)
                                         : (static_cast< uint64_t >(our_params.basic.max_sectors) << SECTOR_SHIFT);
    _reserved_size += ((our_params.basic.dev_sectors << SECTOR_SHIFT) - _reserved_size) % align;

    // L4: verify the array is large enough to hold the reserved region.
    // dev_sectors already holds the minimum of both physical devices, so one check suffices —
    // the larger device is always >= the minimum and passes trivially. Without this guard a
    // too-small device silently underflows dev_sectors and later triggers a confusing
    // "exceeds SuperBitmap max capacity" error from the Bitmap constructor.
    auto const min_dev_bytes = our_params.basic.dev_sectors << SECTOR_SHIFT;
    if (min_dev_bytes < _reserved_size) {
        RLOGE("Devices are too small: {} bytes < reserved {} bytes [uuid:{}]", min_dev_bytes, _reserved_size, _str_uuid)
        throw std::runtime_error(
            fmt::format("device too small: {} bytes < reserved {} bytes", min_dev_bytes, _reserved_size));
    }

    // Reserve space for the superblock/bitmap
    our_params.basic.dev_sectors -= (_reserved_size >> SECTOR_SHIFT);

    if (can_discard())
        our_params.discard.discard_granularity = std::max(our_params.discard.discard_granularity, block_size());
}

void Raid1Disk::__load_and_select_superblock(boost::uuids::uuid const& uuid, std::shared_ptr< ublk_disk > dev_a,
                                             std::shared_ptr< ublk_disk > dev_b, std::string const& parent_id) {
    // Load SuperBlocks to determine original layout
    _device_a = std::make_shared< MirrorDevice >(uuid, std::move(dev_a));
    if (_device_a->new_device) {
        _device_b = std::make_shared< MirrorDevice >(uuid, std::move(dev_b));
        if (!_device_b->new_device && !_device_b->sb->fields.device_b) _device_a.swap(_device_b);
    } else {
        _device_b = std::make_shared< MirrorDevice >(uuid, std::move(dev_b));
        if (!_device_b->new_device && (_device_a->sb->fields.device_b == _device_b->sb->fields.device_b))
            throw std::runtime_error("Found both devices were assigned the same slot!");
        if (_device_a->sb->fields.device_b) _device_a.swap(_device_b);
    }

    // We only keep the latest or if match and A unclean take B, if age diff is > 1 consider new
    if (auto sb_res = pick_superblock(_device_a->sb.get(), _device_b->sb.get()); sb_res) {
        if (sb_res == _device_a->sb.get()) {
            _sb = std::move(_device_a->sb);
            if (!_device_b->sb || 1 < (be64toh(_sb->fields.bitmap.age) - be64toh(_device_b->sb->fields.bitmap.age)))
                _device_b->new_device = true;
        } else {
            _sb = std::move(_device_b->sb);
            if (!_device_a->sb || 1 < (be64toh(_sb->fields.bitmap.age) - be64toh(_device_a->sb->fields.bitmap.age)))
                _device_a->new_device = true;
        }
    } else {
        RLOGE("Could read SuperBlocks from any device, aborting assembly of: {} [parent_id: {}]", _str_uuid, parent_id)
        throw std::runtime_error("Could not find reasonable superblock!"); // LCOV_EXCL_LINE
    }

    // Initialize read_route cache from loaded superblock
    _read_route_cache.store(static_cast< read_route >(_sb->fields.read_route), std::memory_order_release);

    // Initialize Age if New
    if (_device_a->new_device && _device_b->new_device) _sb->fields.bitmap.age = htobe64(1);
}

void Raid1Disk::__init_bitmap_and_degraded_route() {
    // Read in existing dirty BITMAP pages
    _dirty_bitmap = std::make_shared< Bitmap >(capacity(), be32toh(_sb->fields.bitmap.chunk_size), block_size(),
                                               _sb->superbitmap_reserved, _str_uuid);
    // Initialize bitmap pages for any new (or defunct) device slots
    if (_device_a->new_device) _dirty_bitmap->init_to(_device_a->disk);
    if (_device_b->new_device) _dirty_bitmap->init_to(_device_b->disk);

    // Use physical slot references (_device_a/_device_b) directly to avoid the ambiguity of
    // role-relative state captured by __capture_route_state(). The read_route enum refers to
    // physical slots (DEVA=_device_a, DEVB=_device_b), so mapping must be slot-based.
    if (_device_a->disk->is_missing() || _device_b->disk->is_missing()) {
        RLOGW("RAID1 device [uuid:{}] is running with a missing device!", _str_uuid)
        bool const a_is_missing = _device_a->disk->is_missing();
        // Route reads to whichever physical slot is live
        _read_route_cache.store(a_is_missing ? read_route::DEVB : read_route::DEVA, std::memory_order_release);
        // If already degraded and crashed (clean_unmount=0), the superbitmap cannot be trusted —
        // dirty all regions to force a full resync. For any other combination (clean shutdown, or
        // previously healthy with unclean shutdown), the superbitmap is trustworthy: load it and
        // let load_from skip pages that are already clean.
        if (!_sb->fields.clean_unmount && static_cast< read_route >(_sb->fields.read_route) != read_route::EITHER) {
            _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 16);
            RLOGW("Unclean shutdown while degraded with missing device! Dirty all of BITMAP")
            _dirty_bitmap->dirty_region(0, capacity());
        } else {
            _dirty_bitmap->load_from(*(a_is_missing ? _device_b : _device_a)->disk);
        }
    } else if (_device_a->new_device xor _device_b->new_device) {
        // Bump the bitmap age
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + k_age_bump);
        RLOGW("Device is replacement {}, dirty all of BITMAP",
              *(_device_a->new_device ? _device_a->disk : _device_b->disk))
        _dirty_bitmap->dirty_region(0, capacity());
        // Route reads to the existing (non-new) physical slot
        _read_route_cache.store(_device_a->new_device ? read_route::DEVB : read_route::DEVA, std::memory_order_release);
    } else if ((read_route::EITHER != _read_route_cache.load(std::memory_order_acquire)) &&
               (0 == _sb->fields.clean_unmount)) {
        // Bump the bitmap age
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + k_age_bump);
        RLOGW("Unclean shutdown in degraded mode! Dirty all of BITMAP")
        _dirty_bitmap->dirty_region(0, capacity());
    } else if (auto const route = _read_route_cache.load(std::memory_order_acquire); read_route::EITHER != route) {
        auto const& active_dev = (route == read_route::DEVB) ? _device_b : _device_a;
        auto const& backup_dev = (route == read_route::DEVB) ? _device_a : _device_b;
        RLOGW("Raid1 is starting in degraded mode [uuid:{}]! Degraded device: {}", _str_uuid, *backup_dev->disk)
        // clean_unmount=1 implied; superbitmap normally non-empty after degraded+stop.
        // If empty, Fix 1 in _start() (complete() on STOPPING) should have prevented this.
        if (!_dirty_bitmap->superbitmap_nonempty())
            RLOGW("Degraded + clean unmount + empty superbitmap [uuid:{}]", _str_uuid)
        _dirty_bitmap->load_from(*active_dev->disk);
    } else if (0 == _sb->fields.clean_unmount) {
        // Both-present unclean: reads may diverge across legs. Pin to device_a (canonical),
        // dirty all, mark device_b stale. __become_active skips device_b's SB (unavail guard)
        // to preserve the >1 age gap for idempotent crash-mid-resync reassembly.
        DEBUG_ASSERT(_read_route_cache.load(std::memory_order_relaxed) == read_route::EITHER,
                     "self-heal branch reached with non-EITHER route")
        // The XOR branch above catches exactly one-new-device; if both new_device flags are set
        // (both SBs corrupt or absent) the XOR is false and we fall through here. That scenario
        // is not both-present-unclean: skip self-heal and let the caller handle the fresh array.
        if (_device_a->new_device || _device_b->new_device) return;
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + k_age_bump);
        _dirty_bitmap->dirty_region(0, capacity());
        _read_route_cache.store(read_route::DEVA, std::memory_order_release);
        _device_b->unavail.test_and_set(std::memory_order_release);
        RLOGW("Unclean shutdown with both legs present [uuid:{}] -- reads pinned to {} (canonical), "
              "full resync to {} scheduled to restore read-determinism",
              _str_uuid, *_device_a->disk, *_device_b->disk)
    }
}

void Raid1Disk::__become_active() {
    // Mark the devices as ACTIVE and write the updated superblocks
    auto const state = __capture_route_state();
    _sb->fields.clean_unmount = 0x0;
    _sb->fields.device_b = 0; // Reset this in case we loaded from dev_b
    if (!write_superblock(*state.active_dev->disk, _sb.get(), read_route::DEVB == state.route, state.route)) {
        // If already degraded this is Fatal
        if (state.is_degraded) { throw std::runtime_error(fmt::format("Could not initialize superblocks!")); }
        if (!__become_degraded(true, &state, false)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
        return;
    }
    if (state.backup_dev->disk->is_missing()) return;
    // Preserve on-disk age gap for crash-mid-resync idempotency (see __init_bitmap_and_degraded_route).
    if (state.backup_dev->unavail.test(std::memory_order_acquire)) {
        // Both the SB write and its __become_degraded fallback are intentionally skipped: the array
        // is already in the correct degraded state from __init_bitmap_and_degraded_route, and the
        // stale SB must not be updated so the on-disk age gap is preserved. probe_mirror cannot
        // clear unavail before this point because the resync task only starts after construction
        // completes and the first queue thread registers.
        RLOGW("Skipping backup SB write: device_b marked stale at startup [uuid:{}]", _str_uuid)
        return;
    }
    if (!write_superblock(*state.backup_dev->disk, _sb.get(), read_route::DEVB != state.route, state.route)) {
        if (!__become_degraded(false, &state, false)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
    }
}

Raid1Disk::~Raid1Disk() {
    RLOGD("Shutting down; [uuid:{}]", _str_uuid)
    _resync_task->stop();

    if (!_sb) return;

    auto const state = __capture_route_state();
    // Write out our dirty bitmap to the active device.
    // M11: flush even when backup_dev is a missing placeholder — the active device still needs
    // the current bitmap so the next startup can do an incremental resync rather than a full one.
    if (state.is_degraded) {
        RLOGI("Synchronizing BITMAP [uuid: {}] to clean device: {}", _str_uuid, *state.active_dev->disk)
        if (auto res = _dirty_bitmap->sync_to(*state.active_dev->disk, sizeof(SuperBlock)); !res) {
            RLOGW("Could not sync Bitmap to device on shutdown, will require full resync next time! [uuid:{}]",
                  _str_uuid)
            return;
        }
        RLOGI("Synchronized: [uuid: {}]", _str_uuid)
    }
    _sb->fields.clean_unmount = 0x1;
    // Only update the superblock to clean devices. Pass include_superbitmap=true so the
    // on-disk superbitmap reflects the current dirty state. On next startup the call sites
    // for load_from check superbitmap_nonempty() and reject the volume if it is empty.
    if (auto res =
            write_superblock(*state.active_dev->disk, _sb.get(), read_route::DEVB == state.route, state.route, true);
        !res) {
        if (state.is_degraded) {
            RLOGE("Failed to clear clean bit...full sync required upon next assembly [uuid:{}]", _str_uuid)
        }
    }
    if (!state.is_degraded)
        std::ignore =
            write_superblock(*state.backup_dev->disk, _sb.get(), read_route::DEVB != state.route, state.route, true);
}

Raid1Disk::prepare_result Raid1Disk::prepare(ublksrv_queue const* q, int const iouring_device_start) {
    // Called once per queue thread before I/O begins; count queues for multi-queue idle tracking.
    // When q is null (called from init_tgt purely for the SQE ceiling), skip the side effects:
    // no FDs to collect and no queue-count increment.
    auto result = _device_a->disk->prepare(q, iouring_device_start);
    auto b = _device_b->disk->prepare(q, iouring_device_start + static_cast< int >(result.fds.size()));
    result.fds.insert(result.fds.end(), b.fds.begin(), b.fds.end());
    // Writes fan out to both mirrors concurrently; both SQE sets land in the same pool simultaneously.
    // Failover reads are sequential (max of the two), but write is the worst case.
    result.max_sqes_per_io += b.max_sqes_per_io;

    // Enable resync only on the first real queue init (q != nullptr guards the probe-only call).
    if (q && _nr_hw_queues.fetch_add(1, std::memory_order_acq_rel) == 0) toggle_resync(true);

    return result;
}

// ── Mutual exclusion between __swap_device and __become_degraded ─────────────────────────────────
// Both functions advance the RAID1 state machine by atomically CAS-ing _read_route_cache.
// Because compare_exchange_strong() is atomic, exactly one caller wins:
//
//   • __swap_device    CAS: EITHER → DEVA/DEVB  (replacing a device with a healthy one)
//   • __become_degraded CAS: EITHER → DEVA/DEVB  (marking a failed device as unavailable)
//
// If both race, the loser sees the CAS fail and returns early - no further state is mutated.
// No additional lock is needed between them; the CAS IS the synchronization gate.
//
// __swap_device also holds _ctrl_lock so that two concurrent swap_device() callers don't race
// on the _device_a/_device_b pointer mutations.  __become_degraded does not need _ctrl_lock for
// those pointer reads (it accesses them via a captured RouteState snapshot), but it does acquire
// _ctrl_lock for the bitmap.age increment to avoid a data race with __swap_device's +16 bump.
// ─────────────────────────────────────────────────────────────────────────────────────────────────
//
// The order of the _read_route_cache CAS, swap(), and unavail.clear() below must be preserved to keep
// the read-retry loop in __capture_route_state() correct.
bool Raid1Disk::__swap_device(std::string const& outgoing_device_id, std::shared_ptr< MirrorDevice >& incoming_mirror,
                              raid1::read_route const& cur_route) {
    auto lg = std::scoped_lock< std::mutex >(_ctrl_lock);

    bool const swapping_device_a = (_device_a->disk->id() == outgoing_device_id);
    auto new_read_route = swapping_device_a ? read_route::DEVB : read_route::DEVA;

    auto orig_route = cur_route;
    if (!_read_route_cache.compare_exchange_strong(orig_route, new_read_route)) return false;

    auto old_age = be64toh(_sb->fields.bitmap.age);
    auto new_age = old_age + k_age_bump;

    auto& outgoing_dev = swapping_device_a ? _device_a : _device_b;
    outgoing_dev.swap(incoming_mirror);
    _sb->fields.bitmap.age = htobe64(new_age);

    // Write superblock to staying device first (critical path)
    auto& staying_dev = swapping_device_a ? _device_b : _device_a;
    if (auto sync_res = write_superblock(*staying_dev->disk, _sb.get(), swapping_device_a, new_read_route); !sync_res) {
        RLOGE("Could not advance Age [uuid:{}]: {}", _str_uuid, sync_res.error().message())
        // Rollback
        _sb->fields.bitmap.age = htobe64(old_age);
        outgoing_dev.swap(incoming_mirror);
        _read_route_cache.compare_exchange_strong(new_read_route, cur_route);
        return false;
    }
    // Commit SuperBlock to new device; if this fails it's not fatal per say...could work
    // later when we become clean; so let's be optimistic!
    std::ignore = write_superblock(*outgoing_dev->disk, _sb.get(), !swapping_device_a,
                                   _read_route_cache.load(std::memory_order_acquire));

    // Dirty entire bitmap if this is a new device
    if (outgoing_dev->new_device) _dirty_bitmap->dirty_region(0, capacity());
    // Open up for Large WRITES and RESYNC
    outgoing_dev->unavail.clear(std::memory_order_release);
    return true;
}

// ##########################################!! WARNING !!##########################################
// One should not directly access _device_a, _device_b or _read_route directly following this point.
// It is subject to multi-threading in that case and subject to hotswap or degradation altering
// these values between your memory loads. Use the following method to access this in a consistent
// manner that is race-free and use it across the co-routine frame
// ##########################################!! WARNING !!##########################################

// ═══════════════════════════════════════════════════════════════════════════════
// ☠️ ☠️ ☠️  DANGER ZONE: LOCK-FREE SYNCHRONIZATION - DO NOT MODIFY  ☠️ ☠️ ☠️
// ═══════════════════════════════════════════════════════════════════════════════
//
// This function implements a lock-free read-retry pattern that is INTENTIONALLY
// a data race by C++ standard but SAFE (on x86-64) in practice through application-level
// synchronization.
//
// See lock-free-read-retry.txt for additional details
//
// TSAN FLAGS THIS: Yes, correctly. Suppressed via tsan.supp + no_sanitize_thread.
// ASAN FLAGS THIS: Yes, correctly. Suppressed via no_sanitize("address") on the definition.
//
// ⚠️  DO NOT MODIFY UNLESS YOU FULLY UNDERSTAND LOCK-FREE MEMORY MODELS ⚠️
//
// ═══════════════════════════════════════════════════════════════════════════════
// clang-format off
#ifndef NDEBUG
__attribute__((noinline, no_sanitize_thread, no_sanitize("address")))
#else
__attribute__((noinline))
#endif
RouteState Raid1Disk::__capture_route_state() const {
    while (true) {
        auto a = _device_a;
        auto b = _device_b;
        auto const route = _read_route_cache.load(std::memory_order_acquire);
        if (_device_a != a || _device_b != b) continue;

        return RouteState{.active_dev = (read_route::DEVB == route) ? std::move(b) : std::move(a),
                          .backup_dev = (read_route::DEVB == route) ? std::move(a) : std::move(b),
                          .route = route,
                          .is_degraded = (read_route::EITHER != route)};
    }
}
// clang-format on

// Helper: Decode logical route to physical device from captured state.
// Maps DEVA/DEVB to the actual device currently in that physical slot,
// accounting for swaps that may have changed active/backup mapping.
static inline std::shared_ptr< MirrorDevice > const& __route_to_device(RouteState const& state,
                                                                       raid1::read_route logical_route) noexcept {
    bool const is_slot_a = (read_route::DEVA == logical_route);
    bool const use_active = is_slot_a ? (state.route != read_route::DEVB) : (state.route == read_route::DEVB);
    return use_active ? state.active_dev : state.backup_dev;
}

// RAID1 devices have the property of being replacable while maintaining
// consistency due to the fact that the data is replicated. Swapping a device
// may occur for a multitude of _reasons_ but the following RULES apply when
// attemtping to service a given swap_request. We will refer to the *new* device
// being added to the array as the _incoming_ device and the one being replaced
// as the _outgoing_ device.
//
// * _incoming_ device must support DirectI/O w/o volatile caching
// * _incoming_ device must be >= the (capacity() + _reserved_size) of the Array
// * _incoming_ device lbs must be <= the lbs of the Array (k_page_size)
// * _Outgoing_ device is part of a Clean array *OR* the dirty device in a
//   Degraded arrary
//
// Once these checks have been made we are free to replace the _Outgoing_
// device. We first attempt to load the _incoming_ device in case it has
// already been part of this or another Array. The following decision is made
// if:
//
// * Device is not a RAID1 disk: Treat as *new*
// * Device is part of another RAID1 disk: Abort the operation
// * Device was part of this this Array:
//   - If the age is within `1` treat as a FD replacement for _Outgoing_ disk
//     and continue with exisiting Bitmap.
//   - Otherwise we assume the Bitmap is out-of-sync and revert to treating the
//     disk as *new*.
//
// Anytime we determine that a replacement disk is *new* we must dirty _every_
// bit in the Bitmap and do a FULL resync.
std::shared_ptr< ublk_disk > Raid1Disk::swap_device(std::string const& outgoing_device_id,
                                                    std::shared_ptr< ublk_disk > incoming_device) {
    if (!incoming_device->direct_io()) {
        RLOGW("Replacement device {} does not support O_DIRECT - RAID-1 will use buffered I/O (backend caching not "
              "bypassed!)",
              incoming_device)
    }
    auto& our_params = *params();
    auto const incoming_sectors = incoming_device->capacity() >> SECTOR_SHIFT;
    auto const incoming_lbs_shift = static_cast< uint8_t >(ilog2(incoming_device->block_size()));
    if ((our_params.basic.dev_sectors + (_reserved_size >> SECTOR_SHIFT)) > incoming_sectors ||
        (our_params.basic.logical_bs_shift < incoming_lbs_shift)) {
        RLOGE("Refusing to use device, requires: [lbs<={} && cap>={}Ki]!", 1 << our_params.basic.logical_bs_shift,
              (our_params.basic.dev_sectors << SECTOR_SHIFT) / Ki)
        return incoming_device;
    }

    auto const state = __capture_route_state();
    // We check if the outgoing device is actually part of this array first,
    // then we ensure that the incoming device is actually a different device
    // from what we already have. If either is not true, do nothing.
    if ((state.active_dev->disk->id() != outgoing_device_id) && (state.backup_dev->disk->id() != outgoing_device_id)) {
        RLOGE("Refusing to replace unrecognized mirror!")
        return incoming_device;
    } else if ((state.active_dev->disk->id() == incoming_device->id()) ||
               (state.backup_dev->disk->id() == incoming_device->id())) {
        RLOGI("No replacements discovered! {} already in array, nothing to do...", *incoming_device)
        return incoming_device;
    }

    // If we're degraded; check that we're swapping out the degraded device
    if (state.is_degraded && state.active_dev->disk->id() == outgoing_device_id) {
        RLOGE("Refusing to replace working mirror from degraded device!")
        return incoming_device;
    }

    // Initialize incoming mirror BEFORE stopping resync (exception-safe)
    std::shared_ptr< MirrorDevice > incoming_mirror;
    try {
        incoming_mirror = std::make_shared< MirrorDevice >(_uuid, incoming_device);
        if (!incoming_mirror->sb ||
            (be64toh(incoming_mirror->sb->fields.bitmap.age) + 1 < be64toh(_sb->fields.bitmap.age))) {
            RLOGD("Age read: {} Current: {}", be64toh(incoming_mirror->sb->fields.bitmap.age),
                  be64toh(_sb->fields.bitmap.age))
            incoming_mirror->new_device = true;
        }
        // Do not read or write here yet
        incoming_mirror->unavail.test_and_set(std::memory_order_acq_rel);
    } catch (std::runtime_error const& e) { return incoming_device; }

    // Stop resync before touching the bitmap so the resync task doesn't race set_bit/clear_bit
    // against our init_to() below. Write I/Os can still call set_bit() concurrently; clear_all()
    // uses atomic byte stores to avoid UB with those.
    auto old_resync_flag = _resync_enabled.load(std::memory_order_relaxed);
    toggle_resync(false);

    try {
        // TODO we need to save the SuperBitmap Here!
        if (!incoming_mirror->disk->is_missing() && incoming_mirror->new_device)
            _dirty_bitmap->init_to(incoming_mirror->disk);
    } catch (std::runtime_error const&) {
        toggle_resync(old_resync_flag);
        return incoming_device;
    }

    // Atomically swap the device or fail; fail if swapping sole active device
    if (__swap_device(outgoing_device_id, incoming_mirror, state.route)) {
        if (_raid_metrics) _raid_metrics->record_device_swap(); // GCOVR_EXCL_BR_LINE
    }

    // Now set back to IDLE state and kick a resync task off
    if (old_resync_flag) toggle_resync(true);

    // incoming_mirror now holds the outgoing device (or incoming if failed)
    return incoming_mirror->disk;
}

raid1::array_state Raid1Disk::replica_states() const noexcept {
    auto const sz_to_sync = _dirty_bitmap->dirty_data_est();
    auto const state = __capture_route_state();

    // Helper: compute state for a device
    auto const get_state = [&state](MirrorDevice const* dev, bool is_active, uint64_t sync_bytes) -> replica_state {
        bool const is_unavail = dev->unavail.test(std::memory_order_acquire);

        if (!is_active && (sync_bytes > 0 || state.route != read_route::EITHER)) {
            // Device is degraded (doesn't have all data or route not yet restored)
            return is_unavail ? replica_state::ERROR : replica_state::SYNCING;
        }

        if (is_unavail && state.route == read_route::EITHER) {
            // Healthy array, but device has read failures
            return replica_state::UNAVAIL; // Has data, can't reach it
        }

        return replica_state::CLEAN;
    };

    switch (state.route) {
    case read_route::DEVA: // Device B is write-degraded
        return {.device_a = get_state(state.active_dev.get(), true, sz_to_sync),
                .device_b = get_state(state.backup_dev.get(), false, sz_to_sync),
                .bytes_to_sync = sz_to_sync};
    case read_route::DEVB: // Device A is write-degraded
        return {.device_a = get_state(state.backup_dev.get(), false, sz_to_sync),
                .device_b = get_state(state.active_dev.get(), true, sz_to_sync),
                .bytes_to_sync = sz_to_sync};
    case read_route::EITHER: // Healthy array
    default:
        // For EITHER route: active_dev==device_a, backup_dev==device_b by convention
        return {.device_a = get_state(state.active_dev.get(), true, 0),
                .device_b = get_state(state.backup_dev.get(), true, 0),
                .bytes_to_sync = 0};
    }
}

std::pair< std::shared_ptr< ublk_disk >, std::shared_ptr< ublk_disk > > Raid1Disk::replicas() const noexcept {
    auto const state = __capture_route_state();
    // Return devices in their logical positions (A, B) based on current route
    // When route == DEVA: active is A, backup is B
    // When route == DEVB: active is B, backup is A
    // When route == EITHER: active is A, backup is B (clean state)
    if (state.route == read_route::DEVB) {
        return std::make_pair(state.backup_dev->disk, state.active_dev->disk);
    } else {
        return std::make_pair(state.active_dev->disk, state.backup_dev->disk);
    }
}

// Returns true if the array successfully transitioned to EITHER (clean superblocks written),
// or if another concurrent path already won the EITHER CAS (idempotent).
// Returns false in three cases that require the caller to keep resyncing:
//   (a) Site-2 dirty_region() set bits before the lock was acquired — dirty_pages() > 0 under
//       the lock; route stays degraded and the CAS is not attempted.
//   (b) __swap_device raced and changed the route before our CAS — old_route != EITHER.
//   (c) post-write: __become_degraded fired during superblock I/O — H1 re-writes with a fresh
//       route+device capture (coherent across swap races) and returns false.
// Calling toggle_resync() from here would be a no-op — the task is still ACTIVE.
bool Raid1Disk::__become_clean() {
    auto const state = __capture_route_state();
    if (read_route::EITHER == state.route) return true; // already clean

    RLOGI("Device becoming clean [{}] [uuid:{}] ", *state.backup_dev->disk, _str_uuid)

    // Determine which device is device_b based on route:
    // - When route == DEVA: active_dev is A (is_device_b=false), backup_dev is B (is_device_b=true)
    // - When route == DEVB: active_dev is B (is_device_b=true), backup_dev is A (is_device_b=false)
    bool const active_is_device_b = (state.route == read_route::DEVB);

    // _clean_transition_mutex is held across check + CAS + both superblock writes.
    //
    // The lock serializes this path against Sites 1 & 3 (backup_write=true failures),
    // which also hold the lock during dirty_region() + __become_degraded(). Two crash cases:
    //   - Before failure site acquires lock: only EITHER SBs on disk, no dirty bit set
    //     (dirty_region is inside the lock) → system is genuinely clean.
    //   - After failure site's DEVA SB write: working_dev=DEVA(age+1), other=EITHER →
    //     pick_superblock selects by age → DEVA route → resync → safe.
    //
    // Residual crash window (not closed by the mutex): process crashes after Sites 1/3
    // acquire the lock and call dirty_region() but before __become_degraded() completes its
    // write_superblock() I/O. In that window, dirty bits are in-memory only (lost on crash)
    // and both SBs say EITHER at the same age — no resync is triggered on restart. Closing
    // this window requires additional on-disk metadata (a "last-active slot" field) to
    // disambiguate source-of-truth at startup; tracked as a follow-up.
    //
    // No co_await under the lock; write_superblock is synchronous. The lock is cold-path
    // (only acquired on resync completion and on write-leg failures).
    {
        std::lock_guard lock(_clean_transition_mutex);
        if (_dirty_bitmap->dirty_pages() > 0) return false; // bits set under lock → stay degraded

        auto old_route = state.route;
        if (!_read_route_cache.compare_exchange_strong(old_route, read_route::EITHER))
            // - old_route == EITHER: another __become_clean already won → done.
            // - old_route != EITHER: __swap_device raced, route changed → loop __run() to re-sync.
            return old_route == read_route::EITHER;

        // Bitmap is empty and route is EITHER — write clean superblocks under the lock so
        // the failure-path DEVA write (also under this lock) always serializes after them.
        if (auto sync_res =
                write_superblock(*state.active_dev->disk, _sb.get(), active_is_device_b, read_route::EITHER);
            !sync_res) {
            RLOGW("Could not become clean [uuid:{}]: {}", _str_uuid, sync_res.error().message())
        }
        if (!state.backup_dev->disk->is_missing()) {
            if (auto sync_res =
                    write_superblock(*state.backup_dev->disk, _sb.get(), !active_is_device_b, read_route::EITHER);
                !sync_res) {
                RLOGW("Could not become clean [uuid:{}]: {}", _str_uuid, sync_res.error().message())
            }
        }
    } // lock released; both EITHER SBs are on disk

    // H1 defense-in-depth: if a failure path moved route away from EITHER after our lock
    // released (e.g. __swap_device, or a Site-2 __become_degraded without the mutex), re-write
    // the on-disk SBs with the current degraded route. A fresh capture is used so device
    // pointers are coherent with live_route even if __swap_device raced and remapped the slots.
    auto const live_state = __capture_route_state();
    if (live_state.route != read_route::EITHER) {
        bool const live_active_is_b = (live_state.route == read_route::DEVB);
        if (auto sync_res =
                write_superblock(*live_state.active_dev->disk, _sb.get(), live_active_is_b, live_state.route);
            !sync_res) {
            RLOGW("Could not re-write degraded superblock after race [uuid:{}]: {}", _str_uuid,
                  sync_res.error().message())
        }
        if (!live_state.backup_dev->disk->is_missing()) {
            if (auto sync_res =
                    write_superblock(*live_state.backup_dev->disk, _sb.get(), !live_active_is_b, live_state.route);
                !sync_res) {
                RLOGW("Could not re-write degraded superblock after race [uuid:{}]: {}", _str_uuid,
                      sync_res.error().message())
            }
        }
        return false; // caller loops to re-sync the dirty region
    }
    return true;
}

// Called from __become_degraded when the array is already in the target degraded state.
// Retries any pending SB write and, on success, optionally spawns resync.
//
// Concurrent retry safety: _sb->fields is stable here — the original __become_degraded
// returned before toggle_resync(), so _degraded_sb_pending is only set when no resync task
// is running, and __become_clean cannot race without one. Two coroutines may both pass the
// load check before either clears the flag; the resulting concurrent write_superblock calls
// write identical data to the same offset and are therefore idempotent.
//
// cur_state may predate this degradation (e.g. a Site-1 retry passes the original EITHER-mode
// snapshot), so active_dev may point to the failed leg. Re-capture the route state to guarantee
// we write to the surviving device.
bool Raid1Disk::__try_persist_degraded_sb(bool spawn_resync) {
    if (_degraded_sb_pending.load(std::memory_order_acquire)) {
        auto const rs = __capture_route_state();
        bool const is_b = (rs.route == read_route::DEVB);
        if (auto sb = write_superblock(*rs.active_dev->disk, _sb.get(), is_b, rs.route); sb) {
            _degraded_sb_pending.store(false, std::memory_order_release);
            RLOGW("Persisted degraded superblock on retry (pending cleared) [uuid:{}]", _str_uuid)
            if (spawn_resync && _resync_enabled.load(std::memory_order_relaxed)) toggle_resync(true);
        } else {
            RLOGE("SB persist retry failed [uuid:{}]: {}", _str_uuid, sb.error().message())
            return false;
        }
    }
    return true;
}

// See the comment above __swap_device for the CAS-based mutual exclusion between this function
// and __swap_device.  The _read_route_cache CAS below is the synchronization gate: if
// __swap_device wins the CAS first, this call sees old_route != EITHER and returns early (already
// degraded or concurrent swap in progress).  No additional lock is required.
bool Raid1Disk::__become_degraded(bool failed_is_active, RouteState const* cur_state, bool spawn_resync) {
    // Surviving device is backup if active failed, active if backup failed.
    // new_route = the physical slot (DEVA/DEVB) of the surviving device.
    bool const active_is_b = (cur_state->route == read_route::DEVB);
    auto old_route = read_route::EITHER;
    auto const new_route = (failed_is_active == active_is_b) ? read_route::DEVA : read_route::DEVB;
    if (!_read_route_cache.compare_exchange_strong(old_route, new_route)) {
        // CAS lost — either already degraded (no-op) or __swap_device raced in.
        if (old_route == new_route) return __try_persist_degraded_sb(spawn_resync);
        return false; // __swap_device won the CAS
    }

    auto const backup_clean = (read_route::DEVB == new_route);
    auto& failed_device = failed_is_active ? cur_state->active_dev : cur_state->backup_dev;
    auto& working_device = failed_is_active ? *cur_state->backup_dev->disk : *cur_state->active_dev->disk;

    // _ctrl_lock guards _sb->fields against concurrent __swap_device mutations (same field).
    {
        std::lock_guard lock(_ctrl_lock);
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 1);
    }
    RLOGW("Device became degraded {} [age:{}] [uuid:{}]", *failed_device->disk,
          static_cast< uint64_t >(be64toh(_sb->fields.bitmap.age)), _str_uuid);

    // Record degradation event in metrics with device name
    if (_raid_metrics) { // GCOVR_EXCL_BR_LINE -- UblkRaidMetrics requires prometheus registry; not constructible in
                         // unit tests
        // LCOV_EXCL_START
        auto device_name = (new_route == read_route::DEVA) ? "device_b" : "device_a";
        _raid_metrics->record_device_degraded(device_name);
    } // LCOV_EXCL_STOP

    // Must update age first; we do this synchronously to gate pending retry results
    if (auto sync_res = write_superblock(working_device, _sb.get(), backup_clean, new_route); !sync_res) {
        // SB write failed -- we cannot persist the degradation, but rolling back to EITHER would
        // allow round-robin reads to the failed device, serving inconsistent data (the backup may
        // have already received the write). Keep the in-memory degraded route and mark the failed
        // device unavailable.
        //
        // The age increment is NOT reverted: any subsequent SB write must carry a higher age than
        // the stale on-disk SB so pick_superblock selects the surviving device on restart.
        // _degraded_sb_pending signals that the SB write is still outstanding; the next I/O
        // that reaches the already-degraded path will retry it before acking.
        _degraded_sb_pending.store(true, std::memory_order_release);
        failed_device->unavail.test_and_set(std::memory_order_acq_rel);
        RLOGE("Could not persist degradation [uuid:{}]: {}", _str_uuid, sync_res.error().message())
        return false;
    }
    failed_device->unavail.test_and_set(std::memory_order_acq_rel);
    if (spawn_resync && _resync_enabled.load(std::memory_order_relaxed)) toggle_resync(true); // Launch a Resync Task
    return true;
}

disk_task< int > Raid1Disk::__failover_read_async(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                                  uint32_t nr_vecs, uint64_t addr, uint32_t len) {
    // Snapshot before first co_await: in RAID10, iovecs points into RAID0's thread_local
    // sub_cmds. Once we suspend, concurrent I/Os on the same queue thread can overwrite
    // sub_cmds, making iovecs stale when the failover SQE is submitted.
    // nr_vecs is bounded by StripeAccum::io_array (16 entries) or 1 from ublkpp_tgt.
    // std::array avoids an additional heap allocation; the coroutine frame is already heap-allocated.
    DEBUG_ASSERT_LE(nr_vecs, 16u)
    std::array< iovec, 16 > iov_snap{};
    std::copy_n(iovecs, nr_vecs, iov_snap.begin());
    iovecs = iov_snap.data();

    auto const state = __capture_route_state();
    auto devices = __select_read_devices(state, addr, len);
    auto& primary_dev = devices.first;
    auto& failover_dev = devices.second;

    auto primary_task = primary_dev->disk->async_iov(q, data, iovecs, nr_vecs, addr + _reserved_size).start();
    auto const r = co_await primary_task;

    if (r >= 0) {
        primary_dev->unavail.clear(std::memory_order_release);
        co_return r;
    }
    if (!state.is_degraded && !primary_dev->unavail.test_and_set(std::memory_order_acq_rel))
        RLOGW("Device marked unavailable due to read failure: {}", *primary_dev->disk)

    if (!failover_dev) co_return -EAGAIN;

    auto failover_task = (*failover_dev)->disk->async_iov(q, data, iovecs, nr_vecs, addr + _reserved_size).start();
    co_return co_await failover_task;
}

std::pair< std::shared_ptr< MirrorDevice >, std::optional< std::shared_ptr< MirrorDevice > > >
Raid1Disk::__select_read_devices(RouteState const& state, uint64_t addr, uint32_t len) const noexcept {
    thread_local raid1::read_route last_read = raid1::read_route::DEVB;

    auto route = read_route::DEVA;
    if (state.is_degraded && state.backup_dev->unavail.test(std::memory_order_acquire)) {
        route = state.route;
    } else {
        route = (last_read == read_route::DEVB) ? read_route::DEVA : read_route::DEVB;
    }
    // In degraded mode, any dirty region means the backup holds stale data -- regardless of
    // which device was chosen as primary. Redirect to active if needed, and suppress failover.
    bool backup_stale = false;
    if (state.is_degraded && _dirty_bitmap->is_dirty(addr, len)) {
        if (route != state.route) route = state.route;
        backup_stale = true;
    }
    if (!state.is_degraded && __route_to_device(state, route)->unavail.test(std::memory_order_acquire)) {
        route = (route == read_route::DEVA) ? read_route::DEVB : read_route::DEVA;
        RLOGD("Skipping unavail device, routing to alternate")
    }

    last_read = route;
    auto const other_route = (route == read_route::DEVA) ? read_route::DEVB : read_route::DEVA;
    return {__route_to_device(state, route),
            backup_stale ? std::nullopt : std::optional{__route_to_device(state, other_route)}};
}

bool Raid1Disk::__backup_writable(RouteState const& state, uint64_t addr, uint32_t len) const noexcept {
    return !(state.is_degraded &&
             (state.backup_dev->unavail.test(std::memory_order_acquire) || _dirty_bitmap->is_dirty(addr, len)));
}

disk_task< int > Raid1Disk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                                      uint64_t addr) {
    auto const op = ublksrv_get_op(data->iod);
    auto const len = static_cast< uint32_t >(iovec_len(iovecs, iovecs + nr_vecs));

    if (op == UBLK_IO_OP_FLUSH) co_return 0;

    if (op != UBLK_IO_OP_READ && op != UBLK_IO_OP_WRITE && op != UBLK_IO_OP_DISCARD && op != UBLK_IO_OP_WRITE_ZEROES)
        co_return -EINVAL;

    RLOGT("Received {}: [tag:{:#0x}] [lba:{:#0x}|len:{:#0x}] [uuid:{}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE",
          data->tag, addr >> params()->basic.logical_bs_shift, len, _str_uuid)

    if (op == UBLK_IO_OP_READ) co_return co_await __failover_read_async(q, data, iovecs, nr_vecs, addr, len);

    // Write / Discard / WriteZeroes: replicate to both devices
    auto const state = __capture_route_state();

    // Register this write's LBA range in the region tracker so resync skips only the
    // conflicting chunk rather than pausing globally.
    auto _guard = raid1::ResyncWriteGuard{*_resync_task, addr, len};
    auto const backup_write = __backup_writable(state, addr, len);

    auto const adj_addr = addr + _reserved_size;
    auto active_task = state.active_dev->disk->async_iov(q, data, iovecs, nr_vecs, adj_addr).start();

    std::optional< hot_task< int > > backup_task;
    if (backup_write)
        backup_task.emplace(state.backup_dev->disk->async_iov(q, data, iovecs, nr_vecs, adj_addr).start());

    auto const active_res = co_await active_task;

    if (active_res < 0) {
        // Site 1: active fails with backup_write==true — newly dirties a clean region.
        // Lock covers only dirty_region + __become_degraded; co_awaits happen after release.
        bool const become_degraded_ok = [&] {
            std::lock_guard lock(_clean_transition_mutex);
            _dirty_bitmap->dirty_region(addr, len);
            return __become_degraded(true, &state);
        }();
        // CAS lost and no backup to drain — nothing to await.
        if (!become_degraded_ok && !backup_task) co_return -EAGAIN;
        // Either __become_degraded succeeded (backup guaranteed by invariant) or failed with a
        // backup present. Always drain backup before returning: leaving it in-flight while the
        // coroutine exits is unsafe.
        DEBUG_ASSERT(!become_degraded_ok || backup_task.has_value(),
                     "backup_task must exist when become_degraded succeeds"); // LCOV_EXCL_BR_LINE
        auto const backup_res = co_await *backup_task;
        if (!become_degraded_ok) {
            // Backup write landed but SB wasn't persisted. Attempt one retry before returning
            // EAGAIN — avoids an unnecessary client round-trip for a transient SB write failure.
            // __become_degraded sees the already-degraded route and fires the pending-retry path.
            if (!__become_degraded(true, &state)) co_return -EAGAIN;
        }
        co_return backup_res >= 0 ? backup_res
                                  : -EAGAIN; // -EAGAIN: degradation is durable; retry routes to sole device
    }

    if (state.active_dev->unavail.test(std::memory_order_relaxed)) {
        RLOGI("Device {} back online (write succeeded) [uuid:{}]", *state.active_dev->disk, _str_uuid)
        state.active_dev->unavail.clear(std::memory_order_release);
    }

    if (!backup_write) {
        _dirty_bitmap->dirty_region(addr, len);
        // Site 2: no mutex — this path fires when backup is unavail or the region was already
        // dirty. The mutex was designed to exclude Sites 1 and 3 (which newly dirty a clean
        // region and need to be atomic with __become_clean). Here we still call __become_degraded
        // unconditionally so that if __become_clean released the mutex and transitioned to EITHER
        // before this dirty_region call, we re-degrade before ACKing. __become_degraded's own
        // CAS handles idempotency; if already degraded with a pending SB write, it retries the
        // SB write before returning — ensuring the SB is durable before we ack.
        if (!__become_degraded(false, &state)) co_return -EAGAIN;
        co_return active_res;
    }

    auto const backup_res = co_await *backup_task;

    if (backup_res < 0) {
        // Site 3: backup fails with backup_write==true — newly dirties a clean region.
        // Hold _clean_transition_mutex so dirty_region + become_degraded are atomic against
        // __become_clean's check+CAS. No co_await under the lock.
        std::lock_guard lock(_clean_transition_mutex);
        _dirty_bitmap->dirty_region(addr, len);
        if (auto d = __become_degraded(false, &state); !d) co_return -EAGAIN;
    } else if (state.backup_dev->unavail.test(std::memory_order_relaxed)) {
        RLOGI("Device {} back online (write succeeded) [uuid:{}]", *state.backup_dev->disk, _str_uuid)
        state.backup_dev->unavail.clear(std::memory_order_release);
    }

    co_return active_res;
}

io_result Raid1Disk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    auto const len = static_cast< uint32_t >(iovec_len(iovecs, iovecs + nr_vecs));
    [[maybe_unused]] auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("Received {}: [lba:{:#0x}|len:{:#0x}] [uuid:{}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", lba, len,
          _str_uuid)

    auto const state = __capture_route_state();
    auto const adj_addr = addr + static_cast< off_t >(_reserved_size);

    if (UBLK_IO_OP_READ == op) {
        auto const [primary_dev, failover_dev] = __select_read_devices(state, static_cast< uint64_t >(addr), len);
        auto const primary_res = primary_dev->disk->sync_iov(UBLK_IO_OP_READ, iovecs, nr_vecs, adj_addr);
        if (primary_res) {
            primary_dev->unavail.clear(std::memory_order_release);
            return primary_res;
        }
        if (!state.is_degraded && !primary_dev->unavail.test_and_set(std::memory_order_acq_rel))
            RLOGW("Device marked unavailable due to read failure: {}", *primary_dev->disk)

        if (!failover_dev) return std::unexpected(std::make_error_condition(std::errc::resource_unavailable_try_again));
        return (*failover_dev)->disk->sync_iov(UBLK_IO_OP_READ, iovecs, nr_vecs, adj_addr);
    }

    // WRITE / DISCARD / WRITE_ZEROES: flat replication -- mirrors async_iov write path.

    // Register this write's LBA range in the region tracker so resync skips only the
    // conflicting chunk rather than pausing globally.
    auto _guard = raid1::ResyncWriteGuard{*_resync_task, static_cast< uint64_t >(addr), len};
    auto const backup_write = __backup_writable(state, static_cast< uint64_t >(addr), len);

    auto const active_res = state.active_dev->disk->sync_iov(op, iovecs, nr_vecs, adj_addr);

    if (!active_res) {
        std::lock_guard lock(_clean_transition_mutex); // site 1 (sync)
        _dirty_bitmap->dirty_region(static_cast< uint64_t >(addr), len);
        if (auto d = __become_degraded(true, &state); !d)
            return std::unexpected(std::make_error_condition(std::errc::resource_unavailable_try_again));
        // become_degraded succeeded → state was EITHER → bm was WRITE → backup is reachable.
        DEBUG_ASSERT(backup_write,
                     "backup_write must be true when become_degraded succeeds"); // LCOV_EXCL_BR_LINE
        auto const backup_res = state.backup_dev->disk->sync_iov(op, iovecs, nr_vecs, adj_addr);
        return backup_res ? backup_res
                          : std::unexpected(std::make_error_condition(std::errc::resource_unavailable_try_again));
    }

    if (state.active_dev->unavail.test(std::memory_order_relaxed)) {
        RLOGI("Device {} back online (write succeeded) [uuid:{}]", *state.active_dev->disk, _str_uuid)
        state.active_dev->unavail.clear(std::memory_order_release);
    }

    if (!backup_write) {
        _dirty_bitmap->dirty_region(static_cast< uint64_t >(addr), len);
        // Site 2 (sync) — mirrors async_iov Site 2; see that comment.
        if (!__become_degraded(false, &state))
            return std::unexpected(std::make_error_condition(std::errc::resource_unavailable_try_again));
        return active_res;
    }

    auto const backup_res = state.backup_dev->disk->sync_iov(op, iovecs, nr_vecs, adj_addr);

    if (!backup_res) {
        std::lock_guard lock(_clean_transition_mutex); // site 3 (sync)
        _dirty_bitmap->dirty_region(static_cast< uint64_t >(addr), len);
        if (auto d = __become_degraded(false, &state); !d)
            return std::unexpected(std::make_error_condition(std::errc::resource_unavailable_try_again));
    } else if (state.backup_dev->unavail.test(std::memory_order_relaxed)) {
        RLOGI("Device {} back online (write succeeded) [uuid:{}]", *state.backup_dev->disk, _str_uuid)
        state.backup_dev->unavail.clear(std::memory_order_release);
    }

    return active_res;
}

void Raid1Disk::probe_tick(ublksrv_queue const*) noexcept {
    auto const state = __capture_route_state();
    if (state.is_degraded) return; // resync task handles probing in degraded mode

    if (!Raid1ResyncTask::probe_mirror(*state.active_dev, _reserved_size))
        RLOGD("Idle probe: device unavailable: {}", *state.active_dev->disk)
    if (!Raid1ResyncTask::probe_mirror(*state.backup_dev, _reserved_size))
        RLOGD("Idle probe: device unavailable: {}", *state.backup_dev->disk)
}

void Raid1Disk::toggle_resync(bool t) {
    _resync_enabled.store(t, std::memory_order_relaxed);
    if (t) {
        auto const state = __capture_route_state();
        if (read_route::EITHER != state.route && !state.backup_dev->disk->is_missing()) {
            _resync_task->launch(_str_uuid, state.active_dev, state.backup_dev, [this] { return __become_clean(); });
        }
    } else
        _resync_task->stop();
}

namespace {
// Public free functions dispatch through the impl class via dynamic_cast; non-Raid1 disks return
// safe defaults so callers can stay generic across composite topologies.
inline Raid1Disk* as_raid1(ublk_disk& d) noexcept { return dynamic_cast< Raid1Disk* >(&d); }
inline Raid1Disk const* as_raid1(ublk_disk const& d) noexcept { return dynamic_cast< Raid1Disk const* >(&d); }
} // namespace

std::shared_ptr< ublk_disk > swap_device(ublk_disk& disk, std::string const& old_device_id,
                                         std::shared_ptr< ublk_disk > new_device) {
    auto* r1 = as_raid1(disk);
    if (!r1) {
        RLOGE("swap_device called on non-Raid1 disk: {}", *new_device);
        return new_device;
    }
    return r1->swap_device(old_device_id, std::move(new_device));
}

array_state replica_states(ublk_disk const& disk) noexcept {
    auto const* r1 = as_raid1(disk);
    if (!r1) {
        RLOGW("replica_states called on non-Raid1 disk: {}", disk);
        return array_state{};
    }
    return r1->replica_states();
}

std::pair< std::shared_ptr< ublk_disk >, std::shared_ptr< ublk_disk > > replicas(ublk_disk const& disk) noexcept {
    auto const* r1 = as_raid1(disk);
    if (!r1) {
        RLOGW("replicas called on non-Raid1 disk: {}", disk);
        return {nullptr, nullptr};
    }
    return r1->replicas();
}

} // namespace raid1

std::shared_ptr< ublk_disk > make_raid1_disk(boost::uuids::uuid const& uuid, std::shared_ptr< ublk_disk > dev_a,
                                             std::shared_ptr< ublk_disk > dev_b, std::string const& parent_id) {
    return std::make_shared< raid1::Raid1Disk >(uuid, std::move(dev_a), std::move(dev_b), parent_id);
}

} // namespace ublkpp
