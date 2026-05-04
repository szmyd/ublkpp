#include "ublkpp/raid.hpp"

#include <optional>
#include <set>

#include <boost/uuid/uuid_io.hpp>
#include <ublksrv.h>
#include <ublksrv_utils.h>
#include <sisl/options/options.h>

#include "bitmap.hpp"
#include "raid1_avail_probe.hpp"
#include "raid1_impl.hpp"
#include "raid1_resync_task.hpp"
#include "lib/logging.hpp"
#include "target/ublkpp_tgt_impl.hpp"
#include "metrics/ublk_raid_metrics.hpp"

SISL_OPTION_GROUP(raid1,
                  (chunk_size, "", "chunk_size", "The desired chunk_size for new Raid1 devices",
                   cxxopts::value< std::uint32_t >()->default_value("32768"), "<io_size>"),
                  (resync_level, "", "resync_level", "Resync prioritization level (0-32)",
                   cxxopts::value< std::uint32_t >()->default_value("4"), "<io_size>"),
                  (avail_delay, "", "avail_delay", "Delay between checking if a degraded device is available again",
                   cxxopts::value< std::uint32_t >()->default_value("5"), "<seconds>"),
                  (resync_delay, "", "resync_delay", "Delay between I/O and Resync context switches",
                   cxxopts::value< std::uint32_t >()->default_value("300"), "<microseconds> (us)"))

namespace ublkpp {

namespace raid1 {

// Min page-resolution (how much does the smallest page cover?)
constexpr auto k_min_page_depth = k_min_chunk_size * k_page_size * k_bits_in_byte; // 1GiB from above

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
struct RouteState {
    std::shared_ptr< MirrorDevice > active_dev;
    std::shared_ptr< MirrorDevice > backup_dev;
    raid1::read_route route;
    bool is_degraded;
};

Raid1Disk::Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< ublk_disk > dev_a,
                     std::shared_ptr< ublk_disk > dev_b, std::string const& parent_id) :
        ublk_disk(), _uuid(uuid), _str_uuid(boost::uuids::to_string(uuid)) {
    // At least one device has to be "real"
    if (dev_a->is_missing() && dev_b->is_missing())
        throw std::runtime_error("Can not run with both devices missing"); // LCOV_EXCL_LINE

    // Create metrics with parent_id for correlation
    if (!parent_id.empty()) _raid_metrics = std::make_unique< UblkRaidMetrics >(parent_id, _str_uuid);

    // Discover parameters and calculate reserved space
    __init_params(dev_a, dev_b);

    // Load devices, select best superblock, initialize route
    __load_and_select_superblock(uuid, std::move(dev_a), std::move(dev_b), parent_id);

    // Initialize bitmap and handle initial degradation based on route determination
    __init_bitmap_and_degraded_route();

    // Initialize resync_task
    _resync_task = std::make_shared< Raid1ResyncTask >(_dirty_bitmap, _reserved_size, block_size(),
                                                       params()->basic.max_sectors << SECTOR_SHIFT, _raid_metrics);

    // Write the up-to-date superblocks and mark devices as in use
    __become_active();
}

void Raid1Disk::__init_params(std::shared_ptr< ublk_disk > const& dev_a, std::shared_ptr< ublk_disk > const& dev_b) {
    RLOGI("Initializing RAID-1 [uuid:{}] from devices {} and {}", _str_uuid, dev_a, dev_b)

    _direct_io = true; // RAID-1 prefers DIO; downgraded below if any member doesn't support it

    // Discover overall Device parameters
    auto& our_params = *params();
    our_params.types |= UBLK_PARAM_TYPE_DISCARD;
    our_params.basic.io_opt_shift = ilog2(k_min_chunk_size);

    // Set largest underlying user-data size we support as starting point
    our_params.basic.dev_sectors = k_max_user_data >> SECTOR_SHIFT;

    // Now find the what size we should actually set based on the smallest provided device
    for (auto device_array = std::set< std::shared_ptr< ublk_disk > >{dev_a, dev_b};
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

    auto const bitmap_size = ((our_params.basic.dev_sectors << SECTOR_SHIFT) / k_min_chunk_size) / k_bits_in_byte;
    _reserved_size = sizeof(SuperBlock) + bitmap_size;

    // Align user-data to max_sector size
    _reserved_size += ((our_params.basic.dev_sectors << SECTOR_SHIFT) - _reserved_size) %
        (our_params.basic.max_sectors << SECTOR_SHIFT);

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
        // Load bitmap from the live slot; don't bump age - we may get the original disk back via swap
        _dirty_bitmap->load_from(*(a_is_missing ? _device_b : _device_a)->disk);
    } else if (_device_a->new_device xor _device_b->new_device) {
        // Bump the bitmap age
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 16);
        RLOGW("Device is replacement {}, dirty all of BITMAP",
              *(_device_a->new_device ? _device_a->disk : _device_b->disk))
        _dirty_bitmap->dirty_region(0, capacity());
        // Route reads to the existing (non-new) physical slot
        _read_route_cache.store(_device_a->new_device ? read_route::DEVB : read_route::DEVA, std::memory_order_release);
    } else if ((read_route::EITHER != _read_route_cache.load(std::memory_order_acquire)) &&
               (0 == _sb->fields.clean_unmount)) {
        // Bump the bitmap age
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 16);
        RLOGW("Unclean shutdown in degraded mode! Dirty all of BITMAP")
        _dirty_bitmap->dirty_region(0, capacity());
    } else if (auto const route = _read_route_cache.load(std::memory_order_acquire); read_route::EITHER != route) {
        auto const& active_dev = (route == read_route::DEVB) ? _device_b : _device_a;
        auto const& backup_dev = (route == read_route::DEVB) ? _device_a : _device_b;
        RLOGW("Raid1 is starting in degraded mode [uuid:{}]! Degraded device: {}", _str_uuid, *backup_dev->disk)
        _dirty_bitmap->load_from(*active_dev->disk);
    } else if (0 == _sb->fields.clean_unmount) {
        RLOGW("Raid1 was not cleanly shutdown last time [uuid:{}]!", _str_uuid)
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
    // Write out our dirty bitmap
    if (state.is_degraded && !state.backup_dev->disk->is_missing()) {
        RLOGI("Synchronizing BITMAP [uuid: {}] to clean device: {}", _str_uuid, *state.active_dev->disk)
        if (auto res = _dirty_bitmap->sync_to(*state.active_dev->disk, sizeof(SuperBlock)); !res) {
            RLOGW("Could not sync Bitmap to device on shutdown, will require full resync next time! [uuid:{}]",
                  _str_uuid)
            return;
        }
        RLOGI("Synchronized: [uuid: {}]", _str_uuid)
    }
    _sb->fields.clean_unmount = 0x1;
    // Only update the superblock to clean devices
    if (auto res = write_superblock(*state.active_dev->disk, _sb.get(), read_route::DEVB == state.route, state.route);
        !res) {
        if (state.is_degraded) {
            RLOGE("Failed to clear clean bit...full sync required upon next assembly [uuid:{}]", _str_uuid)
        }
    }
    if (!state.is_degraded)
        write_superblock(*state.backup_dev->disk, _sb.get(), read_route::DEVB != state.route, state.route);
}

std::vector< int > Raid1Disk::prepare(ublksrv_queue const* q, int const iouring_device_start) {
    // Called once per queue thread before I/O begins; count queues for multi-queue idle tracking.
    // Always collect FDs from child disks - each queue thread may need its own set.
    auto fds = _device_a->disk->prepare(q, iouring_device_start);
    auto b_fds = _device_b->disk->prepare(q, iouring_device_start + fds.size());
    fds.insert(fds.end(), b_fds.begin(), b_fds.end());

    // Enable resync only on the first call (first queue thread).
    if (_nr_hw_queues.fetch_add(1, std::memory_order_acq_rel) == 0) toggle_resync(true);

    return fds;
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
// on the _device_a/_device_b pointer mutations.  __become_degraded does NOT need _ctrl_lock
// because it only reads those pointers via a captured RouteState snapshot and never mutates them.
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
    auto new_age = old_age + 16;

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
    write_superblock(*outgoing_dev->disk, _sb.get(), !swapping_device_a,
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
        incoming_mirror->unavail.test_and_set(std::memory_order_acquire);
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
        // Stop stale probes - they hold a shared_ptr to the outgoing MirrorDevice.
        // _idle_probe_lock guards _probe members against concurrent launch() in idle_transition.
        auto lk = std::unique_lock{_idle_probe_lock};
        _idle_probe_a.stop();
        _idle_probe_b.stop();
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

io_result Raid1Disk::__become_clean() {
    auto const state = __capture_route_state();
    if (read_route::EITHER == state.route) return 0;

    RLOGI("Device becoming clean [{}] [uuid:{}] ", *state.backup_dev->disk, _str_uuid)

    // Write the new SuperBlock with updated clean read_route
    // Determine which device is device_b based on route:
    // - When route == DEVA: active_dev is A (is_device_b=false), backup_dev is B (is_device_b=true)
    // - When route == DEVB: active_dev is B (is_device_b=true), backup_dev is A (is_device_b=false)
    bool const active_is_device_b = (state.route == read_route::DEVB);

    if (auto sync_res = write_superblock(*state.active_dev->disk, _sb.get(), active_is_device_b, state.route);
        !sync_res) {
        RLOGW("Could not become clean [uuid:{}]: {}", _str_uuid, sync_res.error().message())
    }
    if (auto sync_res = write_superblock(*state.backup_dev->disk, _sb.get(), !active_is_device_b, state.route);
        !sync_res) {
        RLOGW("Could not become clean [uuid:{}]: {}", _str_uuid, sync_res.error().message())
    }

    // Avoid checking DirtyBitmap going forward on reads/writes
    auto old_route = state.route;
    _read_route_cache.compare_exchange_strong(old_route, read_route::EITHER);
    return 0;
}

// See the comment above __swap_device for the CAS-based mutual exclusion between this function
// and __swap_device.  The _read_route_cache CAS below is the synchronization gate: if
// __swap_device wins the CAS first, this call sees old_route != EITHER and returns early (already
// degraded or concurrent swap in progress).  No additional lock is required.
io_result Raid1Disk::__become_degraded(bool failed_is_active, RouteState const* cur_state, bool spawn_resync) {
    // Surviving device is backup if active failed, active if backup failed.
    // new_route = the physical slot (DEVA/DEVB) of the surviving device.
    bool const active_is_b = (cur_state->route == read_route::DEVB);
    auto old_route = read_route::EITHER;
    auto const new_route = (failed_is_active == active_is_b) ? read_route::DEVA : read_route::DEVB;
    if (!_read_route_cache.compare_exchange_strong(old_route, new_route)) {
        if (old_route == new_route) return 0; // Already degraded
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }

    auto const backup_clean = (read_route::DEVB == new_route);
    auto& failed_device = failed_is_active ? cur_state->active_dev : cur_state->backup_dev;
    auto& working_device = failed_is_active ? *cur_state->backup_dev->disk : *cur_state->active_dev->disk;

    auto const old_age = _sb->fields.bitmap.age;
    _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 1);
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
        // device unavailable. The dirty bitmap covers the affected region; resync at shutdown or a
        // full recovery on next start will reconcile any inconsistency.
        _sb->fields.bitmap.age = old_age; // revert age -- not written to disk
        failed_device->unavail.test_and_set(std::memory_order_acquire);
        RLOGE("Could not persist degradation [uuid:{}]: {}", _str_uuid, sync_res.error().message())
        return sync_res;
    }
    failed_device->unavail.test_and_set(std::memory_order_acquire);
    if (spawn_resync && _resync_enabled.load(std::memory_order_relaxed)) toggle_resync(true); // Launch a Resync Task
    return 0;
}

disk_task< int > Raid1Disk::__failover_read_async(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                                  uint32_t nr_vecs, uint64_t addr, uint32_t len) {
    thread_local raid1::read_route last_read = raid1::read_route::DEVB;
    auto const state = __capture_route_state();

    // Pick device (load balancer)
    auto route = read_route::DEVA;
    if (state.is_degraded && state.backup_dev->unavail.test(std::memory_order_acquire)) {
        route = state.route;
    } else {
        route = (last_read == read_route::DEVB) ? read_route::DEVA : read_route::DEVB;
    }

    // In degraded mode, avoid reading backup if the region is dirty
    if (state.is_degraded && route != state.route && _dirty_bitmap->is_dirty(addr, len)) route = state.route;

    // In healthy mode, avoid unavail devices
    if (!state.is_degraded && __route_to_device(state, route)->unavail.test(std::memory_order_acquire)) {
        route = (route == read_route::DEVA) ? read_route::DEVB : read_route::DEVA;
        RLOGD("Skipping unavail device, routing to alternate")
    }

    last_read = route;
    auto const& primary_dev = __route_to_device(state, route);

    auto primary_task = primary_dev->disk->async_iov(q, data, iovecs, nr_vecs, addr + _reserved_size).start();
    auto const r = co_await primary_task;

    if (r >= 0) {
        primary_dev->unavail.clear(std::memory_order_release);
        co_return r;
    }
    if (!primary_dev->unavail.test_and_set(std::memory_order_acquire))
        RLOGW("Device marked unavailable due to read failure: {}", *primary_dev->disk)

    // Failover to the other device
    auto const other_route = (route == read_route::DEVA) ? read_route::DEVB : read_route::DEVA;
    auto const& failover_dev = __route_to_device(state, other_route);

    auto failover_task = failover_dev->disk->async_iov(q, data, iovecs, nr_vecs, addr + _reserved_size).start();
    auto const r2 = co_await failover_task;

    co_return r2 >= 0 ? r2 : -EIO;
}

Raid1Disk::WriteBackupMode Raid1Disk::__compute_backup_mode(RouteState const& state, uint64_t addr, uint32_t len,
                                                            bool is_discard) const noexcept {
    if (!state.is_degraded) return WriteBackupMode::WRITE;
    if (state.backup_dev->unavail.test(std::memory_order_acquire)) return WriteBackupMode::SKIP;
    if (_dirty_bitmap->is_dirty(addr, len)) {
        auto const chunk_size = be32toh(_sb->fields.bitmap.chunk_size);
        auto const totally_aligned = (chunk_size <= len) && (0 == len % chunk_size) && (0 == addr % chunk_size);
        if (!totally_aligned || is_discard) return WriteBackupMode::SKIP;
        return WriteBackupMode::OPTIMISTIC;
    }
    return WriteBackupMode::WRITE;
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
    auto const is_discard = (op == UBLK_IO_OP_DISCARD || op == UBLK_IO_OP_WRITE_ZEROES);

    // Halt the Resync Task for the duration of this write (both active and backup legs).
    auto const _guard = raid1::ResyncWriteGuard{*_resync_task};
    auto const bm = __compute_backup_mode(state, addr, len, is_discard);

    auto const adj_addr = addr + _reserved_size;
    auto active_task = state.active_dev->disk->async_iov(q, data, iovecs, nr_vecs, adj_addr).start();

    std::optional< hot_task< int > > backup_task;
    if (bm != WriteBackupMode::SKIP) {
        backup_task.emplace(state.backup_dev->disk->async_iov(q, data, iovecs, nr_vecs, adj_addr).start());
    }

    auto const active_res = co_await active_task;

    if (active_res < 0) {
        _dirty_bitmap->dirty_region(addr, len);
        if (auto d = __become_degraded(true, &state); !d) {
            // SB write failed or CAS lost. Either way the array is degraded in-memory; disk_b
            // received the write. Drain the backup and return its result -- returning -EIO here
            // would be wrong when disk_b succeeded (backup holds valid data, and EITHER-mode reads
            // could otherwise route to the failed disk_a and serve stale data).
            if (backup_task) {
                auto const backup_res = co_await *backup_task;
                co_return backup_res >= 0 ? backup_res : -EIO;
            }
            co_return -EIO;
        }
        if (backup_task) {
            auto const backup_res = co_await *backup_task;
            co_return backup_res >= 0 ? backup_res : -EIO;
        }
        co_return -EIO;
    }

    if (bm == WriteBackupMode::SKIP) {
        _dirty_bitmap->dirty_region(addr, len);
        co_return active_res;
    }

    auto const backup_res = co_await *backup_task;

    if (backup_res < 0) {
        _dirty_bitmap->dirty_region(addr, len);
        if (!state.is_degraded) {
            if (auto d = __become_degraded(false, &state); !d) co_return -EIO;
        }
    } else if (bm == WriteBackupMode::OPTIMISTIC) {
        state.backup_dev->unavail.clear(std::memory_order_release);
        _resync_task->clean_region(addr, len, *state.active_dev);
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
        // Flat read with inline failover -- mirrors __failover_read_async without coroutines or flags.
        thread_local raid1::read_route last_read = raid1::read_route::DEVB;

        auto route = read_route::DEVA;
        if (state.is_degraded && state.backup_dev->unavail.test(std::memory_order_acquire)) {
            route = state.route;
        } else {
            route = (last_read == read_route::DEVB) ? read_route::DEVA : read_route::DEVB;
        }
        if (state.is_degraded && route != state.route && _dirty_bitmap->is_dirty(addr, len)) route = state.route;
        if (!state.is_degraded && __route_to_device(state, route)->unavail.test(std::memory_order_acquire)) {
            route = (route == read_route::DEVA) ? read_route::DEVB : read_route::DEVA;
            RLOGD("Skipping unavail device, routing to alternate")
        }
        last_read = route;

        auto const& primary_dev = __route_to_device(state, route);
        auto const primary_res = primary_dev->disk->sync_iov(UBLK_IO_OP_READ, iovecs, nr_vecs, adj_addr);
        if (primary_res) {
            primary_dev->unavail.clear(std::memory_order_release);
            return primary_res;
        }
        if (!primary_dev->unavail.test_and_set(std::memory_order_acquire))
            RLOGW("Device marked unavailable due to read failure: {}", *primary_dev->disk)

        auto const other_route = (route == read_route::DEVA) ? read_route::DEVB : read_route::DEVA;
        auto const& failover_dev = __route_to_device(state, other_route);
        auto const failover_res = failover_dev->disk->sync_iov(UBLK_IO_OP_READ, iovecs, nr_vecs, adj_addr);
        return failover_res ? failover_res : std::unexpected(std::make_error_condition(std::errc::io_error));
    }

    // WRITE / DISCARD / WRITE_ZEROES: flat replication -- mirrors async_iov write path.
    auto const is_discard = (op == UBLK_IO_OP_DISCARD || op == UBLK_IO_OP_WRITE_ZEROES);

    // Halt the Resync Task before checking bitmap
    auto _guard = raid1::ResyncWriteGuard{*_resync_task};
    auto const bm = __compute_backup_mode(state, static_cast< uint64_t >(addr), len, is_discard);

    auto const active_res = state.active_dev->disk->sync_iov(op, iovecs, nr_vecs, adj_addr);

    if (!active_res) {
        _dirty_bitmap->dirty_region(static_cast< uint64_t >(addr), len);
        if (auto d = __become_degraded(true, &state); !d)
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        if (bm != WriteBackupMode::SKIP) {
            auto const backup_res = state.backup_dev->disk->sync_iov(op, iovecs, nr_vecs, adj_addr);
            return backup_res ? backup_res : std::unexpected(std::make_error_condition(std::errc::io_error));
        }
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }

    if (bm == WriteBackupMode::SKIP) {
        _dirty_bitmap->dirty_region(static_cast< uint64_t >(addr), len);
        return active_res;
    }

    auto const backup_res = state.backup_dev->disk->sync_iov(op, iovecs, nr_vecs, adj_addr);

    if (!backup_res) {
        _dirty_bitmap->dirty_region(static_cast< uint64_t >(addr), len);
        if (!state.is_degraded) {
            if (auto d = __become_degraded(false, &state); !d)
                return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
    } else if (bm == WriteBackupMode::OPTIMISTIC) {
        state.backup_dev->unavail.clear(std::memory_order_release);
        _resync_task->clean_region(addr, len, *state.active_dev);
    }

    return active_res;
}

void Raid1Disk::idle_transition(ublksrv_queue const*, bool enter) noexcept {
    if (!enter) {
        DEBUG_ASSERT(_idle_queue_count.load(std::memory_order_relaxed) > 0,                      // LCOV_EXCL_LINE
                     "idle_transition exit without matching enter - ublksrv contract violated"); // LCOV_EXCL_LINE
        _idle_queue_count.fetch_sub(1, std::memory_order_acq_rel);
        auto lk = std::unique_lock{_idle_probe_lock};
        _idle_probe_a.stop();
        _idle_probe_b.stop();
        return;
    }

    // Start probes only when all queue threads are idle.
    // When _nr_hw_queues == 0 (no prepare call yet), prev+1 < 0 is always false for
    // uint16_t, so the probe fires unconditionally - preserving compat with zero-queue callers
    // that skip prepare (e.g. tests that call idle_transition directly).
    auto const prev = _idle_queue_count.fetch_add(1, std::memory_order_acq_rel);
    if (prev + 1 < _nr_hw_queues.load(std::memory_order_acquire)) return;

    auto const state = __capture_route_state();
    if (state.is_degraded) return; // Resync task handles avail probing in degraded mode

    // Immediate synchronous probe: clear UNAVAIL on any device that has already recovered.
    auto const immediate_probe = [&](std::shared_ptr< MirrorDevice > const& mirror) {
        if (!mirror->unavail.test(std::memory_order_acquire)) return;
        if (probe_mirror(*mirror, _reserved_size)) RLOGD("Idle probe: device recovered: {}", *mirror->disk)
    };
    immediate_probe(state.active_dev);
    immediate_probe(state.backup_dev);

    // Re-capture state under the lock so launch() uses the device that is actually current.
    // swap_device() holds _idle_probe_lock while calling stop(), so acquiring the lock here
    // ensures we cannot start a background probe for a device that was just swapped out.
    auto lk = std::unique_lock{_idle_probe_lock};
    auto const locked_state = __capture_route_state();
    if (locked_state.is_degraded) return;
    _idle_probe_a.launch(locked_state.active_dev, _reserved_size);
    _idle_probe_b.launch(locked_state.backup_dev, _reserved_size);
}

void Raid1Disk::toggle_resync(bool t) {
    _resync_enabled.store(t, std::memory_order_relaxed);
    if (t) {
        auto const state = __capture_route_state();
        if (read_route::EITHER != state.route && !state.backup_dev->disk->is_missing()) {
            _resync_task->launch(_str_uuid, state.active_dev, state.backup_dev, [this] { __become_clean(); });
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
    RELEASE_ASSERT(r1, "swap_device called on non-Raid1 disk");
    return r1->swap_device(old_device_id, std::move(new_device));
}

array_state replica_states(ublk_disk const& disk) noexcept {
    auto const* r1 = as_raid1(disk);
    if (!r1) return array_state{};
    return r1->replica_states();
}

std::pair< std::shared_ptr< ublk_disk >, std::shared_ptr< ublk_disk > > replicas(ublk_disk const& disk) noexcept {
    auto const* r1 = as_raid1(disk);
    if (!r1) return {nullptr, nullptr};
    return r1->replicas();
}

} // namespace raid1

std::shared_ptr< ublk_disk > make_raid1_disk(boost::uuids::uuid const& uuid, std::shared_ptr< ublk_disk > dev_a,
                                             std::shared_ptr< ublk_disk > dev_b, std::string const& parent_id) {
    return std::make_shared< raid1::Raid1Disk >(uuid, std::move(dev_a), std::move(dev_b), parent_id);
}

} // namespace ublkpp
