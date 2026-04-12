#include "ublkpp/raid/raid1.hpp"

#include <set>

#include <boost/uuid/random_generator.hpp>
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
                  (resync_level, "", "resync_level", "Resync prioritization level (0-32)",
                   cxxopts::value< std::uint32_t >()->default_value("4"), "<io_size>"),
                  (avail_delay, "", "avail_delay", "Delay between checking if a degraded device is available again",
                   cxxopts::value< std::uint32_t >()->default_value("5"), "<seconds>"),
                  (resync_delay, "", "resync_delay", "Delay between I/O and Resync context switches",
                   cxxopts::value< std::uint32_t >()->default_value("300"), "<microseconds> (us)"))

namespace ublkpp {

// SubCmd decoders
#define SEND_TO_A (sub_cmd & ((1U << sqe_tgt_data_width) - 2))
#define SEND_TO_B (sub_cmd | 0b1)

namespace raid1 {

// Min page-resolution (how much does the smallest page cover?)
constexpr auto k_min_page_depth = k_min_chunk_size * k_page_size * k_bits_in_byte; // 1GiB from above

// Max user-data size
constexpr uint64_t k_max_user_data =
    (unsigned __int128)(k_min_page_depth - k_page_size) * (UINT64_MAX - sizeof(SuperBlock)) / k_min_page_depth;

// True if the dirty device is a DefunctDisk type
#define DEFUNCT_DEVICE(d) (std::dynamic_pointer_cast< DefunctDisk >((d)) != nullptr)

MirrorDevice::MirrorDevice(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > device) :
        disk(std::move(device)) {
    auto chunk_size = SISL_OPTIONS["chunk_size"].as< uint32_t >();
    if (k_min_chunk_size > chunk_size) {
        RLOGE("Invalid chunk_size: {}KiB [min:{}KiB]", chunk_size / Ki, k_min_chunk_size / Ki) // LCOV_EXCL_START
        throw std::runtime_error("Invalid Chunk Size");
    } // LCOV_EXCL_STOP

    // It is not a failure to be able to load the superblock from a DefunctDevice
    if (DEFUNCT_DEVICE(disk)) return;

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
    sub_cmd_t active_subcmd;
    sub_cmd_t backup_subcmd;
    raid1::read_route route;
    bool is_degraded;
};

Raid1DiskImpl::Raid1DiskImpl(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a,
                             std::shared_ptr< UblkDisk > dev_b, std::string const& parent_id) :
        UblkDisk(), _uuid(uuid), _str_uuid(boost::uuids::to_string(uuid)) {
    // At least one device has to be "real"
    if (DEFUNCT_DEVICE(dev_a) && DEFUNCT_DEVICE(dev_b))
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
    _resync_task = std::make_shared< Raid1ResyncTask >(_dirty_bitmap, reserved_size, block_size(),
                                                       params()->basic.max_sectors << SECTOR_SHIFT, _raid_metrics);

    // Write the up-to-date superblocks and mark devices as in use
    __become_active();
}

void Raid1DiskImpl::__init_params(std::shared_ptr< UblkDisk > const& dev_a, std::shared_ptr< UblkDisk > const& dev_b) {
    RLOGI("Initializing RAID-1 [uuid:{}] from devices {} and {}", _str_uuid, dev_a, dev_b)

    direct_io = true; // RAID-1 prefers DIO; downgraded below if any member doesn't support it
    // We enqueue async responses for RAID1 retries even if our underlying devices use uring
    uses_ublk_iouring = false;

    // Discover overall Device parameters
    auto& our_params = *params();
    our_params.types |= UBLK_PARAM_TYPE_DISCARD;
    our_params.basic.io_opt_shift = ilog2(k_min_chunk_size);

    // Set largest underlying user-data size we support as starting point
    our_params.basic.dev_sectors = k_max_user_data >> SECTOR_SHIFT;

    // Now find the what size we should actually set based on the smallest provided device
    for (auto device_array = std::set< std::shared_ptr< UblkDisk > >{dev_a, dev_b}; auto const& device : device_array) {
        if (!device->direct_io) {
            RLOGW("Device {} does not support O_DIRECT — RAID-1 will use buffered I/O (backend caching not bypassed!)",
                  device)
            direct_io = false;
        }
        our_params.basic.dev_sectors = std::min(our_params.basic.dev_sectors, device->params()->basic.dev_sectors);
        our_params.basic.logical_bs_shift =
            std::max(our_params.basic.logical_bs_shift, device->params()->basic.logical_bs_shift);
        our_params.basic.physical_bs_shift =
            std::max(our_params.basic.physical_bs_shift, device->params()->basic.physical_bs_shift);

        if (!device->can_discard()) our_params.types &= ~UBLK_PARAM_TYPE_DISCARD;
    }

    auto const bitmap_size = ((our_params.basic.dev_sectors << SECTOR_SHIFT) / k_min_chunk_size) / k_bits_in_byte;
    reserved_size = sizeof(SuperBlock) + bitmap_size;

    // Align user-data to max_sector size
    reserved_size += ((our_params.basic.dev_sectors << SECTOR_SHIFT) - reserved_size) %
        (our_params.basic.max_sectors << SECTOR_SHIFT);

    // Reserve space for the superblock/bitmap
    our_params.basic.dev_sectors -= (reserved_size >> SECTOR_SHIFT);

    if (can_discard())
        our_params.discard.discard_granularity = std::max(our_params.discard.discard_granularity, block_size());
}

void Raid1DiskImpl::__load_and_select_superblock(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a,
                                                 std::shared_ptr< UblkDisk > dev_b, std::string const& parent_id) {
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
    __store_read_route(static_cast< read_route >(_sb->fields.read_route));

    // Initialize Age if New
    if (_device_a->new_device && _device_b->new_device) _sb->fields.bitmap.age = htobe64(1);
}

void Raid1DiskImpl::__init_bitmap_and_degraded_route() {
    auto const state = __capture_route_state();
    // Read in existing dirty BITMAP pages
    _dirty_bitmap = std::make_shared< Bitmap >(capacity(), be32toh(_sb->fields.bitmap.chunk_size), block_size(),
                                               _sb->superbitmap_reserved, _str_uuid);
    if (state.active_dev->new_device) _dirty_bitmap->init_to(state.active_dev->disk);
    if (state.backup_dev->new_device) _dirty_bitmap->init_to(state.backup_dev->disk);

    // We need to completely dirty one side if either is new when the other is not, this will also
    // apply to when a DefunctDisk is provided, load_from will be skipped in this case.
    if ((DEFUNCT_DEVICE(state.active_dev->disk) || DEFUNCT_DEVICE(state.backup_dev->disk))) {
        RLOGW("RAID1 device [uuid:{}] is running with a defunct device!", _str_uuid)
        // Defunct disks are marked NEW
        __store_read_route(state.active_dev->new_device ? read_route::DEVB : read_route::DEVA);
        // We still want to load from the BITMAP and not bump the age, maybe we'll get the orig
        // disk back in a swap when it becomes available?
        _dirty_bitmap->load_from(*state.active_dev->disk);
    } else if (state.active_dev->new_device xor state.backup_dev->new_device) {
        // Bump the bitmap age
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 16);
        RLOGW("Device is replacement {}, dirty all of BITMAP",
              *(state.active_dev->new_device ? state.active_dev->disk : state.backup_dev->disk))
        _dirty_bitmap->dirty_region(0, capacity());
        __store_read_route(state.active_dev->new_device ? read_route::DEVB : read_route::DEVA);
    } else if ((read_route::EITHER != __get_read_route()) && (0 == _sb->fields.clean_unmount)) {
        // Bump the bitmap age
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 16);
        RLOGW("Unclean shutdown in degraded mode! Dirty all of BITMAP")
        _dirty_bitmap->dirty_region(0, capacity());
    } else if (read_route::EITHER != __get_read_route()) {
        RLOGW("Raid1 is starting in degraded mode [uuid:{}]! Degraded device: {}", _str_uuid, *state.backup_dev->disk)
        _dirty_bitmap->load_from(*state.active_dev->disk);
    } else if (0 == _sb->fields.clean_unmount) {
        RLOGW("Raid1 was not cleanly shutdown last time [uuid:{}]!", _str_uuid)
    }
}

void Raid1DiskImpl::__become_active() {
    // Mark the devices as ACTIVE and write the updated superblocks
    auto const state = __capture_route_state();
    _sb->fields.clean_unmount = 0x0;
    _sb->fields.device_b = 0; // Reset this in case we loaded from dev_b
    if (!write_superblock(*state.active_dev->disk, _sb.get(), read_route::DEVB == state.route, state.route)) {
        // If already degraded this is Fatal
        if (state.is_degraded) { throw std::runtime_error(fmt::format("Could not initialize superblocks!")); }
        // Disk A failed to write superblock, trigger a degradation on it by mocking a fake sub_cmd for it
        if (!__become_degraded(0b0, &state, false)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
        return;
    }
    if (DEFUNCT_DEVICE(state.backup_dev->disk)) return;
    if (!write_superblock(*state.backup_dev->disk, _sb.get(), read_route::DEVB != state.route, state.route)) {
        if (!__become_degraded(0b1, &state, false)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
    }
}

Raid1DiskImpl::~Raid1DiskImpl() {
    RLOGD("Shutting down; [uuid:{}]", _str_uuid)
    [[maybe_unused]] auto cnt_at_stop = _resync_task->stop();
    DEBUG_ASSERT_EQ(0, cnt_at_stop, "Outstanding Write Count is Non-Zero!");

    if (!_sb) return;

    auto const state = __capture_route_state();
    // Write out our dirty bitmap
    if (state.is_degraded && !DEFUNCT_DEVICE(state.backup_dev->disk)) {
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

std::list< int > Raid1DiskImpl::open_for_uring(int const iouring_device_start) {
    // Now that we're up and the target wants to begin I/O let's unpause our resync task, there are no i/o threads
    // yet so we can continue to access _device_a and b directly
    auto fds = (_device_a->disk->open_for_uring(iouring_device_start));
    fds.splice(fds.end(), _device_b->disk->open_for_uring(iouring_device_start + fds.size()));

    // I/O will start comming in now, enable resync
    toggle_resync(true);
    return fds;
}

// Care must be taken in this routine as we need the correct order of the read_route, swap() and unavil calls.
// Ensure that these are left in the order as they appear to prevent breaking the read-retry loop in
// __capture_route_state(...)! While this occurs when threaded, it must access the _device_a and _device_b
// pointers directly to mutate them. This is safe as long as we follow this requirement and hold the lock
// throughout the "relative quick" operation (double sync I/O).
bool Raid1DiskImpl::__swap_device(std::string const& outgoing_device_id,
                                  std::shared_ptr< MirrorDevice >& incoming_mirror,
                                  raid1::read_route const& cur_route) {
    // Make this routine exclusive
    auto lg = std::scoped_lock< std::mutex >(_swap_lock);

    bool const swapping_device_a = (_device_a->disk->id() == outgoing_device_id);
    auto new_read_route = swapping_device_a ? read_route::DEVB : read_route::DEVA;

    auto orig_route = cur_route;
    if (!__set_read_route(orig_route, new_read_route)) return false;

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
        __set_read_route(new_read_route, cur_route);
        return false;
    }
    // Commit SuperBlock to new device; if this fails it's not fatal per say...could work
    // later when we become clean; so let's be optimistic!
    write_superblock(*outgoing_dev->disk, _sb.get(), !swapping_device_a, __get_read_route());

    // Dirty entire bitmap if this is a new device
    if (outgoing_dev->new_device) _dirty_bitmap->dirty_region(0, capacity());
    // Open up for Large WRITES and RESYNC
    outgoing_dev->unavail.clear(std::memory_order_release);
    return true;
}

// ##########################################!! WARNING !!##########################################
// One should not directly access _device_a, _device_b or _read_route directly following this point.
// It is running in a multi-threaded case and subject to swap_device an become_degraded altering
// these values between your memory loads. Use the following method to access this in a consistent
// manner that is race-free
// ##########################################!! WARNING !!##########################################

// ═══════════════════════════════════════════════════════════════════════════════
// ☠️ ☠️ ☠️  DANGER ZONE: LOCK-FREE SYNCHRONIZATION - DO NOT MODIFY  ☠️ ☠️ ☠️
// ═══════════════════════════════════════════════════════════════════════════════
//
// This function implements a lock-free read-retry pattern that is INTENTIONALLY
// a data race by C++ standard but SAFE (on x86-64) in practice through application-level
// synchronization.
//
// THE PROBLEM:
// - Concurrent read of shared_ptr while another thread swaps it = UB (data race)
// - shared_ptr is 16 bytes (ptr + control_block), NOT atomically readable
// - Torn reads can create {old_ptr, new_control_block} corrupted state
//
// THE SOLUTION:
// - Read device pointers twice with a route read in between
// - Validate pointers haven't changed → retry if they have
// - Only proceed when we have a consistent snapshot
// - Torn reads are DETECTED by validation before we use the data
//
// WHAT MAKES THIS SAFE:
// 1. Validation loop catches all torn/inconsistent reads
// 2. We NEVER dereference or use corrupted shared_ptr state
// 3. Writer ordering (CAS route → swap devices) ensures consistency
// 4. Pointer reads are atomic on x86-64 (detection works)
//
// WHAT CAN GO WRONG IF YOU MODIFY THIS:
// - Remove validation → use-after-free from torn reads
// - Weaken validation → ABA problems, stale route with new devices
// - Use data before validation → memory corruption
// - Change retry logic → infinite loops or missed swaps
//
// TSAN FLAGS THIS: Yes, correctly. See tsan.supp for suppression rationale.
//
// ⚠️  DO NOT MODIFY UNLESS YOU FULLY UNDERSTAND LOCK-FREE MEMORY MODELS ⚠️
//
// ═══════════════════════════════════════════════════════════════════════════════
RouteState Raid1DiskImpl::__capture_route_state(sub_cmd_t sub_cmd) const {
    auto const sub_to_a = (sub_cmd & ((1U << sqe_tgt_data_width) - 2));
    auto const sub_to_b = (sub_cmd | 0b1);
    while (true) {
        auto a = _device_a;
        auto b = _device_b;
        auto const route = __get_read_route();
        if (_device_a != a || _device_b != b) continue;

        return RouteState{.active_dev = (read_route::DEVB == route) ? std::move(b) : std::move(a),
                          .backup_dev = (read_route::DEVB == route) ? std::move(a) : std::move(b),
                          .active_subcmd = static_cast< sub_cmd_t >((read_route::DEVB == route) ? sub_to_b : sub_to_a),
                          .backup_subcmd = static_cast< sub_cmd_t >((read_route::DEVB == route) ? sub_to_a : sub_to_b),
                          .route = route,
                          .is_degraded = (read_route::EITHER != route)};
    }
}

// Helper return type for route-to-device mapping
struct RouteSelection {
    std::shared_ptr< MirrorDevice > const& device;
    sub_cmd_t subcmd;
};

// Helper: Decode logical route to physical device and subcmd from captured state
// Maps DEVA/DEVB to the actual device/subcmd currently in that physical slot,
// accounting for swaps that may have changed active/backup mapping
static inline RouteSelection __route_to_device(RouteState const& state, raid1::read_route logical_route) noexcept {
    bool const is_slot_a = (read_route::DEVA == logical_route);
    auto const use_active = is_slot_a ? (state.route != read_route::DEVB) : (state.route == read_route::DEVB);
    return {use_active ? state.active_dev : state.backup_dev, use_active ? state.active_subcmd : state.backup_subcmd};
}

// RAID1 devices have the property of being replacable while maintaining
// consistency due to the fact that the data is replicated. Swapping a device
// may occur for a multitude of _reasons_ but the following RULES apply when
// attemtping to service a given swap_request. We will refer to the *new* device
// being added to the array as the _incoming_ device and the one being replaced
// as the _outgoing_ device.
//
// * _incoming_ device must support DirectI/O w/o volatile caching
// * _incoming_ device must be >= the (capacity() + reserved_size) of the Array
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
std::shared_ptr< UblkDisk > Raid1DiskImpl::swap_device(std::string const& outgoing_device_id,
                                                       std::shared_ptr< UblkDisk > incoming_device) {
    if (!incoming_device->direct_io) {
        RLOGW("Replacement device {} does not support O_DIRECT — RAID-1 will use buffered I/O (backend caching not "
              "bypassed!)",
              incoming_device)
    }
    auto& our_params = *params();
    if ((our_params.basic.dev_sectors + (reserved_size >> SECTOR_SHIFT)) >
            incoming_device->params()->basic.dev_sectors ||
        (our_params.basic.logical_bs_shift < incoming_device->params()->basic.logical_bs_shift)) {
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

    // Terminate any ongoing resync task BEFORE clearing bitmap to avoid race condition
    auto old_resync_flag = _resync_enabled;
    toggle_resync(false);

    // Now safe to clear bitmap (resync stopped, no concurrent access)
    try {
        // TODO we need to save the SuperBitmap Here!
        if (!DEFUNCT_DEVICE(incoming_mirror->disk) && incoming_mirror->new_device)
            _dirty_bitmap->init_to(incoming_mirror->disk);
    } catch (std::runtime_error const&) {
        toggle_resync(old_resync_flag);
        return incoming_device;
    }

    // Atomically swap the device or fail; fail if swapping sole active device
    if (__swap_device(outgoing_device_id, incoming_mirror, state.route) && _raid_metrics) {
        // Record successful device swap
        _raid_metrics->record_device_swap();
    }

    // Now set back to IDLE state and kick a resync task off
    if (old_resync_flag) toggle_resync(true);

    // incoming_mirror now holds the outgoing device (or incoming if failed)
    return incoming_mirror->disk;
}

raid1::array_state Raid1DiskImpl::replica_states() const noexcept {
    auto const sz_to_sync = _dirty_bitmap->dirty_data_est();
    auto const state = __capture_route_state();

    switch (state.route) {
    case read_route::DEVA:
        return raid1::array_state{.device_a = replica_state::CLEAN,
                                  .device_b = state.backup_dev->unavail.test(std::memory_order_acquire)
                                      ? replica_state::ERROR
                                      : replica_state::SYNCING,
                                  .bytes_to_sync = sz_to_sync};
    case read_route::DEVB:
        return raid1::array_state{.device_a = state.backup_dev->unavail.test(std::memory_order_acquire)
                                      ? replica_state::ERROR
                                      : replica_state::SYNCING,
                                  .device_b = replica_state::CLEAN,
                                  .bytes_to_sync = sz_to_sync};
    case read_route::EITHER:
    default:
        return raid1::array_state{
            .device_a = replica_state::CLEAN, .device_b = replica_state::CLEAN, .bytes_to_sync = 0};
    }
}

std::pair< std::shared_ptr< UblkDisk >, std::shared_ptr< UblkDisk > > Raid1DiskImpl::replicas() const noexcept {
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

io_result Raid1DiskImpl::__become_clean() {
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
    __set_read_route(old_route, read_route::EITHER);
    return 0;
}

io_result Raid1DiskImpl::__become_degraded(sub_cmd_t failed_path, RouteState const* cur_state, bool spawn_resync) {
    // Determine new route based on which device failed
    auto old_route = read_route::EITHER;
    auto new_route =
        (0b1 & (failed_path >> cur_state->backup_dev->disk->route_size())) ? read_route::DEVA : read_route::DEVB;
    if (!__set_read_route(old_route, new_route)) {
        if (old_route == new_route) return 0; // Already degraded
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }

    auto const backup_clean = (read_route::DEVB == new_route);

    auto const old_age = _sb->fields.bitmap.age;
    _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 1);
    RLOGW("Device became degraded {} [age:{}] [uuid:{}]", *cur_state->backup_dev->disk,
          static_cast< uint64_t >(be64toh(_sb->fields.bitmap.age)), _str_uuid);

    // Record degradation event in metrics with device name
    if (_raid_metrics) {
        auto device_name = (new_route == read_route::DEVA) ? "device_b" : "device_a";
        _raid_metrics->record_device_degraded(device_name);
    }

    // Must update age first; we do this synchronously to gate pending retry results
    auto& working_device = backup_clean ? *cur_state->backup_dev->disk : *cur_state->active_dev->disk;
    if (auto sync_res = write_superblock(working_device, _sb.get(), backup_clean, new_route); !sync_res) {
        // Rollback the failure to update the header
        _sb->fields.bitmap.age = old_age;
        __set_read_route(new_route, old_route);
        RLOGE("Could not become degraded [uuid:{}]: {}", _str_uuid, sync_res.error().message())
        return sync_res;
    }
    auto& failed_device = backup_clean ? cur_state->active_dev : cur_state->backup_dev;
    failed_device->unavail.test_and_set(std::memory_order_acquire);
    if (spawn_resync && _resync_enabled) toggle_resync(true); // Launch a Resync Task
    return 0;
}

// Failed Async WRITEs all end up here and have the side-effect of dirtying the BITMAP
// on the working device. This blocks the final result going back from the original operation
// as we chain additional sub_cmds by returning a value > 0 including a new "result" for the
// original sub_cmd
io_result Raid1DiskImpl::__handle_async_retry(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                              ublk_io_data const* async_data) {
    // No Synchronous operations retry
    DEBUG_ASSERT_NOTNULL(async_data, "Retry on an synchronous I/O!"); // LCOV_EXCL_LINE

    // Record this degraded operation in the bitmap, then unblock _resync_task (if exists)
    _dirty_bitmap->dirty_region(addr, len);
    _resync_task->dequeue_write();

    // Check if this sub_cmd went to what we currently consider Clean, if we're also dirty this is a fatal error
    auto const state = __capture_route_state();
    if (state.is_degraded && state.active_subcmd == (sub_cmd & _route_mask)) {
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }

    io_result dirty_res;
    if (dirty_res = __become_degraded(sub_cmd, &state); !dirty_res) return dirty_res;

    if (is_replicate(sub_cmd)) return dirty_res;

    // Bitmap is marked dirty, queue a new asynchronous "reply" for this original cmd
    _pending_results[q].emplace_back(async_result{async_data, sub_cmd, static_cast< int >(len)});
    if (q) {
        if (0 != ublksrv_queue_send_event(q)) { // LCOV_EXCL_START
            RLOGE("Failed to send event!");
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        } // LCOV_EXCL_STOP
    }
    return dirty_res.value() + 1;
}

/// This is the primary I/O handler call for RAID1
//
//  RAID1 is primary responsible for replicating mutations (e.g. Writes/Discards) to a pair of compatible devices.
//  READ operations need only go to one side. So they are handled separately.
io_result Raid1DiskImpl::__replicate(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len,
                                     ublksrv_queue const* q, ublk_io_data const* async_data,
                                     RouteState* captured_state) {
    // Capture routing state atomically at function entry (if not provided by recursive call)
    RouteState local_state;
    auto second_write = true;
    if (!captured_state) {
        sub_cmd = shift_route(sub_cmd, route_size());
        // sub_cmd should not be used for the remainder of this execution, use the captured versions
        local_state = __capture_route_state(sub_cmd);
        captured_state = &local_state;
        second_write = false;
    }

    // Sync IOVs have to track this differently as the do not receive on_io_complete
    if (async_data) _resync_task->enqueue_write();

    // Determine where we're going
    auto cur_subcmd = second_write ? captured_state->backup_subcmd : captured_state->active_subcmd;
    auto cur_disk = second_write ? captured_state->backup_dev->disk : captured_state->active_dev->disk;

    // Queue the I/O on the active device
    auto active_res = func(*cur_disk, cur_subcmd);

    // If not-degraded and sub_cmd failed immediately, dirty bitmap and return result of op on alternate-path
    // This condition always returns before the nested call!
    if (!active_res) {
        if (async_data) _resync_task->dequeue_write();
        if (captured_state->is_degraded && !second_write) {
            RLOGE("Double failure! [tag:{:#0x},sub_cmd:{}]", async_data->tag, ublkpp::to_string(sub_cmd))
            return active_res;
        }
        // We only dirty if we can become degraded here, because unlike in the handle_async_retry
        // case, the I/O has not potentially landed on the replicant disk yet; saving us the effort
        // of clearing it later.
        io_result backup_res;
        if (backup_res = __become_degraded(cur_subcmd, captured_state); !backup_res) return backup_res;
        _dirty_bitmap->dirty_region(addr, len);
        if (second_write) return backup_res;

        // Queue the I/O on the backup device after active failed
        if (async_data) _resync_task->enqueue_write();
        if (active_res = func(*captured_state->backup_dev->disk, captured_state->backup_subcmd); !active_res) {
            if (async_data) _resync_task->dequeue_write();
            return active_res;
        }
        return active_res.value() + backup_res.value();
    }
    if (second_write) return active_res;

    // If the address or length are not entirely aligned by the chunk size and there are dirty bits, then try
    // and dirty more pages, the recovery strategy will need to correct this later
    if (captured_state->is_degraded) {
        if (auto dirty_unavail = captured_state->backup_dev->unavail.test(std::memory_order_acquire);
            dirty_unavail || _dirty_bitmap->is_dirty(addr, len)) {
            auto const chunk_size = be32toh(_sb->fields.bitmap.chunk_size);
            auto const totally_aligned = ((chunk_size <= len) && (0 == len % chunk_size) && (0 == addr % chunk_size));
            if (dirty_unavail || !totally_aligned) {
                _dirty_bitmap->dirty_region(addr, len);
                return active_res.value();
            }
            // We will go ahead and attempt this WRITE on a known degraded device,
            // set this flag so we can clear any bits in the bitmap should is succeed
            captured_state->backup_subcmd = set_flags(captured_state->backup_subcmd, sub_cmd_flags::INTERNAL);
        }
    }

    // Otherwise tag the replica sub_cmd so we don't include its value in the target result
    captured_state->backup_subcmd = set_flags(captured_state->backup_subcmd, sub_cmd_flags::REPLICATE);
    auto replica_res = __replicate(sub_cmd, std::move(func), addr, len, q, async_data, captured_state);
    return replica_res ? active_res.value() += replica_res.value() : replica_res;
}

io_result Raid1DiskImpl::__failover_read(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len,
                                         RouteState const* state) {
    // Per-thread read load balancer state (each queue runs on dedicated thread)
    thread_local raid1::read_route last_read = raid1::read_route::DEVB;

    auto const retry = is_retry(sub_cmd);
    if (!state && !retry) {
        // Shift route before capturing state for first attempt
        sub_cmd = shift_route(sub_cmd, route_size());
    }

    // Capture routing state atomically at function entry, or reuse passed state
    RouteState const local_state = state ? RouteState{} : __capture_route_state(sub_cmd);
    auto nested_retry = !!state;
    if (!state) state = &local_state;

    // Decode last_read from existing route bits, if nested call last_route is already set correctly.
    if (retry && !nested_retry) {
        last_read = (0b1 & ((sub_cmd) >> state->backup_dev->disk->route_size())) ? read_route::DEVB : read_route::DEVA;
    }

    // Pick a device to read from (load-balancer)
    auto route = read_route::DEVA;
    auto need_to_test{false};
    if (state->is_degraded && (!retry && state->backup_dev->unavail.test(std::memory_order_acquire))) {
        route = state->route;
    } else {
        if (read_route::DEVB == last_read) {
            if (read_route::DEVB == state->route) need_to_test = true;
        } else {
            route = read_route::DEVB;
            if (read_route::DEVA == state->route) need_to_test = true;
        }
    }

    // If we are degraded and the load-balancer wants to use the dirty disk, check it for dirty bits first
    // and if all true; then  use the current active route from the captured state
    if (state->is_degraded && need_to_test && _dirty_bitmap->is_dirty(addr, len)) route = state->route;

    // We've already attempted this device...we don't want to re-attempt
    if (retry && (last_read == route)) return std::unexpected(std::make_error_condition(std::errc::io_error));

    // Move load-balancer forward, this is optimistic, doesn't need to be atomic
    last_read = route;

    // Attempt read on device using captured state shared_ptrs to avoid races with swap_device
    // Map logical route (DEVA/DEVB) to physical device and subcmd from captured state
    auto const [chosen_dev, chosen_sub_base] = __route_to_device(*state, route);
    // Add RETRIED flag if this is a retry attempt
    auto const chosen_sub = retry ? set_flags(chosen_sub_base, sub_cmd_flags::RETRIED) : chosen_sub_base;

    if (auto res = func(*chosen_dev->disk, chosen_sub); res || retry) { return res; }

    // Otherwise fail over the device and attempt the READ again marking this a retry
    sub_cmd = set_flags(sub_cmd, sub_cmd_flags::RETRIED);
    return __failover_read(sub_cmd, std::move(func), addr, len, state);
}

io_result Raid1DiskImpl::handle_internal(ublksrv_queue const*, ublk_io_data const* data,
                                         [[maybe_unused]] sub_cmd_t sub_cmd, iovec* iovecs, uint32_t nr_vecs,
                                         uint64_t addr, int res) {
    DEBUG_ASSERT(is_internal(sub_cmd), "handle_internal on: {}", ublkpp::to_string(sub_cmd));
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    auto const lba = addr >> params()->basic.logical_bs_shift;

    // Capture routing state atomically at function entry
    auto const state = __capture_route_state();

    if (UBLK_IO_OP_READ == ublksrv_get_op(data->iod)) {
        state.backup_dev->unavail.clear(std::memory_order_release);
        return io_result(0);
    }
    if (0 == res) {
        RLOGI("Cleared {:#0x}Ki Inline! @ lba:{:#0x} [uuid:{}]", len / Ki, lba, _str_uuid)
        state.backup_dev->unavail.clear(std::memory_order_release);
        _resync_task->clean_region(addr, len, *state.active_dev); // We helped!
    } else {
        RLOGW("Dirtied: {:#0x}Ki Inline! @ lba:{:#0x} [uuid:{}]", len / Ki, lba, _str_uuid)
        _dirty_bitmap->dirty_region(addr, len);
    }
    _resync_task->dequeue_write();
    return io_result(0);
}

void Raid1DiskImpl::collect_async(ublksrv_queue const* q, std::list< async_result >& results) {
    auto const state = __capture_route_state();
    results.splice(results.end(), std::move(_pending_results[q]));
    if (!state.active_dev->disk->uses_ublk_iouring) state.active_dev->disk->collect_async(q, results);
    if (!state.backup_dev->disk->uses_ublk_iouring) state.backup_dev->disk->collect_async(q, results);
}

io_result Raid1DiskImpl::handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd,
                                        uint32_t len, uint64_t addr) {
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("received DISCARD: [tag:{:#0x}] [lba:{:#0x}|len:{:#0x}] [uuid:{}]", data->tag, lba, len, _str_uuid)

    if (is_retry(sub_cmd)) [[unlikely]]
        return __handle_async_retry(sub_cmd, addr, len, q, data);

    return __replicate(
        sub_cmd,
        [q, data, len, a = addr + reserved_size](UblkDisk& d, sub_cmd_t scmd) -> io_result {
            // Discard does not support internal commands, we can safely ignore these optimistic operations
            if (is_internal(scmd)) return 0;
            return d.handle_discard(q, data, scmd, len, a);
        },
        addr, len, q, data);
}

io_result Raid1DiskImpl::async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                                   uint32_t nr_vecs, uint64_t addr) {
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    RLOGT("Received {}: [tag:{:#0x}] [lba:{:#0x}|len:{:#0x}] [sub_cmd:{}] [uuid:{}]",
          ublksrv_get_op(data->iod) == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag,
          addr >> params()->basic.logical_bs_shift, len, ublkpp::to_string(sub_cmd), _str_uuid)

    // READs are a special sub_cmd that just go to one side we'll do explicitly
    if (UBLK_IO_OP_READ == ublksrv_get_op(data->iod))
        return __failover_read(
            sub_cmd,
            [q, data, iovecs, nr_vecs, a = addr + reserved_size](UblkDisk& d, sub_cmd_t scmd) {
                return d.async_iov(q, data, scmd, iovecs, nr_vecs, a);
            },
            addr, len);

    if (is_retry(sub_cmd)) [[unlikely]]
        return __handle_async_retry(sub_cmd, addr, len, q, data);

    return __replicate(
        sub_cmd,
        [q, data, iovecs, nr_vecs, a = addr + reserved_size](UblkDisk& d, sub_cmd_t scmd) {
            return d.async_iov(q, data, scmd, iovecs, nr_vecs, a);
        },
        addr, len, q, data);
}

io_result Raid1DiskImpl::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("Received {}: [lba:{:#0x}|len:{:#0x}] [uuid:{}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", lba, len,
          _str_uuid)

    // READs are a special sub_cmd that just go to one side we'll do explicitly
    if (UBLK_IO_OP_READ == op)
        return __failover_read(
            0U,
            [iovecs, nr_vecs, a = addr + reserved_size](UblkDisk& d, sub_cmd_t) {
                return d.sync_iov(UBLK_IO_OP_READ, iovecs, nr_vecs, a);
            },
            addr, len);

    _resync_task->enqueue_write();
    size_t res{0};
    auto io_res = __replicate(
        0U,
        [&res, op, iovecs, nr_vecs, a = addr + reserved_size](UblkDisk& d, sub_cmd_t s) {
            auto p_res = d.sync_iov(op, iovecs, nr_vecs, a);
            // Noramlly the target handles the result being duplicated for WRITEs, we handle it for sync_io here
            if (p_res && !is_replicate(s)) res += p_res.value();
            return p_res;
        },
        addr, len);
    _resync_task->dequeue_write();

    if (!io_res) return io_res;
    return res;
}

void Raid1DiskImpl::on_io_complete(ublk_io_data const* data, sub_cmd_t sub_cmd, int res) {
    auto const state = __capture_route_state();
    // Determine which device handled this I/O based on the lowest bit of sub_cmd
    // 0 = device A, 1 = device B
    auto const orig_route =
        (0b1 & ((sub_cmd) >> state.backup_dev->disk->route_size())) ? read_route::DEVB : read_route::DEVA;
    DLOGT("Raid1DiskImpl::on_io_complete [tag:{:0x}] [sub_cmd:{}] orig_route:{}", data->tag, ublkpp::to_string(sub_cmd),
          orig_route)

    // Pass completion notification to the underlying device for its metrics
    // Map orig_route to physical device accounting for potential swap
    __route_to_device(state, orig_route).device->disk->on_io_complete(data, sub_cmd, res);

    // Decrement outstanding write counter for writes (not reads), but only if it was a
    // success.
    // Error cases will be handled by __handle_async_retry
    // as they need to dirty the BITMAP prior to unblocking the resync task.
    if (UBLK_IO_OP_READ != ublksrv_get_op(data->iod)) {
        DEBUG_ASSERT(!is_retry(sub_cmd), "Retried a WRITE command?!");
        if (0 <= res && !is_internal(sub_cmd)) _resync_task->dequeue_write();
    }
}

void Raid1DiskImpl::toggle_resync(bool t) {
    _resync_enabled = t;
    if (_resync_enabled) {
        auto const state = __capture_route_state();
        if (read_route::EITHER != state.route && !DEFUNCT_DEVICE(state.backup_dev->disk)) {
            _resync_task->launch(_str_uuid, state.active_dev, state.backup_dev, [this] { __become_clean(); });
        }
    } else
        _resync_task->stop();
}
} // namespace raid1
} // namespace ublkpp
