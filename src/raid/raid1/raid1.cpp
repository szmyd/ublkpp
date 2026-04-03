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

using namespace std::chrono_literals;

namespace ublkpp {
using raid1::read_route;

// SubCmd decoders
#define SEND_TO_A (sub_cmd & ((1U << sqe_tgt_data_width) - 2))
#define SEND_TO_B (sub_cmd | 0b1)

// True if the dirty device is a DefunctDisk type
#define DEFUNCT_DEVICE(d) (std::dynamic_pointer_cast< DefunctDisk >((d)) != nullptr)
#define RUNNING_DEFUNCT DEFUNCT_DEVICE(DIRTY_DEVICE->disk)

namespace raid1 {

// Min page-resolution (how much does the smallest page cover?)
constexpr auto k_min_page_depth = k_min_chunk_size * k_page_size * k_bits_in_byte; // 1GiB from above

// Max user-data size
constexpr uint64_t k_max_user_data =
    (unsigned __int128)(k_min_page_depth - k_page_size) * (UINT64_MAX - sizeof(SuperBlock)) / k_min_page_depth;

struct free_page {
    void operator()(void* x) { free(x); }
};

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
        sb = std::shared_ptr< SuperBlock >(read_super.value().first, free_page());
    }
}

// Route routines
// *NOTE* These Macros are ONLY safe to use in the destructor/constructors
// An alternative "cleaner" approach is recommended
// ***********************************************************************
#define IS_DEGRADED (read_route::EITHER != __get_read_route())
#define CLEAN_DEVICE (read_route::DEVB == __get_read_route() ? _device_b : _device_a)
#define DIRTY_DEVICE (read_route::DEVB == __get_read_route() ? _device_a : _device_b)
#define DIRTY_SUBCMD (read_route::DEVB == __get_read_route() ? SEND_TO_A : SEND_TO_B)
// ***********************************************************************
Raid1DiskImpl::Raid1DiskImpl(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a,
                             std::shared_ptr< UblkDisk > dev_b, std::string const& parent_id) :
        UblkDisk(), _uuid(uuid), _str_uuid(boost::uuids::to_string(uuid)) {
    // Create metrics with parent_id for correlation
    if (!parent_id.empty()) { _raid_metrics = std::make_unique< UblkRaidMetrics >(parent_id, _str_uuid); }
    direct_io = true; // RAID-1 requires DIO

    // We enqueue async responses for RAID1 retries even if our underlying devices use uring
    uses_ublk_iouring = false;

    // Discover overall Device parameters
    auto& our_params = *params();
    our_params.types |= UBLK_PARAM_TYPE_DISCARD;
    our_params.basic.io_opt_shift = ilog2(k_min_chunk_size);

    // Set largest underlying user-data size we support as starting point
    our_params.basic.dev_sectors = k_max_user_data >> SECTOR_SHIFT;

    RLOGI("Initializing RAID-1 [uuid:{}] from devices {} and {}", _str_uuid, dev_a, dev_b)

    // Now find the what size we should actually set based on the smallest provided device
    for (auto device_array = std::set< std::shared_ptr< UblkDisk > >{dev_a, dev_b}; auto const& device : device_array) {
        if (!device->direct_io) throw std::runtime_error(fmt::format("Device does not support O_DIRECT! {}", device));
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
    //
    reserved_size += ((our_params.basic.dev_sectors << SECTOR_SHIFT) - reserved_size) %
        (our_params.basic.max_sectors << SECTOR_SHIFT);

    // Reserve space for the superblock/bitmap
    our_params.basic.dev_sectors -= (reserved_size >> SECTOR_SHIFT);

    if (can_discard())
        our_params.discard.discard_granularity = std::max(our_params.discard.discard_granularity, block_size());

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
    _read_route_cache.store(_sb->fields.read_route, std::memory_order_relaxed);

    // Initialize Age if New
    if (_device_a->new_device && _device_b->new_device) _sb->fields.bitmap.age = htobe64(1);

    // Read in existing dirty BITMAP pages
    _dirty_bitmap = std::make_shared< Bitmap >(capacity(), be32toh(_sb->fields.bitmap.chunk_size), block_size(),
                                               _sb->superbitmap_reserved, _str_uuid);
    if (_device_a->new_device) {
        _dirty_bitmap->init_to(_device_a->disk);
        if (!_device_b->new_device)
            _read_route_cache.store(static_cast< uint8_t >(read_route::DEVB), std::memory_order_relaxed);
    }
    if (_device_b->new_device) {
        _dirty_bitmap->init_to(_device_b->disk);
        if (!_device_a->new_device)
            _read_route_cache.store(static_cast< uint8_t >(read_route::DEVA), std::memory_order_relaxed);
    }

    // We need to completely dirty one side if either is new when the other is not, this will also
    // apply to when a DefunctDisk is provided, load_from will be skipped in this case.
    sub_cmd_t const sub_cmd = 0U;
    if ((_device_a->new_device xor _device_b->new_device) ||
        ((read_route::EITHER != __get_read_route()) && (0 == _sb->fields.clean_unmount))) {
        // Bump the bitmap age
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 16);
        RLOGW("Device is replacement {}, dirty all of BITMAP",
              *(_device_a->new_device ? _device_a->disk : _device_b->disk))
        _dirty_bitmap->dirty_region(0, capacity());
    } else if (read_route::EITHER != __get_read_route()) {
        RLOGW("Raid1 is starting in degraded mode [uuid:{}]! Degraded device: {}", _str_uuid, *DIRTY_DEVICE->disk)
        _dirty_bitmap->load_from(*CLEAN_DEVICE->disk);
    } else if (0 == _sb->fields.clean_unmount) {
        RLOGW("Raid1 was not cleanly shutdown last time [uuid:{}]!", _str_uuid)
    }

    // We mark the SB dirty here and clean in our destructor so we know if we _crashed_ at some instance later
    _sb->fields.clean_unmount = 0x0;
    _sb->fields.device_b = 0;

    if (RUNNING_DEFUNCT) RLOGW("RAID1 device [uuid:{}] is running with a defunct device!", _str_uuid)

    // If we Fail to write the SuperBlock to CLEAN device we immediately become degraded and try to write to DIRTY
    auto dirty_written{false};
    if (!write_superblock(*CLEAN_DEVICE->disk, _sb.get(), CLEAN_DEVICE == _device_b, __get_read_route())) {
        // If already degraded this is Fatal
        if (IS_DEGRADED) { throw std::runtime_error(fmt::format("Could not initialize superblocks!")); }
        // Disk A failed to write superblock, trigger a degradation on it by mocking a fake sub_cmd for it
        if (!__become_degraded((read_route::DEVB == __get_read_route() ? SEND_TO_B : SEND_TO_A), false)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
        dirty_written = true;
    }
    if (RUNNING_DEFUNCT ||
        (!dirty_written &&
         !write_superblock(*DIRTY_DEVICE->disk, _sb.get(), DIRTY_DEVICE == _device_b, __get_read_route()))) {
        if (!__become_degraded(DIRTY_SUBCMD, false)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
    }
    // Initialize resync_task
    _resync_task = std::make_shared< Raid1ResyncTask >(_dirty_bitmap, reserved_size, block_size(),
                                                       params()->basic.max_sectors << SECTOR_SHIFT, _raid_metrics);
}

Raid1DiskImpl::~Raid1DiskImpl() {
    RLOGD("Shutting down; [uuid:{}]", _str_uuid)
    [[maybe_unused]] auto cnt_at_stop = _resync_task->stop();
    DEBUG_ASSERT_EQ(0, cnt_at_stop, "Outstanding Write Count is Non-Zero!");

    if (!_sb) return;

    // Write out our dirty bitmap
    if (IS_DEGRADED && !RUNNING_DEFUNCT) {
        RLOGI("Synchronizing BITMAP [uuid: {}] to clean device: {}", _str_uuid, *CLEAN_DEVICE->disk)
        if (auto res = _dirty_bitmap->sync_to(*CLEAN_DEVICE->disk, sizeof(SuperBlock)); !res) {
            RLOGW("Could not sync Bitmap to device on shutdown, will require full resync next time! [uuid:{}]",
                  _str_uuid)
            return;
        }
        RLOGI("Synchronized: [uuid: {}]", _str_uuid)
    }
    _sb->fields.clean_unmount = 0x1;
    // Only update the superblock to clean devices
    if (auto res = write_superblock(*CLEAN_DEVICE->disk, _sb.get(), CLEAN_DEVICE == _device_b, __get_read_route());
        !res) {
        if (IS_DEGRADED) {
            RLOGE("Failed to clear clean bit...full sync required upon next assembly [uuid:{}]", _str_uuid)
        }
    }
    if (!IS_DEGRADED) write_superblock(*DIRTY_DEVICE->disk, _sb.get(), DIRTY_DEVICE == _device_b, __get_read_route());
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
    if (!incoming_device->direct_io) return incoming_device;
    auto& our_params = *params();
    if ((our_params.basic.dev_sectors + (reserved_size >> SECTOR_SHIFT)) >
            incoming_device->params()->basic.dev_sectors ||
        (our_params.basic.logical_bs_shift < incoming_device->params()->basic.logical_bs_shift)) {
        RLOGE("Refusing to use device, requires: [lbs<={} && cap>={}Ki]!", 1 << our_params.basic.logical_bs_shift,
              (our_params.basic.dev_sectors << SECTOR_SHIFT) / Ki)
        return incoming_device;
    }

    // We check if the outgoing device is actually part of this array first,
    // then we ensure that the incoming device is actually a different device
    // from what we already have. If either is not true, do nothing.
    if ((_device_a->disk->id() != outgoing_device_id) && (_device_b->disk->id() != outgoing_device_id)) {
        RLOGE("Refusing to replace unrecognized mirror!")
        return incoming_device;
    } else if ((_device_a->disk->id() == incoming_device->id()) || (_device_b->disk->id() == incoming_device->id())) {
        RLOGI("No replacements discovered! {} already in array, nothing to do...", *incoming_device)
        return incoming_device;
    }

    // If we're degraded; check that we're swapping out the degraded device
    auto const cur_route = __get_read_route();
    if (read_route::EITHER != cur_route &&
        (read_route::DEVB == cur_route ? _device_b : _device_a)->disk->id() == outgoing_device_id) {
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
    if (__swap_device(outgoing_device_id, incoming_mirror) && _raid_metrics) {
        // Record successful device swap
        _raid_metrics->record_device_swap();
    }

    // Now set back to IDLE state and kick a resync task off
    if (old_resync_flag) toggle_resync(true);

    // incoming_mirror now holds the outgoing device (or incoming if failed)
    return incoming_mirror->disk;
}

bool Raid1DiskImpl::__swap_device(std::string const& outgoing_device_id,
                                  std::shared_ptr< MirrorDevice >& incoming_mirror) {
    bool const swapping_device_a = (_device_a->disk->id() == outgoing_device_id);
    auto cur_read_route = static_cast< uint8_t >(__get_read_route());
    auto new_read_route = static_cast< uint8_t >(swapping_device_a ? read_route::DEVB : read_route::DEVA);

    if (!__set_read_route(cur_read_route, new_read_route)) return false;

    auto old_age = be64toh(_sb->fields.bitmap.age);
    auto new_age = old_age + 16;

    auto& outgoing_dev = swapping_device_a ? _device_a : _device_b;
    outgoing_dev.swap(incoming_mirror);
    _sb->fields.bitmap.age = htobe64(new_age);

    // Write superblock to staying device first (critical path)
    auto& staying_dev = swapping_device_a ? _device_b : _device_a;
    if (auto sync_res = write_superblock(*staying_dev->disk, _sb.get(), swapping_device_a, __get_read_route());
        !sync_res) {
        RLOGE("Could not advance Age [uuid:{}]: {}", _str_uuid, sync_res.error().message())
        outgoing_dev.swap(incoming_mirror); // Bail Out!
        __set_read_route(new_read_route, cur_read_route);
        _sb->fields.bitmap.age = htobe64(old_age);
        return false;
    } else {
        // Commit SuperBlock to new device; if this fails it's not fatal per say...could work
        // later when we become clean; so let's be optimistic!
        write_superblock(*outgoing_dev->disk, _sb.get(), !swapping_device_a, __get_read_route());
    }

    // Dirty entire bitmap if this is a new device
    if (outgoing_dev->new_device) _dirty_bitmap->dirty_region(0, capacity());
    // Open up for Large WRITES and RESYNC
    outgoing_dev->unavail.clear(std::memory_order_release);
    return true;
}

raid1::array_state Raid1DiskImpl::replica_states() const {
    auto const sz_to_sync = _dirty_bitmap->dirty_data_est();
    switch (__get_read_route()) {
    case read_route::DEVA:
        return raid1::array_state{.device_a = replica_state::CLEAN,
                                  .device_b = _device_b->unavail.test(std::memory_order_acquire)
                                      ? replica_state::ERROR
                                      : replica_state::SYNCING,
                                  .bytes_to_sync = sz_to_sync};
    case read_route::DEVB:
        return raid1::array_state{.device_a = _device_a->unavail.test(std::memory_order_acquire)
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

std::pair< std::shared_ptr< UblkDisk >, std::shared_ptr< UblkDisk > > Raid1DiskImpl::replicas() const {
    return std::make_pair(_device_a->disk, _device_b->disk);
}

std::list< int > Raid1DiskImpl::open_for_uring(int const iouring_device_start) {
    // Now that we're up and the target wants to begin I/O let's unpause our resync task
    auto fds = (_device_a->disk->open_for_uring(iouring_device_start));
    fds.splice(fds.end(), _device_b->disk->open_for_uring(iouring_device_start + fds.size()));

    // I/O will start comming in now, enable resync
    toggle_resync(true);
    return fds;
}

io_result Raid1DiskImpl::__become_clean() {
    auto old_read_route = static_cast< uint8_t >(__get_read_route());
    if (static_cast< uint8_t >(read_route::EITHER) == old_read_route) return 0;
    RLOGI("Device becoming clean [{}] [uuid:{}] ",
          *(static_cast< uint8_t >(read_route::DEVB) == old_read_route ? _device_a : _device_b)->disk, _str_uuid)
    // Write the new SuperBlock with updated clean read_route
    if (auto sync_res = write_superblock(*_device_a->disk, _sb.get(), false, static_cast< read_route >(old_read_route));
        !sync_res) {
        RLOGW("Could not become clean [uuid:{}]: {}", _str_uuid, sync_res.error().message())
    }
    if (auto sync_res = write_superblock(*_device_b->disk, _sb.get(), true, static_cast< read_route >(old_read_route));
        !sync_res) {
        RLOGW("Could not become clean [uuid:{}]: {}", _str_uuid, sync_res.error().message())
    }
    // Avoid checking DirtyBitmap going forward on reads/writes
    __set_read_route(old_read_route, static_cast< uint8_t >(read_route::EITHER));
    return 0;
}

io_result Raid1DiskImpl::__become_degraded(sub_cmd_t sub_cmd, bool spawn_resync) {
    // Determine new route based on which device failed
    auto old_route = static_cast< uint8_t >(read_route::EITHER);
    auto new_route = static_cast< uint8_t >((0b1 & ((sub_cmd) >> _device_b->disk->route_size())) ? read_route::DEVA
                                                                                                 : read_route::DEVB);
    if (!__set_read_route(old_route, new_route)) {
        if (old_route == new_route) return 0; // Already degraded
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }

    // Capture device pointers based on new route to prevent race with swap_device
    auto* clean_dev = (read_route::DEVB == static_cast< read_route >(new_route)) ? _device_b.get() : _device_a.get();
    auto* dirty_dev = (read_route::DEVB == static_cast< read_route >(new_route)) ? _device_a.get() : _device_b.get();
    auto const device_b_is_clean = (read_route::DEVB == static_cast< read_route >(new_route));

    auto const old_age = _sb->fields.bitmap.age;
    _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 1);
    RLOGW("Device became degraded {} [age:{}] [uuid:{}] ", *dirty_dev->disk,
          static_cast< uint64_t >(be64toh(_sb->fields.bitmap.age)), _str_uuid);

    // Record degradation event in metrics with device name
    if (_raid_metrics) {
        auto device_name = (static_cast< read_route >(new_route) == read_route::DEVA) ? "device_b" : "device_a";
        _raid_metrics->record_device_degraded(device_name);
    }

    // Must update age first; we do this synchronously to gate pending retry results
    if (auto sync_res =
            write_superblock(*clean_dev->disk, _sb.get(), device_b_is_clean, static_cast< read_route >(new_route));
        !sync_res) {
        // Rollback the failure to update the header
        _sb->fields.bitmap.age = old_age;
        __set_read_route(new_route, old_route);
        RLOGE("Could not become degraded [uuid:{}]: {}", _str_uuid, sync_res.error().message())
        return sync_res;
    }
    dirty_dev->unavail.test_and_set(std::memory_order_acquire);
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
    auto const cur_route = __get_read_route();
    auto const cur_clean_cmd = (read_route::DEVB == cur_route ? SEND_TO_B : SEND_TO_A);
    if (read_route::EITHER != cur_route && cur_clean_cmd == sub_cmd) {
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }

    io_result dirty_res;
    if (dirty_res = __become_degraded(sub_cmd); !dirty_res) return dirty_res;

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
                                     ublksrv_queue const* q, ublk_io_data const* async_data, MirrorDevice* active_dev,
                                     MirrorDevice* backup_dev, sub_cmd_t active_subcmd) {
    // Apply our shift to the sub_cmd if it's not a replica write

    // Capture routing state atomically at function entry (if not provided by recursive call)
    sub_cmd_t backup_subcmd = SEND_TO_B;
    auto const cur_route = __get_read_route();
    if (!active_dev) {
        sub_cmd = shift_route(sub_cmd, route_size());
        active_dev = (read_route::DEVB == cur_route) ? _device_b.get() : _device_a.get();
        backup_dev = (read_route::DEVB == cur_route) ? _device_a.get() : _device_b.get();
        active_subcmd = (read_route::DEVB == cur_route) ? SEND_TO_B : SEND_TO_A;
        backup_subcmd = (read_route::DEVB == cur_route) ? SEND_TO_A : SEND_TO_B;
    }
    auto const replica_write = is_replicate(active_subcmd);

    // Sync IOVs have to track this differently as the do not receive on_io_complete
    if (async_data) _resync_task->enqueue_write();
    auto res = func(*active_dev->disk, active_subcmd);

    // If not-degraded and sub_cmd failed immediately, dirty bitmap and return result of op on alternate-path
    // This condition always returns before the nested call!
    if (!res) {
        if (async_data) _resync_task->dequeue_write();
        if (read_route::EITHER != cur_route && !replica_write) {
            RLOGE("Double failure! [tag:{:#0x},sub_cmd:{}]", async_data->tag, ublkpp::to_string(sub_cmd))
            return res;
        }
        // We only dirty if we can become degraded here, because unlike in the handle_async_retry
        // case, the I/O has not potentially landed on the replicant disk yet; saving us the effort
        // of clearing it later.
        io_result dirty_res;
        if (dirty_res = __become_degraded(sub_cmd); !dirty_res) return dirty_res;
        _dirty_bitmap->dirty_region(addr, len);

        if (replica_write) return dirty_res;
        if (async_data) _resync_task->enqueue_write();
        if (res = func(*backup_dev->disk, backup_subcmd); !res) {
            if (async_data) _resync_task->dequeue_write();
            return res;
        }
        return res.value() + dirty_res.value();
    }
    if (replica_write) return res;

    // If the address or length are not entirely aligned by the chunk size and there are dirty bits, then try
    // and dirty more pages, the recovery strategy will need to correct this later
    sub_cmd = backup_subcmd;
    if (read_route::EITHER != cur_route) {
        if (auto dirty_unavail = backup_dev->unavail.test(std::memory_order_acquire);
            dirty_unavail || _dirty_bitmap->is_dirty(addr, len)) {
            auto const chunk_size = be32toh(_sb->fields.bitmap.chunk_size);
            auto const totally_aligned = ((chunk_size <= len) && (0 == len % chunk_size) && (0 == addr % chunk_size));
            if (dirty_unavail || !totally_aligned) {
                _dirty_bitmap->dirty_region(addr, len);
                return res.value();
            }
            // We will go ahead and attempt this WRITE on a known degraded device,
            // set this flag so we can clear any bits in the bitmap should is succeed
            sub_cmd = set_flags(sub_cmd, sub_cmd_flags::INTERNAL);
        }
    }

    // Otherwise tag the replica sub_cmd so we don't include its value in the target result
    sub_cmd = set_flags(sub_cmd, sub_cmd_flags::REPLICATE);
    auto replica_res = __replicate(sub_cmd, std::move(func), addr, len, q, async_data, backup_dev, active_dev, sub_cmd);
    if (!replica_res) return replica_res;
    res = res.value() += replica_res.value();
    // Assuming all was successful, return the aggregate of the results
    return res;
}

io_result Raid1DiskImpl::__failover_read(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len) {
    // Capture routing state atomically at function entry
    auto const current_read_route = __get_read_route();
    auto const is_degraded = read_route::EITHER != current_read_route;
    auto* dirty_dev = (read_route::DEVB == current_read_route) ? _device_a.get() : _device_b.get();

    auto const retry = is_retry(sub_cmd);
    if (retry) {
        _last_read = (0b1 & ((sub_cmd) >> _device_b->disk->route_size())) ? read_route::DEVB : read_route::DEVA;
    } else
        sub_cmd = shift_route(sub_cmd, route_size());

    // Pick a device to read from
    auto route = read_route::DEVA;
    auto need_to_test{false};
    if (is_degraded && (!retry && dirty_dev->unavail.test(std::memory_order_acquire))) {
        route = current_read_route;
    } else {
        if (read_route::DEVB == _last_read) {
            if (read_route::DEVB == current_read_route) need_to_test = true;
        } else {
            route = read_route::DEVB;
            if (read_route::DEVA == current_read_route) need_to_test = true;
        }
    }

    // We allow READing from a degraded device if the bitmap does not indicate the chunk is dirty
    if (is_degraded && need_to_test && _dirty_bitmap->is_dirty(addr, len))
        route = (read_route::DEVA == route) ? read_route::DEVB : read_route::DEVA;

    // We've already attempted this device...we don't want to re-attempt
    if (retry && (_last_read == route)) return std::unexpected(std::make_error_condition(std::errc::io_error));

    _last_read = route;

    // Attempt read on device; if it succeeds or we are degraded return the result
    auto device = (read_route::DEVA == route) ? _device_a->disk : _device_b->disk;
    if (auto res = func(*device, _device_a->disk == (device) ? SEND_TO_A : SEND_TO_B); res || retry) { return res; }

    // Otherwise fail over the device and attempt the READ again marking this a retry
    sub_cmd = set_flags(sub_cmd, sub_cmd_flags::RETRIED);
    return __failover_read(sub_cmd, std::move(func), addr, len);
}

io_result Raid1DiskImpl::handle_internal(ublksrv_queue const*, ublk_io_data const* data,
                                         [[maybe_unused]] sub_cmd_t sub_cmd, iovec* iovecs, uint32_t nr_vecs,
                                         uint64_t addr, int res) {
    DEBUG_ASSERT(is_internal(sub_cmd), "handle_internal on: {}", ublkpp::to_string(sub_cmd));
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    auto const lba = addr >> params()->basic.logical_bs_shift;

    // Capture routing state atomically at function entry
    auto const route = __get_read_route();
    auto* dirty_dev = (read_route::DEVB == route) ? _device_a.get() : _device_b.get();
    auto* clean_dev = (read_route::DEVB == route) ? _device_b.get() : _device_a.get();

    if (UBLK_IO_OP_READ == ublksrv_get_op(data->iod)) {
        dirty_dev->unavail.clear(std::memory_order_release);
        return io_result(0);
    }
    if (0 == res) {
        RLOGI("Cleared {:#0x}Ki Inline! @ lba:{:#0x} [uuid:{}]", len / Ki, lba, _str_uuid)
        dirty_dev->unavail.clear(std::memory_order_release);
        _resync_task->clean_region(addr, len, *clean_dev); // We helped!
    } else {
        RLOGW("Dirtied: {:#0x}Ki Inline! @ lba:{:#0x} [uuid:{}]", len / Ki, lba, _str_uuid)
        _dirty_bitmap->dirty_region(addr, len);
    }
    _resync_task->dequeue_write();
    return io_result(0);
}

void Raid1DiskImpl::collect_async(ublksrv_queue const* q, std::list< async_result >& results) {
    results.splice(results.end(), std::move(_pending_results[q]));
    if (!_device_a->disk->uses_ublk_iouring) _device_a->disk->collect_async(q, results);
    if (!_device_b->disk->uses_ublk_iouring) _device_b->disk->collect_async(q, results);
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
    // Determine which device handled this I/O based on the lowest bit of sub_cmd
    // 0 = device A, 1 = device B
    auto const device_bit = static_cast< uint8_t >(sub_cmd & 0x1);
    auto* device = (device_bit == 0) ? _device_a->disk.get() : _device_b->disk.get();

    DLOGT("Raid1DiskImpl::on_io_complete [tag:{:0x}] [sub_cmd:{}] device_bit:{}", data->tag, ublkpp::to_string(sub_cmd),
          device_bit)

    // Pass completion notification to the underlying device for its metrics
    device->on_io_complete(data, sub_cmd, res);

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
        auto const cur_route = __get_read_route();
        if (!RUNNING_DEFUNCT && read_route::EITHER != cur_route) {
            // Capture routing state atomically before launching resync task
            auto clean_dev = (read_route::DEVB == cur_route) ? _device_b : _device_a;
            auto dirty_dev = (read_route::DEVB == cur_route) ? _device_a : _device_b;
            _resync_task->launch(_str_uuid, clean_dev, dirty_dev, [this] { __become_clean(); });
        }
    } else
        _resync_task->stop();
}
} // namespace raid1
} // namespace ublkpp
