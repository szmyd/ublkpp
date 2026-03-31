#include "ublkpp/raid/raid1.hpp"

#include <set>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <ublksrv.h>
#include <ublksrv_utils.h>
#include <sisl/options/options.h>
#include <sisl/utility/thread_factory.hpp>

#include "bitmap.hpp"
#include "raid1_impl.hpp"
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

// Route routines
#define IS_DEGRADED (_is_degraded.test(std::memory_order_acquire))
#define READ_ROUTE (__get_read_route())
#define CLEAN_DEVICE (read_route::DEVB == READ_ROUTE ? _device_b : _device_a)
#define DIRTY_DEVICE (read_route::DEVB == READ_ROUTE ? _device_a : _device_b)
#define CLEAN_SUBCMD (read_route::DEVB == READ_ROUTE ? SEND_TO_B : SEND_TO_A)
#define DIRTY_SUBCMD (read_route::DEVB == READ_ROUTE ? SEND_TO_A : SEND_TO_B)

// True if the dirty device is a DefunctDisk type
#define DEFUNCT_DEVICE(d) (std::dynamic_pointer_cast< DefunctDisk >((d)) != nullptr)
#define RUNNING_DEFUNCT DEFUNCT_DEVICE(DIRTY_DEVICE->disk)

// Track outstanding writes
#ifndef NDEBUG
#define ENQUEUE_WRITE_OP                                                                                               \
    {                                                                                                                  \
        if (auto const old_val = _outstanding_writes.fetch_add(1, std::memory_order_release); 0 == old_val) {          \
            RLOGT("Outstanding Writes: {}", old_val + 1);                                                              \
            __pause_resync();                                                                                          \
        } else if (UINT32_MAX == old_val) {                                                                            \
            DEBUG_ASSERT(false, "Outstanding Write Count Reached UINT32_MAX!");                                        \
        } else {                                                                                                       \
            RLOGT("Outstanding Writes: {}", old_val + 1);                                                              \
        }                                                                                                              \
    }
#define DEQUEUE_WRITE_OP                                                                                               \
    {                                                                                                                  \
        if (auto const old_val = _outstanding_writes.fetch_sub(1, std::memory_order_acquire); 1 == old_val) {          \
            RLOGT("Outstanding Writes: {}", old_val - 1);                                                              \
            __resume_resync();                                                                                         \
        } else if (0 == old_val) {                                                                                     \
            DEBUG_ASSERT(false, "Outstanding Write Count is Negative!");                                               \
        } else {                                                                                                       \
            RLOGT("Outstanding Writes: {}", old_val - 1);                                                              \
        }                                                                                                              \
    }
#else
#define ENQUEUE_WRITE_OP                                                                                               \
    {                                                                                                                  \
        if (0 == _outstanding_writes.fetch_add(1, std::memory_order_release)) __pause_resync();                        \
    }
#define DEQUEUE_WRITE_OP                                                                                               \
    {                                                                                                                  \
        if (1 == _outstanding_writes.fetch_sub(1, std::memory_order_acquire)) __resume_resync();                       \
    }
#endif

namespace raid1 {

// Min page-resolution (how much does the smallest page cover?)
constexpr auto k_min_page_depth = k_min_chunk_size * k_page_size * k_bits_in_byte; // 1GiB from above
constexpr auto k_state_spin_time = 50us;

// Max user-data size
constexpr uint64_t k_max_user_data =
    (unsigned __int128)(k_min_page_depth - k_page_size) * (UINT64_MAX - sizeof(SuperBlock)) / k_min_page_depth;

struct free_page {
    void operator()(void* x) { free(x); }
};

struct MirrorDevice {
    MirrorDevice(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > device);
    std::shared_ptr< UblkDisk > const disk;
    std::shared_ptr< SuperBlock > sb;
    std::atomic_flag unavail;

    bool new_device{true};
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
    __set_read_route(static_cast< read_route >(_sb->fields.read_route));

    // Initialize Age if New
    if (_device_a->new_device && _device_b->new_device) _sb->fields.bitmap.age = htobe64(1);

    // Read in existing dirty BITMAP pages
    _dirty_bitmap = std::make_unique< Bitmap >(capacity(), be32toh(_sb->fields.bitmap.chunk_size), block_size(),
                                               _sb->superbitmap_reserved, _str_uuid);
    if (_device_a->new_device) {
        _dirty_bitmap->init_to(_device_a->disk);
        if (!_device_b->new_device) __set_read_route(read_route::DEVB);
    }
    if (_device_b->new_device) {
        _dirty_bitmap->init_to(_device_b->disk);
        if (!_device_a->new_device) __set_read_route(read_route::DEVA);
    }

    // We need to completely dirty one side if either is new when the other is not, this will also
    // apply to when a DefunctDisk is provided, load_from will be skipped in this case.
    sub_cmd_t const sub_cmd = 0U;
    if ((_device_a->new_device xor _device_b->new_device) ||
        ((read_route::EITHER != READ_ROUTE) && (0 == _sb->fields.clean_unmount))) {
        // Bump the bitmap age
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 16);
        RLOGW("Device is replacement {}, dirty all of BITMAP",
              *(_device_a->new_device ? _device_a->disk : _device_b->disk))
        _dirty_bitmap->dirty_region(0, capacity());
        _is_degraded.test_and_set(std::memory_order_relaxed);
    } else if (read_route::EITHER != READ_ROUTE) {
        RLOGW("Raid1 is starting in degraded mode [uuid:{}]! Degraded device: {}", _str_uuid, *DIRTY_DEVICE->disk)
        _is_degraded.test_and_set(std::memory_order_relaxed);
        _dirty_bitmap->load_from(*CLEAN_DEVICE->disk);
    } else if (0 == _sb->fields.clean_unmount) {
        RLOGW("Raid1 was not cleanly shutdown last time [uuid:{}]!", _str_uuid)
    }

    // We mark the SB dirty here and clean in our destructor so we know if we _crashed_ at some instance later
    _sb->fields.clean_unmount = 0x0;
    _sb->fields.device_b = 0;
    _resync_state.store(static_cast< uint8_t >(resync_state::PAUSE));

    if (RUNNING_DEFUNCT) RLOGW("RAID1 device [uuid:{}] is running with a defunct device!", _str_uuid)

    // If we Fail to write the SuperBlock to then CLEAN device we immediately dirty the bitmap and try to write to
    // DIRTY
    if (!write_superblock(*CLEAN_DEVICE->disk, _sb.get(), CLEAN_DEVICE == _device_b, READ_ROUTE)) {
        // If already degraded this is Fatal
        if (IS_DEGRADED) { throw std::runtime_error(fmt::format("Could not initialize superblocks!")); }
        // This will write the SB to DIRTY so we can skip this down below
        if (!__become_degraded(CLEAN_SUBCMD)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
        return;
    }

    // If we're starting degraded, we need to initiate a resync_task
    if (IS_DEGRADED && !RUNNING_DEFUNCT) {
        _resync_task = sisl::named_thread(fmt::format("r_{}", _str_uuid.substr(0, 13)), [this] { __resync_task(); });
        if (!DIRTY_DEVICE->new_device) return;
    }

    if (RUNNING_DEFUNCT || !write_superblock(*DIRTY_DEVICE->disk, _sb.get(), DIRTY_DEVICE == _device_b, READ_ROUTE)) {
        if (!__become_degraded(DIRTY_SUBCMD)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
    }
}

Raid1DiskImpl::~Raid1DiskImpl() {
    DEBUG_ASSERT_EQ(0, _outstanding_writes.load(), "Outstanding Write Count is Non-Zero!");
    RLOGD("Shutting down; [uuid:{}]", _str_uuid)
    __stop_resync();

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
    if (auto res = write_superblock(*CLEAN_DEVICE->disk, _sb.get(), CLEAN_DEVICE == _device_b, READ_ROUTE); !res) {
        if (IS_DEGRADED) {
            RLOGE("Failed to clear clean bit...full sync required upon next assembly [uuid:{}]", _str_uuid)
        }
    }
    if (!IS_DEGRADED) write_superblock(*DIRTY_DEVICE->disk, _sb.get(), DIRTY_DEVICE == _device_b, READ_ROUTE);
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

    if (IS_DEGRADED && CLEAN_DEVICE->disk->id() == outgoing_device_id) {
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
    } catch (std::runtime_error const& e) { return incoming_device; }

    // Terminate any ongoing resync task BEFORE clearing bitmap to avoid race condition
    __stop_resync();
    _is_degraded.clear(std::memory_order_release);

    // Now safe to clear bitmap (resync stopped, no concurrent access)
    if (incoming_mirror->new_device) _dirty_bitmap->init_to(incoming_mirror->disk);

    // Dirty the entire bitmap if new disk
    if (incoming_mirror->new_device) _dirty_bitmap->dirty_region(0, capacity());

    // Write the superblock to the new device and advance the age to make the outgoint device invalid
    _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 16);
    if (_device_a->disk->id() == outgoing_device_id) {
        _device_a.swap(incoming_mirror);
        write_superblock(*_device_a->disk, _sb.get(), false, READ_ROUTE);
        if (!__become_degraded(0U, false)) {
            __resume_resync();
            return incoming_device;
        }
    } else {
        _device_b.swap(incoming_mirror);
        write_superblock(*_device_b->disk, _sb.get(), true, READ_ROUTE);
        if (!__become_degraded(1U << _device_b->disk->route_size(), false)) {
            __resume_resync();
            return incoming_device;
        }
    }

    // Record successful device swap
    if (_raid_metrics) { _raid_metrics->record_device_swap(); }

    // Now set back to IDLE state and kick a resync task off
    __resume_resync();

    // incoming_mirror now holds the outgoing device
    return incoming_mirror->disk;
}

raid1::array_state Raid1DiskImpl::replica_states() const {
    auto const sz_to_sync = _dirty_bitmap->dirty_data_est();
    switch (READ_ROUTE) {
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
    __resume_resync();
    auto fds = (_device_a->disk->open_for_uring(iouring_device_start));
    fds.splice(fds.end(), _device_b->disk->open_for_uring(iouring_device_start + fds.size()));
    return fds;
}

io_result Raid1DiskImpl::__become_clean() {
    if (!IS_DEGRADED) return 0;
    RLOGI("Device becoming clean [{}] [uuid:{}] ", *DIRTY_DEVICE->disk, _str_uuid)
    __set_read_route(read_route::EITHER);
    if (auto sync_res = write_superblock(*_device_a->disk, _sb.get(), false, READ_ROUTE); !sync_res) {
        RLOGW("Could not become clean [uuid:{}]: {}", _str_uuid, sync_res.error().message())
    }
    if (auto sync_res = write_superblock(*_device_b->disk, _sb.get(), true, READ_ROUTE); !sync_res) {
        RLOGW("Could not become clean [uuid:{}]: {}", _str_uuid, sync_res.error().message())
    }
    _is_degraded.clear(std::memory_order_release);
    return 0;
}

static inline io_result __copy_region(iovec* iovec, int nr_vecs, uint64_t addr, auto& src, auto& dest) {
    auto res = src.sync_iov(UBLK_IO_OP_READ, iovec, nr_vecs, addr);
    if (res) {
        if (res = dest.sync_iov(UBLK_IO_OP_WRITE, iovec, nr_vecs, addr); !res) {
            RLOGW("Could not write clean chunks of [sz:{}] [res:{}]", __iovec_len(iovec, iovec + nr_vecs),
                  res.error().message())
        }
    } else {
        RLOGE("Could not read Data of [sz:{}] [res:{}]", __iovec_len(iovec, iovec + nr_vecs), res.error().message())
    }
    return res;
}

resync_state Raid1DiskImpl::__clean_bitmap() {
    static auto const unavail_delay = std::chrono::seconds(SISL_OPTIONS["avail_delay"].as< uint32_t >());
    static auto const avail_delay = std::chrono::microseconds(SISL_OPTIONS["resync_delay"].as< uint32_t >());

    auto cur_state = resync_state::ACTIVE;
    // Set ourselves up with a buffer to do all the read/write operations from
    auto iov = iovec{.iov_base = nullptr, .iov_len = 0};
    if (auto err = ::posix_memalign(&iov.iov_base, block_size(), params()->basic.max_sectors << SECTOR_SHIFT);
        0 != err || nullptr == iov.iov_base) [[unlikely]] { // LCOV_EXCL_START
        RLOGE("Could not allocate memory for I/O: {}", strerror(err))
        return cur_state;
    } // LCOV_EXCL_STOP

    auto nr_pages = _dirty_bitmap->dirty_pages();
    if (_raid_metrics) { _raid_metrics->record_dirty_pages(nr_pages); }
    while (0 < nr_pages) {
        // TODO Change this so it's easier to control with a future QoS algorithm
        auto copies_left = ((std::min(32U, SISL_OPTIONS["resync_level"].as< uint32_t >()) * 100U) / 32U) * 5U;
        auto [logical_off, sz] = _dirty_bitmap->next_dirty();
        while (0 < sz && 0U < copies_left--) {
            if (0 == sz) break;
            iov.iov_len = std::min(sz, params()->basic.max_sectors << SECTOR_SHIFT);
            // Copy Region from CLEAN to DIRTY
            if (auto res =
                    __copy_region(&iov, 1, logical_off + reserved_size, *CLEAN_DEVICE->disk, *DIRTY_DEVICE->disk);
                res) {
                // Clear Bitmap and set device as available if successful
                if (DIRTY_DEVICE->unavail.test(std::memory_order_acquire)) {
                    RLOGI("Mirror became available again: {} [uuid:{}] ", *DIRTY_DEVICE->disk, _str_uuid)
                    DIRTY_DEVICE->unavail.clear(std::memory_order_release);
                }
                __clean_region(logical_off, iov.iov_len);
                // Record resync progress
                if (_raid_metrics) { _raid_metrics->record_resync_progress(iov.iov_len); }
            } else {
                DIRTY_DEVICE->unavail.test_and_set(std::memory_order_acquire);
                break;
            }
            std::tie(logical_off, sz) = _dirty_bitmap->next_dirty();
        }

        // Yield and check for stopped
        if (cur_state = __yield_resync(
                DIRTY_DEVICE->unavail.test(std::memory_order_acquire) ? unavail_delay : avail_delay, avail_delay);
            resync_state::STOPPED == cur_state)
            break;

        // Sweep and count dirty pages left
        nr_pages = _dirty_bitmap->dirty_pages();
        if (_raid_metrics) { _raid_metrics->record_dirty_pages(nr_pages); }
    }
    free(iov.iov_base);
    return cur_state;
}

void Raid1DiskImpl::__resync_task() {
    DEBUG_ASSERT(!RUNNING_DEFUNCT, "Ran resync task on Defunct disk!");
    RLOGD("Resync Task created for [uuid:{}]", _str_uuid)
    auto const resync_start = std::chrono::steady_clock::now();
    auto cur_state = static_cast< uint8_t >(resync_state::IDLE);
    // Record resync start - increment global and per-device counters
    auto const active_count = s_active_resyncs.fetch_add(1, std::memory_order_relaxed) + 1;
    // Capture the initial size of data to resync
    auto const initial_resync_size = _dirty_bitmap->dirty_data_est();
    if (_raid_metrics) {
        _raid_metrics->record_resync_start();
        _raid_metrics->record_active_resyncs(active_count);
    }
    // Wait to become IDLE
    while (IS_DEGRADED &&
           !_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::ACTIVE))) {
        // If we're stopped or another task was started we should exit
        if ((static_cast< uint8_t >(resync_state::STOPPED) == cur_state) ||
            (static_cast< uint8_t >(resync_state::ACTIVE) == cur_state) ||
            (static_cast< uint8_t >(resync_state::SLEEPING) == cur_state)) {
            RLOGD("Resync Task aborted for [uuid:{}] state: {}", _str_uuid, cur_state)
            return;
        }
        cur_state = static_cast< uint8_t >(resync_state::IDLE);
        std::this_thread::sleep_for(std::chrono::microseconds(SISL_OPTIONS["resync_delay"].as< uint32_t >()));
    }

    // We are now guaranteed to be the only active thread performing I/O on the device
    cur_state = static_cast< uint8_t >(__clean_bitmap());

    // I/O may have been interrupted, if not check the bitmap and mark us as _clean_
    if (static_cast< uint8_t >(resync_state::STOPPED) == cur_state) {
        RLOGD("Resync Task Stopped for [uuid:{}]", _str_uuid)
        if (_raid_metrics) {
            auto const stopped_count = s_active_resyncs.fetch_sub(1, std::memory_order_relaxed) - 1;
            _raid_metrics->record_active_resyncs(stopped_count);
        }
        return;
    }
    if (IS_DEGRADED && 0 == _dirty_bitmap->dirty_pages()) { __become_clean(); }
    _resync_state.compare_exchange_strong(cur_state, static_cast< uint8_t >(resync_state::IDLE));
    if (_raid_metrics) {
        auto const final_count = s_active_resyncs.fetch_sub(1, std::memory_order_relaxed) - 1;
        _raid_metrics->record_active_resyncs(final_count);
        auto const resync_end = std::chrono::steady_clock::now();
        auto const duration_seconds =
            std::chrono::duration_cast< std::chrono::seconds >(resync_end - resync_start).count();
        if (duration_seconds > 0) { _raid_metrics->record_resync_complete(duration_seconds); }
        // Record the size of data that was resynced (initial size before resync started)
        _raid_metrics->record_last_resync_size(initial_resync_size);
    }
    RLOGD("Resync Task Finished for [uuid:{}]", _str_uuid)
}

io_result Raid1DiskImpl::__become_degraded(sub_cmd_t sub_cmd, bool spawn_resync) {
    // We only update the AGE if we're not degraded already
    if (_is_degraded.test_and_set(std::memory_order_acquire)) return 0;
    auto const old_route = READ_ROUTE;
    __set_read_route((0b1 & ((sub_cmd) >> _device_b->disk->route_size())) ? read_route::DEVA : read_route::DEVB);
    auto const old_age = _sb->fields.bitmap.age;
    _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 1);
    RLOGW("Device became degraded {} [age:{}] [uuid:{}] ", *DIRTY_DEVICE->disk,
          static_cast< uint64_t >(be64toh(_sb->fields.bitmap.age)), _str_uuid);

    // Record degradation event in metrics with device name
    if (_raid_metrics) {
        auto device_name = (READ_ROUTE == read_route::DEVA) ? "device_b" : "device_a";
        _raid_metrics->record_device_degraded(device_name);
    }

    // Must update age first; we do this synchronously to gate pending retry results
    if (auto sync_res = write_superblock(*CLEAN_DEVICE->disk, _sb.get(), CLEAN_DEVICE == _device_b, READ_ROUTE);
        !sync_res) {
        // Rollback the failure to update the header
        __set_read_route(old_route);
        _sb->fields.bitmap.age = old_age;
        _is_degraded.clear(std::memory_order_release);
        RLOGE("Could not become degraded [uuid:{}]: {}", _str_uuid, sync_res.error().message())
        return sync_res;
    }
    DIRTY_DEVICE->unavail.test_and_set(std::memory_order_acquire);
    if (spawn_resync) __resume_resync();
    return 0;
}

void Raid1DiskImpl::__clean_region(uint64_t addr, uint32_t len) {
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("Cleaning pages for [lba:{:#0x}|len:{:#0x}] [uuid:{}]", lba, len, _str_uuid);

    auto const pg_size = _dirty_bitmap->page_size();
    auto iov = iovec{.iov_base = nullptr, .iov_len = pg_size};

    auto const end = addr + len;
    auto cur_off = addr;
    while (end > cur_off) {
        auto [page, pg_offset, sz] = _dirty_bitmap->clean_region(cur_off, end - cur_off);
        cur_off += sz;
        if (!page) continue;
        iov.iov_base = page;

        auto const page_addr = (pg_size * pg_offset) + pg_size;

        // These don't actually need to succeed; this page will remain dirty and loaded the next time
        // we use this bitmap (extra copies for this page).
        if (auto res = CLEAN_DEVICE->disk->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, page_addr); !res) {
            RLOGW("Failed to clear bitmap page. [uuid:{}]", _str_uuid)
        }
    }
}

// Failed Async WRITEs all end up here and have the side-effect of dirtying the BITMAP
// on the working device. This blocks the final result going back from the original operation
// as we chain additional sub_cmds by returning a value > 0 including a new "result" for the
// original sub_cmd
io_result Raid1DiskImpl::__handle_async_retry(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                              ublk_io_data const* async_data) {
    // No Synchronous operations retry
    DEBUG_ASSERT_NOTNULL(async_data, "Retry on an synchronous I/O!"); // LCOV_EXCL_LINE

    // Record this degraded operation in the bitmap, then unblock resync_task (if exists)
    _dirty_bitmap->dirty_region(addr, len);
    DEQUEUE_WRITE_OP

    if (IS_DEGRADED && CLEAN_SUBCMD == sub_cmd) {
        // If we're already degraded and failure was on CLEAN disk then treat this as a fatal
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
                                     ublksrv_queue const* q, ublk_io_data const* async_data) {
    // Apply our shift to the sub_cmd if it's not a replica write
    auto const replica_write = is_replicate(sub_cmd);
    if (!replica_write) {
        sub_cmd = shift_route(sub_cmd, route_size());
        sub_cmd = CLEAN_SUBCMD;
        // Track outstanding writes for resync coordination
        // If this is the first outstanding write, pause resync
    }

    // Sync IOVs have to track this differently as the do not receive on_io_complete
    if (async_data) ENQUEUE_WRITE_OP
    auto res = func(*(CLEAN_SUBCMD == sub_cmd ? CLEAN_DEVICE->disk : DIRTY_DEVICE->disk), sub_cmd);

    // If not-degraded and sub_cmd failed immediately, dirty bitmap and return result of op on alternate-path
    if (!res) {
        if (async_data) DEQUEUE_WRITE_OP
        if (IS_DEGRADED && !replica_write) {
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
        if (async_data) ENQUEUE_WRITE_OP
        if (res = func(*CLEAN_DEVICE->disk, CLEAN_SUBCMD); !res) {
            if (async_data) DEQUEUE_WRITE_OP
            return res;
        }
        return res.value() + dirty_res.value();
    }
    if (replica_write) return res;

    // If the address or length are not entirely aligned by the chunk size and there are dirty bits, then try
    // and dirty more pages, the recovery strategy will need to correct this later
    if (IS_DEGRADED) {
        if (auto dirty_unavail = DIRTY_DEVICE->unavail.test(std::memory_order_acquire);
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
    sub_cmd = DIRTY_SUBCMD;
    sub_cmd = set_flags(sub_cmd, sub_cmd_flags::REPLICATE);
    auto replica_res = __replicate(sub_cmd, std::move(func), addr, len, q, async_data);
    if (!replica_res) return replica_res;
    res = res.value() += replica_res.value();
    // Assuming all was successful, return the aggregate of the results
    return res;
}

io_result Raid1DiskImpl::__failover_read(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len) {
    auto const retry = is_retry(sub_cmd);
    if (retry) {
        _last_read = (0b1 & ((sub_cmd) >> _device_b->disk->route_size())) ? read_route::DEVB : read_route::DEVA;
    } else
        sub_cmd = shift_route(sub_cmd, route_size());

    // Pick a device to read from
    auto route = read_route::DEVA;
    auto need_to_test{false};
    if (IS_DEGRADED && (!retry && DIRTY_DEVICE->unavail.test(std::memory_order_acquire))) {
        route = READ_ROUTE;
    } else {
        if (read_route::DEVB == _last_read) {
            if (read_route::DEVB == READ_ROUTE) need_to_test = true;
        } else {
            route = read_route::DEVB;
            if (read_route::DEVA == READ_ROUTE) need_to_test = true;
        }
    }

    // We allow READing from a degraded device if the bitmap does not indicate the chunk is dirty
    if (IS_DEGRADED && need_to_test && _dirty_bitmap->is_dirty(addr, len))
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

io_result Raid1DiskImpl::handle_internal(ublksrv_queue const*, ublk_io_data const* data, sub_cmd_t sub_cmd,
                                         iovec* iovecs, uint32_t nr_vecs, uint64_t addr, int res) {
    sub_cmd = unset_flags(sub_cmd, sub_cmd_flags::INTERNAL);
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    auto const lba = addr >> params()->basic.logical_bs_shift;

    if (UBLK_IO_OP_READ == ublksrv_get_op(data->iod)) {
        DIRTY_DEVICE->unavail.clear(std::memory_order_release);
        return io_result(0);
    }
    DEQUEUE_WRITE_OP
    if (0 == res) {
        RLOGI("Cleared {:#0x}Ki Inline! @ lba:{:#0x} [uuid:{}]", len / Ki, lba, _str_uuid)
        DIRTY_DEVICE->unavail.clear(std::memory_order_release);
        __clean_region(addr, len);
    } else {
        RLOGW("Dirtied: {:#0x}Ki Inline! @ lba:{:#0x} [uuid:{}]", len / Ki, lba, _str_uuid)
        _dirty_bitmap->dirty_region(addr, len);
    }
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

    ENQUEUE_WRITE_OP
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
    DEQUEUE_WRITE_OP

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
    // success or a failed retry which we will not re-enqueue.
    // Error cases will be handled by handle_internal and __handle_async_retry
    // as they need to dirty the BITMAP prior to unblocking the resync task.
    if (UBLK_IO_OP_READ != ublksrv_get_op(data->iod) && (0 == res || is_retry(sub_cmd))) { DEQUEUE_WRITE_OP }
}

// Pause an ongoing resync task (spin while ACTIVE) by moving to SLEEPING
void Raid1DiskImpl::__pause_resync() {
    using namespace std::chrono_literals;
    auto cur_state = static_cast< uint8_t >(resync_state::SLEEPING);
    while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::PAUSE))) {
        if ((static_cast< uint8_t >(resync_state::PAUSE) == cur_state) ||
            (static_cast< uint8_t >(resync_state::STOPPED) == cur_state))
            break;
        if (static_cast< uint8_t >(resync_state::ACTIVE) == cur_state)
            cur_state = static_cast< uint8_t >(resync_state::SLEEPING);
        else if (static_cast< uint8_t >(resync_state::IDLE) == cur_state)
            continue;
        // Sleep a little since the resync thread is actively reading/writing
        std::this_thread::sleep_for(k_state_spin_time);
    }
}

// Resume any on-going resync by moving to IDLE
void Raid1DiskImpl::__resume_resync() {
    auto cur_state = static_cast< uint8_t >(resync_state::PAUSE);
    // If we went from PAUSE->IDLE then a task is running already!
    if (_resync_state.compare_exchange_strong(cur_state, static_cast< uint8_t >(resync_state::IDLE))) return;
    // If we were already IDLE then we need to spawn a task ourselves
    // Also handle STOPPED state (from __stop_resync during swap_device)
    if ((static_cast< uint8_t >(resync_state::IDLE) == cur_state) ||
        (static_cast< uint8_t >(resync_state::STOPPED) == cur_state)) {
        // Cleanup any finished resync tasks
        if (_resync_task.joinable()) _resync_task.join();
        // Atomically transition STOPPED->IDLE before spawning (if needed)
        if (static_cast< uint8_t >(resync_state::STOPPED) == cur_state) {
            auto stopped = static_cast< uint8_t >(resync_state::STOPPED);
            if (!_resync_state.compare_exchange_strong(stopped, static_cast< uint8_t >(resync_state::IDLE))) {
                // State changed between check and CAS - someone else changed it, bail out
                return;
            }
        }
        if (RUNNING_DEFUNCT || !_resync_enabled) return;
        _resync_task = sisl::named_thread(fmt::format("r_{}", _str_uuid.substr(0, 13)), [this] { __resync_task(); });
    }
    DEBUG_ASSERT_NE(static_cast< uint8_t >(resync_state::SLEEPING), cur_state,
                    "Should never find Sleeping Resync here. *BUG*!");
}

// Abort any on-going resync task by moving to STOPPED and rejoin the thread
void Raid1DiskImpl::__stop_resync() {
    // Terminate any ongoing resync task
    auto cur_state = static_cast< uint8_t >(resync_state::PAUSE);
    while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::STOPPED))) {
        if (static_cast< uint8_t >(resync_state::STOPPED) == cur_state) break;
        if (static_cast< uint8_t >(resync_state::ACTIVE) == cur_state) {
            cur_state = static_cast< uint8_t >(resync_state::SLEEPING);
            std::this_thread::sleep_for(k_state_spin_time);
        }
    }
    if (_resync_task.joinable()) _resync_task.join();
}

resync_state Raid1DiskImpl::__yield_resync(std::chrono::microseconds const yield_for,
                                           std::chrono::microseconds const spin_time) {
    auto cur_state = static_cast< uint8_t >(resync_state::ACTIVE);
    // Give I/O a chance to interrupt resync
    while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::SLEEPING))) {
        if (static_cast< uint8_t >(resync_state::STOPPED) == cur_state) return static_cast< resync_state >(cur_state);
    }
    cur_state = static_cast< uint8_t >(resync_state::SLEEPING);

    // Yield to the I/O threads; but allow early bail out if we're stopped
    auto const end_time = std::chrono::steady_clock::now() + yield_for;
    while (end_time > std::chrono::steady_clock::now()) {
        std::this_thread::sleep_for(spin_time);
        if (static_cast< uint8_t >(resync_state::STOPPED) == _resync_state.load(std::memory_order_acquire)) break;
    }

    // Resume resync after short delay
    while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::ACTIVE))) {
        if (static_cast< uint8_t >(resync_state::PAUSE) == cur_state) {
            cur_state = static_cast< uint8_t >(resync_state::IDLE);
            std::this_thread::sleep_for(yield_for);
        } else if (static_cast< uint8_t >(resync_state::STOPPED) == cur_state)
            return static_cast< resync_state >(cur_state);
    }
    return resync_state::ACTIVE;
}

void Raid1DiskImpl::toggle_resync(bool t) {
    _resync_enabled = t;
    if (t) return __resume_resync();
    __pause_resync();
}

} // namespace raid1

} // namespace ublkpp
