#include "ublkpp/raid/raid1.hpp"

#include <set>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <ublksrv.h>
#include <ublksrv_utils.h>
#include <sisl/options/options.h>

#include "bitmap.hpp"
#include "raid1_impl.hpp"
#include "lib/logging.hpp"

SISL_OPTION_GROUP(raid1,
                  (chunk_size, "", "chunk_size", "The desired chunk_size for new Raid1 devices",
                   cxxopts::value< std::uint32_t >()->default_value("32768"), "<io_size>"),
                  (no_read_from_dirty, "", "no_read_from_dirty", "Allow reads from a Dirty device",
                   cxxopts::value< bool >(), ""),
                  (no_write_to_dirty, "", "no_write_to_dirty", "Allow writes to a Dirty device",
                   cxxopts::value< bool >(), ""))

using namespace std::chrono_literals;

namespace ublkpp {
using raid1::read_route;

ENUM(resync_state, uint8_t, IDLE = 0, ACTIVE = 1, SLEEPING = 2, PAUSE = 3, STOPPED = 4);

// SubCmd decoders
#define SEND_TO_A (sub_cmd & ((1U << sqe_tgt_data_width) - 2))
#define SEND_TO_B (sub_cmd | 0b1)

// Route routines
#define IS_DEGRADED (_is_degraded.test(std::memory_order_acquire))
#define READ_ROUTE static_cast< read_route >(_sb->fields.read_route)
#define CLEAN_DEVICE (read_route::DEVB == READ_ROUTE ? _device_b : _device_a)
#define DIRTY_DEVICE (read_route::DEVB == READ_ROUTE ? _device_a : _device_b)
#define CLEAN_SUBCMD (read_route::DEVB == READ_ROUTE ? SEND_TO_B : SEND_TO_A)
#define DIRTY_SUBCMD (read_route::DEVB == READ_ROUTE ? SEND_TO_A : SEND_TO_B)
#define DEV_SUBCMD(device) (_device_a->disk == (device) ? SEND_TO_A : SEND_TO_B)

static io_result write_superblock(UblkDisk& device, raid1::SuperBlock* sb, bool device_b);
static folly::Expected< std::pair< raid1::SuperBlock*, bool >, std::error_condition >
load_superblock(UblkDisk& device, boost::uuids::uuid const& uuid, uint32_t const chunk_size);

namespace raid1 {

struct free_page {
    void operator()(void* x) { free(x); }
};

struct MirrorDevice {
    MirrorDevice(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > device);
    std::shared_ptr< UblkDisk > const disk;
    std::shared_ptr< raid1::SuperBlock > sb;
    std::atomic_flag unavail;

    bool new_device{false};
};

MirrorDevice::MirrorDevice(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > device) :
        disk(std::move(device)) {
    auto chunk_size = SISL_OPTIONS["chunk_size"].as< uint32_t >();
    if ((raid1::k_min_chunk_size > chunk_size) || (raid1::k_max_dev_size < chunk_size)) {
        RLOGE("Invalid chunk_size: {}KiB [min:{}KiB]", chunk_size / Ki, raid1::k_min_chunk_size / Ki) // LCOV_EXCL_START
        throw std::runtime_error("Invalid Chunk Size");
    } // LCOV_EXCL_STOP

    auto read_super = load_superblock(*disk, uuid, chunk_size);
    if (!read_super)
        throw std::runtime_error(fmt::format("Could not read superblock! {}", read_super.error().message()));
    new_device = read_super.value().second;
    sb = std::shared_ptr< raid1::SuperBlock >(read_super.value().first, free_page());
}

Raid1DiskImpl::Raid1DiskImpl(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a,
                             std::shared_ptr< UblkDisk > dev_b) :
        UblkDisk(), _uuid(uuid), _str_uuid(boost::uuids::to_string(uuid)) {
    direct_io = true; // RAID-1 requires DIO

    // We enqueue async responses for RAID1 retries even if our underlying devices use uring
    uses_ublk_iouring = false;

    // Discover overall Device parameters
    auto& our_params = *params();
    our_params.types |= UBLK_PARAM_TYPE_DISCARD;
    for (auto device_array = std::set< std::shared_ptr< UblkDisk > >{dev_a, dev_b}; auto const& device : device_array) {
        if (!device->direct_io) throw std::runtime_error(fmt::format("Device does not support O_DIRECT! {}", device));
        our_params.basic.dev_sectors = std::min(our_params.basic.dev_sectors, device->params()->basic.dev_sectors);
        our_params.basic.logical_bs_shift =
            std::max(our_params.basic.logical_bs_shift, device->params()->basic.logical_bs_shift);
        our_params.basic.physical_bs_shift =
            std::max(our_params.basic.physical_bs_shift, device->params()->basic.physical_bs_shift);

        if (!device->can_discard()) our_params.types &= ~UBLK_PARAM_TYPE_DISCARD;
    }
    // Reserve space for the superblock/bitmap
    RLOGD("RAID-1 : reserving {} blocks for SuperBlock & Bitmap",
          raid1::reserved_size >> our_params.basic.logical_bs_shift)
    our_params.basic.dev_sectors -= (raid1::reserved_size >> SECTOR_SHIFT);
    if (our_params.basic.dev_sectors > (raid1::k_max_dev_size >> SECTOR_SHIFT)) {
        RLOGW("Device would be larger than supported, only exposing [{}Gi] sized device", raid1::k_max_dev_size / Gi)
        our_params.basic.dev_sectors = (raid1::k_max_dev_size >> SECTOR_SHIFT);
    }
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

    // Let's check the BITMAP uuids if either device is new
    if ((!_device_a->new_device && !_device_b->new_device) &&
        (0 !=
         memcmp(_device_a->sb->fields.bitmap.uuid, _device_b->sb->fields.bitmap.uuid,
                sizeof(_sb->fields.bitmap.uuid)))) {
        RLOGE("Devices do not belong to the same RAID-1 device!");
        throw std::runtime_error("Devices do not belong to the same RAID-1 device.");
    }

    // We only keep the latest or if match and A unclean take B, if age diff is > 1 consider new
    if (auto sb_res = pick_superblock(_device_a->sb.get(), _device_b->sb.get()); sb_res) {
        if (sb_res == _device_a->sb.get()) {
            _sb = std::move(_device_a->sb);
            if (1 < (be64toh(_sb->fields.bitmap.age) - be64toh(_device_b->sb->fields.bitmap.age)))
                _device_b->new_device = true;
        } else {
            _sb = std::move(_device_b->sb);
            if (1 < (be64toh(_sb->fields.bitmap.age) - be64toh(_device_a->sb->fields.bitmap.age)))
                _device_a->new_device = true;
        }
    } else
        throw std::runtime_error("Could not find reasonable superblock!"); // LCOV_EXCL_LINE

    // Initialize those that are new
    if (_device_a->new_device && _device_b->new_device) {
        // Generate a new random UUID for the BITMAP that we'll use to protected ourselves on re-assembly
        auto const bitmap_uuid = boost::uuids::random_generator()();
        RLOGD("Generated new BITMAP: {} [vol:{}]", boost::uuids::to_string(bitmap_uuid), _str_uuid);
        memcpy(_sb->fields.bitmap.uuid, bitmap_uuid.data, sizeof(_sb->fields.bitmap.uuid));
        _sb->fields.bitmap.age = htobe64(1);
    }

    // Read in existing dirty BITMAP pages
    _dirty_bitmap = std::make_unique< raid1::Bitmap >(capacity(), be32toh(_sb->fields.bitmap.chunk_size), block_size());
    if (_device_a->new_device) {
        _dirty_bitmap->init_to(*_device_a->disk);
        if (!_device_b->new_device) _sb->fields.read_route = static_cast< uint8_t >(read_route::DEVB);
    }
    if (_device_b->new_device) {
        _dirty_bitmap->init_to(*_device_b->disk);
        if (!_device_a->new_device) _sb->fields.read_route = static_cast< uint8_t >(read_route::DEVA);
    }

    // We need to completely dirty one side if either is new when the other is not
    sub_cmd_t const sub_cmd = 0U;
    if ((_device_a->new_device xor _device_b->new_device) || 0 == _sb->fields.clean_unmount) {
        // Bump the bitmap age
        _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 16);
        RLOGW("Device is new [{}], dirty all of device [{}]",
              *(_device_a->new_device ? _device_a->disk : _device_b->disk),
              *(_device_a->new_device ? _device_b->disk : _device_a->disk))
        __dirty_pages(CLEAN_SUBCMD, 0, capacity(), nullptr, nullptr);
        _is_degraded.test_and_set(std::memory_order_relaxed);
    } else if (read_route::EITHER != READ_ROUTE) {
        RLOGW("Raid1 is starting in degraded mode [vol:{}]! Degraded device: [{}]", _str_uuid, *DIRTY_DEVICE->disk)
        _is_degraded.test_and_set(std::memory_order_relaxed);
        _dirty_bitmap->load_from(*CLEAN_DEVICE->disk);
    }

    // We mark the SB dirty here and clean in our destructor so we know if we _crashed_ at some instance later
    _sb->fields.clean_unmount = 0x0;
    _sb->fields.device_b = 0;
    _resync_state.store(static_cast< uint8_t >(resync_state::PAUSE));

    // If we Fail to write the SuperBlock to then CLEAN device we immediately dirty the bitmap and try to write to
    // DIRTY
    if (!write_superblock(*CLEAN_DEVICE->disk, _sb.get(), read_route::DEVB == READ_ROUTE)) {
        RLOGE("Failed writing SuperBlock to: [{}] becoming degraded. [vol:{}]", *CLEAN_DEVICE->disk, _str_uuid)
        // If already degraded this is Fatal
        if (IS_DEGRADED) { throw std::runtime_error(fmt::format("Could not initialize superblocks!")); }
        // This will write the SB to DIRTY so we can skip this down below
        if (!__become_degraded(CLEAN_SUBCMD)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
        return;
    }

    if (write_superblock(*DIRTY_DEVICE->disk, _sb.get(), read_route::DEVB != READ_ROUTE)) {
        // If we're starting degraded, we need to initiate a resync_task
        if (IS_DEGRADED) _resync_task = std::thread([this] { __resync_task(); });
    } else if (!__become_degraded(DIRTY_SUBCMD)) {
        throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
    }
}

Raid1DiskImpl::~Raid1DiskImpl() {
    RLOGD("Shutting down; [vol:{}]", _str_uuid)
    auto cur_state = static_cast< uint8_t >(resync_state::PAUSE);
    while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::STOPPED))) {
        if (static_cast< uint8_t >(resync_state::ACTIVE) == cur_state)
            cur_state = static_cast< uint8_t >(resync_state::SLEEPING);
    }
    if (_resync_task.joinable()) _resync_task.join();
    if (!_sb) return;
    // Write out our dirty bitmap
    if (IS_DEGRADED) {
        if (auto res = _dirty_bitmap->sync_to(*CLEAN_DEVICE->disk); !res) {
            RLOGW("Could not sync Bitmap to device on shutdown, will require full resync next time! [vol:{}]",
                  _str_uuid)
            return;
        }
    }
    _sb->fields.clean_unmount = 0x1;
    // Only update the superblock to clean devices
    if (auto res = write_superblock(*CLEAN_DEVICE->disk, _sb.get(), read_route::DEVB == READ_ROUTE); !res) {
        if (IS_DEGRADED) {
            RLOGE("Failed to clear clean bit...full sync required upon next assembly [vol:{}]", _str_uuid)
        } else {
            RLOGW("Failed to clear clean bit [vol:{}] dev: {}", _str_uuid, *CLEAN_DEVICE->disk)
        }
    }
    if (!IS_DEGRADED && !write_superblock(*DIRTY_DEVICE->disk, _sb.get(), read_route::DEVB != READ_ROUTE)) {
        RLOGW("Failed to clear clean bit [vol:{}] dev: {}", _str_uuid, *DIRTY_DEVICE->disk)
    }
}

std::shared_ptr< UblkDisk > Raid1DiskImpl::swap_device(std::string const& old_device_id,
                                                       std::shared_ptr< UblkDisk > new_device) {
    if (!new_device->direct_io) return new_device;
    auto& our_params = *params();
    if (our_params.basic.dev_sectors > new_device->params()->basic.dev_sectors ||
        our_params.basic.logical_bs_shift < new_device->params()->basic.logical_bs_shift) {
        RLOGE("Refusing to use device, requires: [lbs<={} && cap>={}Ki]!", 1 << our_params.basic.logical_bs_shift,
              (our_params.basic.dev_sectors << SECTOR_SHIFT) / Ki)
        return new_device;
    }

    if (IS_DEGRADED && CLEAN_DEVICE->disk->id() == old_device_id) {
        RLOGE("Refusing to replace working mirror from degraded device!")
        return new_device;
    }

    // Write the superblock and clear the BITMAP region of the new device before usage
    std::shared_ptr< MirrorDevice > new_mirror;
    try {
        new_mirror = std::make_shared< MirrorDevice >(_uuid, new_device);
        _dirty_bitmap->init_to(*new_mirror->disk);
    } catch (std::runtime_error const& e) { return new_device; }

    // Terminate any ongoing resync task
    auto cur_state = static_cast< uint8_t >(resync_state::PAUSE);
    while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::STOPPED))) {
        if (static_cast< uint8_t >(resync_state::STOPPED) == cur_state)
            return new_device;
        else if (static_cast< uint8_t >(resync_state::ACTIVE) == cur_state)
            cur_state = static_cast< uint8_t >(resync_state::SLEEPING);
    }
    cur_state = static_cast< uint8_t >(resync_state::STOPPED);
    if (_resync_task.joinable()) _resync_task.join();
    _is_degraded.clear(std::memory_order_release);

    // Write the superblock to the new device and advance the age to make the old device invalid
    auto const old_age = _sb->fields.bitmap.age;
    _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 16);
    if (_device_a->disk->id() == old_device_id) {
        if (auto res = write_superblock(*new_mirror->disk, _sb.get(), false); !res || !__become_degraded(0U, false)) {
            _sb->fields.bitmap.age = old_age;
            return new_device;
        }
        _device_a.swap(new_mirror);
    } else if (_device_b->disk->id() == old_device_id) {
        if (auto res = write_superblock(*new_mirror->disk, _sb.get(), true);
            !res || !__become_degraded(1U << _device_b->disk->route_size(), false)) {
            _sb->fields.bitmap.age = old_age;
            return new_device;
        }
        _device_b.swap(new_mirror);
    }

    // TODO what's the right thing to do here if this fails?
    __dirty_pages(0U, 0, capacity(), nullptr, nullptr);

    // Now set back to IDLE state and kick a resync task off
    _resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::IDLE));
    _resync_task = std::thread([this] {
        std::this_thread::sleep_for(1s); // This makes unit testing more deterministic
        __resync_task();
    });

    return new_mirror->disk;
}

std::pair< replica_state, replica_state > Raid1DiskImpl::replica_states() const {
    auto const cur_sync_state = replica_state::ERROR;
    switch (READ_ROUTE) {
    case read_route::DEVA:
        return std::make_pair(replica_state::CLEAN, cur_sync_state);
    case read_route::DEVB:
        return std::make_pair(cur_sync_state, replica_state::CLEAN);
    case read_route::EITHER:
    default:
        return std::make_pair(replica_state::CLEAN, replica_state::CLEAN);
    }
}

std::pair< std::shared_ptr< UblkDisk >, std::shared_ptr< UblkDisk > > Raid1DiskImpl::replicas() const {
    return std::make_pair(_device_a->disk, _device_b->disk);
}

std::list< int > Raid1DiskImpl::open_for_uring(int const iouring_device_start) {
    auto fds = (_device_a->disk->open_for_uring(iouring_device_start));
    fds.splice(fds.end(), _device_b->disk->open_for_uring(iouring_device_start + fds.size()));
    return fds;
}

io_result Raid1DiskImpl::__become_clean() {
    if (!IS_DEGRADED) return 0;
    RLOGW("Device becoming clean [{}] [vol:{}] ", *DIRTY_DEVICE->disk, _str_uuid)
    _sb->fields.read_route = static_cast< uint8_t >(read_route::EITHER);
    if (auto sync_res = write_superblock(*_device_a->disk, _sb.get(), false); !sync_res) {
        RLOGW("Could not become clean [vol:{}]: {}", _str_uuid, sync_res.error().message())
    }
    if (auto sync_res = write_superblock(*_device_b->disk, _sb.get(), true); !sync_res) {
        RLOGW("Could not become clean [vol:{}]: {}", _str_uuid, sync_res.error().message())
    }
    _is_degraded.clear(std::memory_order_release);
    return 0;
}

static inline io_result __copy_chunks(iovec* iovec, int nr_vecs, uint64_t addr, auto& src, auto& dest) {
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

void Raid1DiskImpl::__resync_task() {
    RLOGD("Resync Task created for [vol:{}]", _str_uuid)
    auto cur_state = static_cast< uint8_t >(resync_state::IDLE);
    // Wait to become IDLE
    while (IS_DEGRADED &&
           !_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::ACTIVE))) {
        // If we're stopped or another task was started we should exit
        if ((static_cast< uint8_t >(resync_state::STOPPED) == cur_state) ||
            (static_cast< uint8_t >(resync_state::ACTIVE) == cur_state) ||
            (static_cast< uint8_t >(resync_state::SLEEPING) == cur_state)) {
            RLOGD("Resync Task aborted for [vol:{}] state: {}", _str_uuid, cur_state)
            return;
        }
        cur_state = static_cast< uint8_t >(resync_state::IDLE);
        std::this_thread::sleep_for(1s);
    }
    cur_state = static_cast< uint8_t >(resync_state::ACTIVE);

    // Set ourselves up with a buffer to do all the read/write operations from
    auto iov = iovec{.iov_base = nullptr, .iov_len = 0};
    if (auto err = ::posix_memalign(&iov.iov_base, block_size(), params()->basic.max_sectors << SECTOR_SHIFT);
        0 != err || nullptr == iov.iov_base) [[unlikely]] { // LCOV_EXCL_START
        RLOGE("Could not allocate memory for I/O: {}", strerror(err))
        while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::IDLE)))
            ;
        return;
    } // LCOV_EXCL_STOP

    [&] {
        while (IS_DEGRADED) {
            auto [logical_off, sz] = _dirty_bitmap->next_dirty();
            if (0 == sz) break;
            iov.iov_len = std::min(sz, params()->basic.max_sectors << SECTOR_SHIFT);

            RLOGT("Copying lba: {:0x} for {}KiB cap:{}", logical_off >> params()->basic.logical_bs_shift,
                  iov.iov_len / Ki, capacity())
            // Copy Chunk from CLEAN to DIRTY
            if (auto res = __copy_chunks(&iov, 1, logical_off + raid1::reserved_size, *CLEAN_DEVICE->disk,
                                         *DIRTY_DEVICE->disk);
                res) {
                // Clear Bitmap and set device as available if successful
                DIRTY_DEVICE->unavail.clear(std::memory_order_release);
                __clean_pages(0, logical_off, iov.iov_len, nullptr, nullptr);
            } else
                DIRTY_DEVICE->unavail.test_and_set(std::memory_order_acquire);

            // Give I/O a chance to interrupt resync
            while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::SLEEPING))) {
                if (static_cast< uint8_t >(resync_state::STOPPED) == cur_state) return;
            }
            cur_state = static_cast< uint8_t >(resync_state::SLEEPING);
            // Give time for degraded device to become available again
            std::this_thread::sleep_for(DIRTY_DEVICE->unavail.test(std::memory_order_acquire) ? 5s : 10us);

            // Resume resync after short delay
            while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::ACTIVE))) {
                if (static_cast< uint8_t >(resync_state::PAUSE) == cur_state) {
                    cur_state = static_cast< uint8_t >(resync_state::IDLE);
                    std::this_thread::sleep_for(1s);
                } else if (static_cast< uint8_t >(resync_state::STOPPED) == cur_state)
                    return;
            }
            cur_state = static_cast< uint8_t >(resync_state::ACTIVE);
        }
    }();
    free(iov.iov_base);
    if (static_cast< uint8_t >(resync_state::STOPPED) != cur_state) {
        if (IS_DEGRADED && 0 == _dirty_bitmap->dirty_pages()) __become_clean();
        while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::IDLE))) {
            if (static_cast< uint8_t >(resync_state::STOPPED) == cur_state) return;
        }
    } else
        RLOGD("Resync Task Stopped for [vol:{}]", _str_uuid)
}

io_result Raid1DiskImpl::__become_degraded(sub_cmd_t sub_cmd, bool spawn_resync) {
    // We only update the AGE if we're not degraded already
    if (_is_degraded.test_and_set(std::memory_order_acquire)) return 0;
    auto const orig_route = _sb->fields.read_route;
    _sb->fields.read_route = (0b1 & ((sub_cmd) >> _device_b->disk->route_size()))
        ? static_cast< uint8_t >(read_route::DEVA)
        : static_cast< uint8_t >(read_route::DEVB);
    auto const old_age = _sb->fields.bitmap.age;
    _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 1);
    RLOGW("Device became degraded [{}] [age:{}] [vol:{}] ", *DIRTY_DEVICE->disk,
          static_cast< uint64_t >(be64toh(_sb->fields.bitmap.age)), _str_uuid);
    // Must update age first; we do this synchronously to gate pending retry results
    if (auto sync_res = write_superblock(*CLEAN_DEVICE->disk, _sb.get(), read_route::DEVB == READ_ROUTE); !sync_res) {
        // Rollback the failure to update the header
        _sb->fields.read_route = static_cast< uint8_t >(orig_route);
        _sb->fields.bitmap.age = old_age;
        _is_degraded.clear(std::memory_order_release);
        RLOGE("Could not become degraded [vol:{}]: {}", _str_uuid, sync_res.error().message())
        return sync_res;
    }
    DIRTY_DEVICE->unavail.test_and_set(std::memory_order_acquire);
    if (spawn_resync) {
        if (_resync_task.joinable()) _resync_task.join();
        _resync_task = std::thread([this] { __resync_task(); });
    }
    return 0;
}

io_result Raid1DiskImpl::__clean_pages(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                       ublk_io_data const* data) {
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("Cleaning pages for [lba:{:0x}|len:{:0x}|sub_cmd:{}] [vol:{}]", lba, len, ublkpp::to_string(sub_cmd),
          _str_uuid);
    auto [page, pg_offset, sz] = _dirty_bitmap->clean_page(addr, len);
    auto res = io_result(0);
    if (page) {
        auto const pg_size = _dirty_bitmap->page_size();
        auto iov = iovec{.iov_base = page, .iov_len = pg_size};
        auto page_addr = (pg_size * pg_offset) + pg_size;

        // These don't actually need to succeed; it's optimistic
        res = data ? CLEAN_DEVICE->disk->async_iov(q, data, CLEAN_SUBCMD, &iov, 1, page_addr)
                   : CLEAN_DEVICE->disk->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, page_addr);
        if (!res) return 0;
        if (!data) res = io_result(0); // We don't need this if sync op
    }

    // We can write across a page boundary; if we detect that we did not consume all the bytes, we need to
    // issue another dirty_page and aggregate its result
    if (sz < len) [[unlikely]] {
        if (auto chained_pg_res = __clean_pages(sub_cmd, addr + sz, len - sz, q, data); chained_pg_res)
            res = res.value() + chained_pg_res.value();
    }

    // MUST Submit here since iov is on the stack!
    if (q && 0 < res.value()) io_uring_submit(q->ring_ptr);

    // We've cleaned the BITMAP!
    if (0 == _dirty_bitmap->dirty_pages()) __become_clean();

    return res;
}

// Generate and submit the BITMAP pages we need to write in order to record the incoming mutations (WRITE/DISCARD)
//
// After some bit-twiddling to manipulate the bits indicated above we asynchronously write that page to the
// corresponding region of the working device. This is added to the sub_cmds that the target requires to complete
// successfully in order to acknowledge the client. It's possible for the offset/length of the operation to require
// that we shift into the next word to set the remaining bits. That's handled here as well, but it is not expected
// that this is unbounded as the bits each represent a significant amount of data. With 32KiB chunks (the minimum) a
// word represents: (64 * 32 * 1024) == 2MiB which is larger than our max I/O for an operation.
void Raid1DiskImpl::__dirty_pages(sub_cmd_t sub_cmd, uint64_t addr, uint64_t len, ublksrv_queue const* q,
                                  ublk_io_data const* data) {
    auto sz = _dirty_bitmap->dirty_page(addr, len);
    RLOGT("Dirty lba: {:0x} for {}KiB cap:{}", addr >> params()->basic.logical_bs_shift, sz / Ki, capacity())

    // We can write across a page boundary; if we detect that we did not consume all the bytes, we need to
    // issue another dirty_page and aggregate its result
    if (sz < len) [[unlikely]]
        __dirty_pages(sub_cmd, addr + sz, len - sz, q, data);
}

// Failed Async WRITEs all end up here and have the side-effect of dirtying the BITMAP
// on the working device. This blocks the final result going back from the original operation
// as we chain additional sub_cmds by returning a value > 0 including a new "result" for the
// original sub_cmd
io_result Raid1DiskImpl::__handle_async_retry(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                              ublk_io_data const* async_data) {
    // No Synchronous operations retry
    DEBUG_ASSERT_NOTNULL(async_data, "Retry on an synchronous I/O!"); // LCOV_EXCL_LINE

    if (IS_DEGRADED && CLEAN_SUBCMD == sub_cmd)
        // If we're already degraded and failure was on CLEAN disk then treat this as a fatal
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));

    // Record this degraded operation in the bitmap, result is # of async writes enqueued
    io_result dirty_res;
    if (dirty_res = __become_degraded(sub_cmd); !dirty_res) return dirty_res;
    __dirty_pages(sub_cmd, addr, len, q, async_data);

    if (is_replicate(sub_cmd)) return dirty_res;

    // Bitmap is marked dirty, queue a new asynchronous "reply" for this original cmd
    _pending_results[q].emplace_back(async_result{async_data, sub_cmd, static_cast< int >(len)});
    if (q) {
        if (0 != ublksrv_queue_send_event(q)) { // LCOV_EXCL_START
            RLOGE("Failed to send event!");
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
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
    }

    auto res = func(*(CLEAN_SUBCMD == sub_cmd ? CLEAN_DEVICE->disk : DIRTY_DEVICE->disk), sub_cmd);

    // If not-degraded and sub_cmd failed immediately, dirty bitmap and return result of op on alternate-path
    if (!res) {
        if (IS_DEGRADED && !replica_write) {
            RLOGE("Double failure! [tag:{:0x},sub_cmd:{}]", async_data->tag, ublkpp::to_string(sub_cmd))
            return res;
        }
        io_result dirty_res;
        if (dirty_res = __become_degraded(sub_cmd); !dirty_res) return dirty_res;
        __dirty_pages(sub_cmd, addr, len, q, async_data);

        if (replica_write) return dirty_res;
        if (res = func(*CLEAN_DEVICE->disk, CLEAN_SUBCMD); !res) return res;
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
            if (dirty_unavail || !totally_aligned || (0 < SISL_OPTIONS["no_write_to_dirty"].as< bool >())) {
                __dirty_pages(sub_cmd, addr, len, q, async_data);
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
    if (IS_DEGRADED &&
        ((!retry && DIRTY_DEVICE->unavail.test(std::memory_order_acquire)) ||
         0 < SISL_OPTIONS["no_read_from_dirty"].count())) {
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
    if (retry && (_last_read == route)) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));

    _last_read = route;

    // Attempt read on device; if it succeeds or we are degraded return the result
    auto device = (read_route::DEVA == route) ? _device_a->disk : _device_b->disk;
    if (auto res = func(*device, DEV_SUBCMD(device)); res || retry) { return res; }

    // Otherwise fail over the device and attempt the READ again marking this a retry
    sub_cmd = set_flags(sub_cmd, sub_cmd_flags::RETRIED);
    return __failover_read(sub_cmd, std::move(func), addr, len);
}

io_result Raid1DiskImpl::handle_internal(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd,
                                         iovec* iovecs, uint32_t nr_vecs, uint64_t addr, int res) {
    sub_cmd = unset_flags(sub_cmd, sub_cmd_flags::INTERNAL);
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);

    if (0 == res) {
        DIRTY_DEVICE->unavail.clear(std::memory_order_release);
        return __clean_pages(sub_cmd, addr, len, q, data);
    }
    __dirty_pages(sub_cmd, addr, len, q, data);
    return io_result(0);
}

void Raid1DiskImpl::idle_transition(ublksrv_queue const*, bool enter) {
    using namespace std::chrono_literals;
    if (enter) {
        auto cur_state = static_cast< uint8_t >(resync_state::PAUSE);
        while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::IDLE))) {
            if (static_cast< uint8_t >(resync_state::PAUSE) != cur_state) return;
        }
        return;
    }
    // To allow I/O wait for resync task to PAUSE (if any running)
    auto cur_state = static_cast< uint8_t >(resync_state::IDLE);
    while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::PAUSE))) {
        if (static_cast< uint8_t >(resync_state::PAUSE) == cur_state)
            break;
        else if (static_cast< uint8_t >(resync_state::ACTIVE) == cur_state)
            cur_state = static_cast< uint8_t >(resync_state::SLEEPING);
        else if (static_cast< uint8_t >(resync_state::STOPPED) == cur_state)
            cur_state = static_cast< uint8_t >(resync_state::IDLE);
        std::this_thread::sleep_for(10us);
    }
}

void Raid1DiskImpl::collect_async(ublksrv_queue const* q, std::list< async_result >& results) {
    results.splice(results.end(), std::move(_pending_results[q]));
    if (!_device_a->disk->uses_ublk_iouring) _device_a->disk->collect_async(q, results);
    if (!_device_b->disk->uses_ublk_iouring) _device_b->disk->collect_async(q, results);
}

io_result Raid1DiskImpl::handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd,
                                        uint32_t len, uint64_t addr) {
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("received DISCARD: [tag:{:0x}] [lba:{:0x}|len:{:0x}] [vol:{}]", data->tag, lba, len, _str_uuid)

    // Stop any on-going resync
    if (static_cast< uint8_t >(resync_state::PAUSE) != _resync_state.load()) idle_transition(q, false);

    if (is_retry(sub_cmd)) [[unlikely]]
        return __handle_async_retry(sub_cmd, addr, len, q, data);

    return __replicate(
        sub_cmd,
        [q, data, len, addr](UblkDisk& d, sub_cmd_t scmd) -> io_result {
            // Discard does not support internal commands, we can safely ignore these optimistic operations
            if (is_internal(scmd)) return 0;
            return d.handle_discard(q, data, scmd, len, addr + raid1::reserved_size);
        },
        addr, len, q, data);
}

io_result Raid1DiskImpl::async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                                   uint32_t nr_vecs, uint64_t addr) {
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    RLOGT("Received {}: [tag:{:0x}] [lba:{:0x}|len:{:0x}] [sub_cmd:{}] [vol:{}]",
          ublksrv_get_op(data->iod) == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag,
          addr >> params()->basic.logical_bs_shift, len, ublkpp::to_string(sub_cmd), _str_uuid)

    // Stop any on-going resync
    if (static_cast< uint8_t >(resync_state::PAUSE) != _resync_state.load()) idle_transition(q, false);

    // READs are a special sub_cmd that just go to one side we'll do explicitly
    if (UBLK_IO_OP_READ == ublksrv_get_op(data->iod))
        return __failover_read(
            sub_cmd,
            [q, data, iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t scmd) {
                return d.async_iov(q, data, scmd, iovecs, nr_vecs, addr + raid1::reserved_size);
            },
            addr, len);

    if (is_retry(sub_cmd)) [[unlikely]]
        return __handle_async_retry(sub_cmd, addr, len, q, data);

    return __replicate(
        sub_cmd,
        [q, data, iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t scmd) {
            return d.async_iov(q, data, scmd, iovecs, nr_vecs, addr + raid1::reserved_size);
        },
        addr, len, q, data);
}

io_result Raid1DiskImpl::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("Received {}: [lba:{:0x}|len:{:0x}] [vol:{}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", lba, len, _str_uuid)

    // Stop any on-going resync
    if (static_cast< uint8_t >(resync_state::PAUSE) != _resync_state.load()) idle_transition(nullptr, false);

    // READs are a special sub_cmd that just go to one side we'll do explicitly
    if (UBLK_IO_OP_READ == op)
        return __failover_read(
            0U,
            [iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t) {
                return d.sync_iov(UBLK_IO_OP_READ, iovecs, nr_vecs, addr + raid1::reserved_size);
            },
            addr, len);

    size_t res{0};
    if (auto io_res = __replicate(
            0U,
            [&res, op, iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t s) {
                auto p_res = d.sync_iov(op, iovecs, nr_vecs, addr + raid1::reserved_size);
                // Noramlly the target handles the result being duplicated for WRITEs, we handle it for sync_io here
                if (p_res && !is_replicate(s)) res += p_res.value();
                return p_res;
            },
            addr, len);
        !io_res)
        return io_res;
    return res;
}

raid1::SuperBlock* pick_superblock(raid1::SuperBlock* dev_a, raid1::SuperBlock* dev_b) {
    if (be64toh(dev_a->fields.bitmap.age) < be64toh(dev_b->fields.bitmap.age)) {
        dev_b->fields.read_route = static_cast< uint8_t >(read_route::DEVB);
        return dev_b;
    } else if (be64toh(dev_a->fields.bitmap.age) > be64toh(dev_b->fields.bitmap.age)) {
        dev_a->fields.read_route = static_cast< uint8_t >(read_route::DEVA);
        return dev_a;
    } else if (dev_a->fields.clean_unmount != dev_b->fields.clean_unmount)
        return dev_a->fields.clean_unmount ? dev_a : dev_b;

    return dev_a;
}

} // namespace raid1

static const uint8_t magic_bytes[16] = {0123, 045, 0377, 012, 064,  0231, 076, 0305,
                                        0147, 072, 0310, 027, 0111, 0256, 033, 0144};

constexpr auto SB_VERSION = 1;

static raid1::SuperBlock* read_superblock(UblkDisk& device) {
    auto const sb_size = sizeof(raid1::SuperBlock);
    RLOGT("Reading Superblock from: [{}] {}%{} == {}", device, sb_size, device.block_size(),
          sb_size % device.block_size())
    DEBUG_ASSERT_EQ(0, sb_size % device.block_size(), "Device [{}] blocksize does not support alignment of [{}B]",
                    device, sb_size)
    auto iov = iovec{.iov_base = nullptr, .iov_len = sb_size};
    if (auto err = ::posix_memalign(&iov.iov_base, device.block_size(), sb_size); 0 != err || nullptr == iov.iov_base)
        [[unlikely]] { // LCOV_EXCL_START
        if (EINVAL == err) RLOGE("Invalid Argument while reading superblock!")
        RLOGE("Out of Memory while reading superblock!")
        return nullptr;
    } // LCOV_EXCL_STOP
    if (auto res = device.sync_iov(UBLK_IO_OP_READ, &iov, 1, 0UL); !res) {
        RLOGE("Could not read SuperBlock of [sz:{}] [res:{}]", sb_size, res.error().message())
        free(iov.iov_base);
        return nullptr;
    }
    return static_cast< raid1::SuperBlock* >(iov.iov_base);
}

static io_result write_superblock(UblkDisk& device, raid1::SuperBlock* sb, bool device_b) {
    auto const sb_size = sizeof(raid1::SuperBlock);
    RLOGT("Writing Superblock to: [{}]", device)
    DEBUG_ASSERT_EQ(0, sb_size % device.block_size(), "Device [{}] blocksize does not support alignment of [{}B]",
                    device, sb_size)
    auto iov = iovec{.iov_base = sb, .iov_len = sb_size};
    // We temporarily set the Superblock for Device A/B based on argument
    if (device_b) sb->fields.device_b = 1;
    auto res = device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 0UL);
    sb->fields.device_b = 0;
    if (!res) RLOGE("Error writing Superblock to: [{}]!", device, res.error().message())
    return res;
}

// Read and load the RAID1 superblock off a device. If it is not set, meaning the Magic is missing, then initialize
// the superblock to the current version. Otherwise migrate any changes needed after version discovery.
static folly::Expected< std::pair< raid1::SuperBlock*, bool >, std::error_condition >
load_superblock(UblkDisk& device, boost::uuids::uuid const& uuid, uint32_t const chunk_size) {
    auto sb = read_superblock(device);
    if (!sb) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    bool was_new{false};
    if (memcmp(sb->header.magic, magic_bytes, sizeof(magic_bytes))) {
        memset(sb, 0x00, raid1::k_page_size);
        memcpy(sb->header.magic, magic_bytes, sizeof(magic_bytes));
        memcpy(sb->header.uuid, uuid.data, sizeof(sb->header.uuid));
        sb->fields.clean_unmount = 1;
        sb->fields.bitmap.chunk_size = htobe32(chunk_size);
        sb->fields.bitmap.age = 0;
        sb->fields.read_route = static_cast< uint8_t >(read_route::EITHER);
        was_new = true;
    }

    // Verify some details in the superblock
    auto read_uuid = boost::uuids::uuid();
    memcpy(read_uuid.data, sb->header.uuid, sizeof(sb->header.uuid));
    if (uuid != read_uuid) {
        RLOGE("Superblock did not have a matching UUID expected: {} read: {}", to_string(uuid), to_string(read_uuid))
        free(sb);
        return folly::makeUnexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    if (chunk_size != be32toh(sb->fields.bitmap.chunk_size)) {
        RLOGW("Superblock was created with different chunk_size: [{}B] will not use runtime config of [{}B] "
              "[vol:{}] ",
              be32toh(sb->fields.bitmap.chunk_size), chunk_size, to_string(uuid))
    }
    RLOGD("device has v{:0x} superblock [age:{},chunk_sz:{:0x},{}] [vol:{}] ", be16toh(sb->header.version),
          be64toh(sb->fields.bitmap.age), chunk_size, (1 == sb->fields.clean_unmount) ? "Clean" : "Dirty",
          to_string(uuid))

    if (SB_VERSION > be16toh(sb->header.version)) { sb->header.version = htobe16(SB_VERSION); }
    return std::make_pair(sb, was_new);
}

/// Raid1Disk Public Class
Raid1Disk::Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a,
                     std::shared_ptr< UblkDisk > dev_b) :
        _impl(std::make_unique< raid1::Raid1DiskImpl >(uuid, dev_a, dev_b)) {
    direct_io = _impl->direct_io;
    uses_ublk_iouring = _impl->uses_ublk_iouring;
}

Raid1Disk::~Raid1Disk() = default;

std::shared_ptr< UblkDisk > Raid1Disk::swap_device(std::string const& old_device_id,
                                                   std::shared_ptr< UblkDisk > new_device) {
    return _impl->swap_device(old_device_id, new_device);
}
std::pair< raid1::replica_state, raid1::replica_state > Raid1Disk::replica_states() const {
    return _impl->replica_states();
}
std::pair< std::shared_ptr< UblkDisk >, std::shared_ptr< UblkDisk > > Raid1Disk::replicas() const {
    return _impl->replicas();
}

uint32_t Raid1Disk::block_size() const { return _impl->block_size(); }
bool Raid1Disk::can_discard() const { return _impl->can_discard(); }
uint64_t Raid1Disk::capacity() const { return _impl->capacity(); }

ublk_params* Raid1Disk::params() { return _impl->params(); }
ublk_params const* Raid1Disk::params() const { return _impl->params(); }
std::string Raid1Disk::id() const { return _impl->id(); }
std::list< int > Raid1Disk::open_for_uring(int const iouring_device) { return _impl->open_for_uring(iouring_device); }
uint8_t Raid1Disk::route_size() const { return _impl->route_size(); }
void Raid1Disk::idle_transition(ublksrv_queue const* q, bool enter) { return _impl->idle_transition(q, enter); }

io_result Raid1Disk::handle_internal(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovec,
                                     uint32_t nr_vecs, uint64_t addr, int res) {
    return _impl->handle_internal(q, data, sub_cmd, iovec, nr_vecs, addr, res);
}
void Raid1Disk::collect_async(ublksrv_queue const* q, std::list< async_result >& compl_list) {
    return _impl->collect_async(q, compl_list);
}
io_result Raid1Disk::handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) {
    return _impl->handle_flush(q, data, sub_cmd);
}
io_result Raid1Disk::handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                                    uint64_t addr) {
    return _impl->handle_discard(q, data, sub_cmd, len, addr);
}
io_result Raid1Disk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                               uint32_t nr_vecs, uint64_t addr) {
    return _impl->async_iov(q, data, sub_cmd, iovecs, nr_vecs, addr);
}
io_result Raid1Disk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept {
    return _impl->sync_iov(op, iovecs, nr_vecs, offset);
}
// ================

} // namespace ublkpp
