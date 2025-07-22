#include "ublkpp/raid/raid1.hpp"

#include <set>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <ublksrv.h>
#include <ublksrv_utils.h>
#include <sisl/options/options.h>

#include "bitmap.hpp"
#include "raid/superblock.hpp"

SISL_OPTION_GROUP(raid1,
                  (chunk_size, "", "chunk_size", "The desired chunk_size for new Raid1 devices",
                   cxxopts::value< std::uint32_t >()->default_value("32768"), "<io_size>"),
                  (no_read_from_dirty, "", "no_read_from_dirty", "Allow reads from a Dirty device",
                   cxxopts::value< bool >(), ""),
                  (no_write_to_dirty, "", "no_write_to_dirty", "Allow writes to a Dirty device",
                   cxxopts::value< bool >(), ""))

namespace ublkpp {
using raid1::read_route;

ENUM(resync_state, uint8_t, IDLE = 0, ACTIVE = 1, SLEEPING = 2, PAUSE = 3);

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
#define DEV_SUBCMD(device) (_device_a == (device) ? SEND_TO_A : SEND_TO_B)

static folly::Expected< std::pair< raid1::SuperBlock*, bool >, std::error_condition >
load_superblock(UblkDisk& device, boost::uuids::uuid const& uuid, uint32_t const chunk_size);

struct free_page {
    void operator()(void* x) { free(x); }
};

Raid1Disk::Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a,
                     std::shared_ptr< UblkDisk > dev_b) :
        UblkDisk(), _str_uuid(boost::uuids::to_string(uuid)), _device_a(std::move(dev_a)), _device_b(std::move(dev_b)) {
    direct_io = true;
    // We enqueue async responses for RAID1 retries even if our underlying devices use uring
    uses_ublk_iouring = false;
    // Discover overall Device parameters
    auto& our_params = *params();
    our_params.types |= UBLK_PARAM_TYPE_DISCARD;
    for (auto device_array = std::set< std::shared_ptr< UblkDisk > >{_device_a, _device_b};
         auto const& device : device_array) {
        if (!device->direct_io) throw std::runtime_error(fmt::format("Device does not support O_DIRECT! {}", device));
        our_params.basic.dev_sectors = std::min(our_params.basic.dev_sectors, device->params()->basic.dev_sectors);
        our_params.basic.logical_bs_shift =
            std::max(our_params.basic.logical_bs_shift, device->params()->basic.logical_bs_shift);
        our_params.basic.physical_bs_shift =
            std::max(our_params.basic.physical_bs_shift, device->params()->basic.physical_bs_shift);

        if (!device->can_discard()) our_params.types &= ~UBLK_PARAM_TYPE_DISCARD;
    }
    // Reserve space for the superblock/bitmap
    RLOGI("RAID-1 : reserving {} blocks for SuperBlock & Bitmap",
          raid1::reserved_size >> our_params.basic.logical_bs_shift)
    our_params.basic.dev_sectors -= (raid1::reserved_size >> SECTOR_SHIFT);
    if (our_params.basic.dev_sectors > (raid1::k_max_dev_size >> SECTOR_SHIFT)) {
        RLOGW("Device would be larger than supported, only exposing [{}Gi] sized device", raid1::k_max_dev_size / Gi)
        our_params.basic.dev_sectors = (raid1::k_max_dev_size >> SECTOR_SHIFT);
    }
    if (can_discard())
        our_params.discard.discard_granularity = std::max(our_params.discard.discard_granularity, block_size());

    auto chunk_size = SISL_OPTIONS["chunk_size"].as< uint32_t >();
    if ((raid1::k_min_chunk_size > chunk_size) || (raid1::k_max_dev_size < chunk_size)) {
        RLOGE("Invalid chunk_size: {}KiB [min:{}KiB]", chunk_size / Ki, raid1::k_min_chunk_size / Ki) // LCOV_EXCL_START
        throw std::runtime_error("Invalid Chunk Size");
    } // LCOV_EXCL_STOP

    auto read_super = load_superblock(*_device_a, uuid, chunk_size);
    if (!read_super)
        throw std::runtime_error(fmt::format("Could not read superblock! {}", read_super.error().message()));
    auto a_new = read_super.value().second;
    auto sb_a = std::shared_ptr< raid1::SuperBlock >(read_super.value().first, free_page());
    read_super = load_superblock(*_device_b, uuid, chunk_size);
    if (!read_super)
        throw std::runtime_error(fmt::format("Could not read superblock! {}", read_super.error().message()));
    auto b_new = read_super.value().second;
    auto sb_b = std::shared_ptr< raid1::SuperBlock >(read_super.value().first, free_page());

    // Let's check the BITMAP uuids if neither devices are new
    if ((!a_new && !b_new) &&
        (0 != memcmp(sb_a->fields.bitmap.uuid, sb_b->fields.bitmap.uuid, sizeof(sb_a->fields.bitmap.uuid)))) {
        RLOGE("Devices do not belong to the same RAID-1 device!");
        throw std::runtime_error("Devices do not belong to the same RAID-1 device.");
    }

    // We only keep the latest or if match and A unclean take B
    if (auto sb_res = pick_superblock(sb_a.get(), sb_b.get()); sb_res) {
        _sb = (sb_res == sb_a.get() ? std::move(sb_a) : std::move(sb_b));
    } else
        throw std::runtime_error("Could not find reasonable superblock!"); // LCOV_EXCL_LINE

    // Initialize those that are new
    if (a_new && b_new) {
        // Generate a new random UUID for the BITMAP that we'll use to protected ourselves on re-assembly
        auto const bitmap_uuid = boost::uuids::random_generator()();
        RLOGD("Generated new BITMAP: {} [vol:{}]", boost::uuids::to_string(bitmap_uuid), _str_uuid);
        memcpy(_sb->fields.bitmap.uuid, bitmap_uuid.data, sizeof(_sb->fields.bitmap.uuid));
        _sb->fields.bitmap.age = htobe64(1);
    }

    // Read in existing dirty BITMAP pages
    _dirty_bitmap = std::make_unique< raid1::Bitmap >(capacity(), be32toh(_sb->fields.bitmap.chunk_size), block_size());
    if (a_new) {
        _dirty_bitmap->init_to(*_device_a);
        if (!b_new) _sb->fields.read_route = static_cast< uint8_t >(read_route::DEVB);
    }
    if (b_new) {
        _dirty_bitmap->init_to(*_device_b);
        if (!a_new) { _sb->fields.read_route = static_cast< uint8_t >(read_route::DEVA); }
    }

    // We need to completely dirty one side if either is new when the other is not
    sub_cmd_t const sub_cmd = 0U;
    if ((a_new != b_new) && (a_new || b_new)) {
        RLOGW("Device is new [{}], dirty all of device [{}]", *(a_new ? _device_a : _device_b),
              *(a_new ? _device_b : _device_a))
        if (auto res = __dirty_pages(CLEAN_SUBCMD, 0, capacity(), nullptr, nullptr); !res)
            throw std::runtime_error(fmt::format("Could not dirty bitmap! {}", res.error().message()));
        _is_degraded.test_and_set(std::memory_order_relaxed);
    } else if (read_route::EITHER != READ_ROUTE) {
        RLOGW("Raid1 is starting in degraded mode [vol:{}]! Degraded device: [{}]", _str_uuid, *DIRTY_DEVICE)
        _is_degraded.test_and_set(std::memory_order_relaxed);
        _dirty_bitmap->load_from(*CLEAN_DEVICE);
    }

    // We mark the SB dirty here and clean in our destructor so we know if we _crashed_ at some instance later
    _sb->fields.clean_unmount = 0x0;
    _resync_state.store(static_cast< uint8_t >(resync_state::PAUSE));

    // If we Fail to write the SuperBlock to then CLEAN device we immediately dirty the bitmap and try to write to
    // DIRTY
    if (!write_superblock(*CLEAN_DEVICE, _sb.get())) {
        RLOGE("Failed writing SuperBlock to: [{}] becoming degraded. [vol:{}]", *CLEAN_DEVICE, _str_uuid)
        // If already degraded this is Fatal
        if (IS_DEGRADED) { throw std::runtime_error(fmt::format("Could not initialize superblocks!")); }
        // This will write the SB to DIRTY so we can skip this down below
        if (!__become_degraded(CLEAN_SUBCMD)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
        return;
    }

    if (write_superblock(*DIRTY_DEVICE, _sb.get())) return;
    RLOGE("Failed writing SuperBlock to: [{}] becoming degraded. [vol:{}] ", *DIRTY_DEVICE, _str_uuid)
    if (!__become_degraded(DIRTY_SUBCMD)) {
        throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
    }
}

Raid1Disk::~Raid1Disk() {
    if (!_sb) return;
    _sb->fields.clean_unmount = 0x1;
    // Only update the superblock to clean devices
    if (auto res = write_superblock(*CLEAN_DEVICE, _sb.get()); !res) {
        if (IS_DEGRADED) {
            RLOGE("failed to clear clean bit...full sync required upon next assembly [vol:{}]", _str_uuid)
        } else {
            RLOGW("failed to clear clean bit [vol:{}]", _str_uuid)
        }
    }
    RLOGD("shutting down array; clean bit set [vol:{}]", _str_uuid)
    if (!IS_DEGRADED) {
        if (!write_superblock(*DIRTY_DEVICE, _sb.get())) { RLOGW("Write clean_unmount failed [vol:{}]", _str_uuid) }
    }
}

std::list< int > Raid1Disk::open_for_uring(int const iouring_device_start) {
    auto fds = (_device_a->open_for_uring(iouring_device_start));
    fds.splice(fds.end(), _device_b->open_for_uring(iouring_device_start + fds.size()));
    return fds;
}

io_result Raid1Disk::__become_clean() {
    RLOGW("Device becoming clean [vol:{}] ", _str_uuid)
    // We only update the AGE if we're not degraded already
    _sb->fields.read_route = static_cast< uint8_t >(read_route::EITHER);
    if (auto sync_res = write_superblock(*CLEAN_DEVICE, _sb.get()); !sync_res) {
        RLOGW("Could not become clean [vol:{}]: {}", _str_uuid, sync_res.error().message())
    }
    if (auto sync_res = write_superblock(*DIRTY_DEVICE, _sb.get()); !sync_res) {
        RLOGW("Could not become clean [vol:{}]: {}", _str_uuid, sync_res.error().message())
    }
    _is_degraded.clear(std::memory_order_release);
    return 0;
}

void Raid1Disk::__resync_task() {
    using namespace std::chrono_literals;
    RLOGW("Resync Task started for [vol:{}]", _str_uuid);
    auto cur_state = static_cast< uint8_t >(resync_state::IDLE);
    while (IS_DEGRADED) {
        // Wait to become IDLE
        while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::ACTIVE))) {
            cur_state = static_cast< uint8_t >(resync_state::IDLE);
            RLOGD("Resync Task Sleeping...")
            std::this_thread::sleep_for(2s);
        }
        while (0 < _dirty_bitmap->dirty_pages()) {
            auto [logical_off, sz] = _dirty_bitmap->next_dirty();
            if (0 == sz) break;
            auto const lba = logical_off >> params()->basic.logical_bs_shift;
            RLOGD("Resync Task will clear [sz:{}KiB|lba:{:x}] from [vol:{}]", sz, lba, _str_uuid)

            // TODO DO SOME WORK
            auto iov = iovec{.iov_base = nullptr, .iov_len = sz};
            if (auto err = ::posix_memalign(&iov.iov_base, block_size(), sz); 0 != err || nullptr == iov.iov_base)
                [[unlikely]] { // LCOV_EXCL_START
                if (EINVAL == err) RLOGE("Invalid Argument while reading superblock!")
                RLOGE("Out of Memory while reading superblock!")
                return;
            } // LCOV_EXCL_STOP
            if (auto res = CLEAN_DEVICE->sync_iov(UBLK_IO_OP_READ, &iov, 1, logical_off + raid1::reserved_size); res) {
                if (auto clear_res =
                        DIRTY_DEVICE->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, logical_off + raid1::reserved_size);
                    !clear_res) {
                    RLOGW("Could not write clean chunks of [sz:{}] [res:{}]", sz, clear_res.error().message());
                } else
                    __clean_pages(0, logical_off, sz, nullptr, nullptr);
            } else {
                RLOGE("Could not read Data of [sz:{}] [res:{}]", sz, res.error().message())
            }
            free(iov.iov_base);

            while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::SLEEPING))) {
                std::this_thread::sleep_for(50ms);
            }
            std::this_thread::sleep_for(5ms);
            while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::ACTIVE))) {
                if (static_cast< uint8_t >(resync_state::PAUSE) == cur_state) {
                    cur_state = static_cast< uint8_t >(resync_state::IDLE);
                    RLOGD("Waiting for IDLE.")
                }
            }
        }
        RLOGW("Resync Completed? Exiting!")
    }
    cur_state = static_cast< uint8_t >(resync_state::ACTIVE);
    while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::IDLE)))
        ;
}

io_result Raid1Disk::__become_degraded(sub_cmd_t sub_cmd) {
    // We only update the AGE if we're not degraded already
    if (_is_degraded.test_and_set(std::memory_order_acquire)) return 0;
    auto const orig_route = _sb->fields.read_route;
    _sb->fields.read_route = (0b1 & ((sub_cmd) >> _device_b->route_size())) ? static_cast< uint8_t >(read_route::DEVA)
                                                                            : static_cast< uint8_t >(read_route::DEVB);
    auto const old_age = _sb->fields.bitmap.age;
    _sb->fields.bitmap.age = htobe64(be64toh(_sb->fields.bitmap.age) + 1);
    RLOGW("Device becoming degraded [sub_cmd:{}] [age:{}] [vol:{}] ", ublkpp::to_string(sub_cmd),
          static_cast< uint64_t >(be64toh(_sb->fields.bitmap.age)), _str_uuid);
    // Must update age first; we do this synchronously to gate pending retry results
    if (auto sync_res = write_superblock(*CLEAN_DEVICE, _sb.get()); !sync_res) {
        // Rollback the failure to update the header
        _sb->fields.read_route = static_cast< uint8_t >(orig_route);
        _sb->fields.bitmap.age = old_age;
        _is_degraded.clear(std::memory_order_release);
        RLOGE("Could not become degraded [vol:{}]: {}", _str_uuid, sync_res.error().message())
        return sync_res;
    }
    std::thread([this] { __resync_task(); }).detach();
    return 0;
}

io_result Raid1Disk::__clean_pages(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                   ublk_io_data const* data) {
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("Cleaning pages for [lba:{:x}|len:{}|sub_cmd:{}] [vol:{}]", lba, len, ublkpp::to_string(sub_cmd), _str_uuid);
    auto [page, pg_offset, sz] = _dirty_bitmap->clean_page(addr, len);
    auto res = io_result(0);
    if (page) {
        auto const pg_size = _dirty_bitmap->page_size();
        auto iov = iovec{.iov_base = page, .iov_len = pg_size};
        auto page_addr = (pg_size * pg_offset) + pg_size;

        // These don't actually need to succeed; it's optimistic
        res = data ? CLEAN_DEVICE->async_iov(q, data, CLEAN_SUBCMD, &iov, 1, page_addr)
                   : CLEAN_DEVICE->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, page_addr);
        if (!res) return 0;
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
io_result Raid1Disk::__dirty_pages(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                   ublk_io_data const* data) {
    // Flag this operation as a required dependencies for the original sub_cmd
    auto new_cmd = set_flags(CLEAN_SUBCMD, sub_cmd_flags::DEPENDENT);

    auto [page, pg_offset, sz] = _dirty_bitmap->dirty_page(addr, len);
    auto res = io_result(0);
    if (page) {
        auto const pg_size = _dirty_bitmap->page_size();
        auto iov = iovec{.iov_base = page, .iov_len = pg_size};
        auto page_addr = (pg_size * pg_offset) + pg_size;

        res = data ? CLEAN_DEVICE->async_iov(q, data, new_cmd, &iov, 1, page_addr)
                   : CLEAN_DEVICE->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, page_addr);
        if (!res) return res;
    }

    // We can write across a page boundary; if we detect that we did not consume all the bytes, we need to
    // issue another dirty_page and aggregate its result
    if (sz < len) [[unlikely]] {
        if (auto chained_pg_res = __dirty_pages(sub_cmd, addr + sz, len - sz, q, data); !chained_pg_res)
            return chained_pg_res;
        else
            res = res.value() + chained_pg_res.value();
    }

    // MUST Submit here since iov is on the stack!
    if (q && 0 < res.value()) io_uring_submit(q->ring_ptr);
    return res;
}

// Failed Async WRITEs all end up here and have the side-effect of dirtying the BITMAP
// on the working device. This blocks the final result going back from the original operation
// as we chain additional sub_cmds by returning a value > 0 including a new "result" for the
// original sub_cmd
io_result Raid1Disk::__handle_async_retry(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                          ublk_io_data const* async_data) {
    // No Synchronous operations retry
    DEBUG_ASSERT_NOTNULL(async_data, "Retry on an synchronous I/O!"); // LCOV_EXCL_LINE

    if (IS_DEGRADED && CLEAN_SUBCMD == sub_cmd)
        // If we're already degraded and failure was on CLEAN disk then treat this as a fatal
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));

    // Record this degraded WRITE in the bitmap, result is # of async writes enqueued
    io_result dirty_res;
    if (dirty_res = __become_degraded(sub_cmd); !dirty_res) return dirty_res;
    dirty_res = __dirty_pages(sub_cmd, addr, len, q, async_data);
    if (!dirty_res) return dirty_res;

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
io_result Raid1Disk::__replicate(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                 ublk_io_data const* async_data) {
    // Apply our shift to the sub_cmd if it's not a replica write
    auto const replica_write = is_replicate(sub_cmd);
    if (!replica_write) {
        sub_cmd = shift_route(sub_cmd, route_size());
        sub_cmd = CLEAN_SUBCMD;
    }

    auto res = func(*(CLEAN_SUBCMD == sub_cmd ? CLEAN_DEVICE : DIRTY_DEVICE), sub_cmd);

    // If not-degraded and sub_cmd failed immediately, dirty bitmap and return result of op on alternate-path
    if (!res) {
        if (IS_DEGRADED && !replica_write) {
            RLOGE("Double failure! [tag:{:x},sub_cmd:{}]", async_data->tag, ublkpp::to_string(sub_cmd))
            return res;
        }
        io_result dirty_res;
        if (dirty_res = __become_degraded(sub_cmd); !dirty_res) return dirty_res;
        dirty_res = __dirty_pages(sub_cmd, addr, len, q, async_data);
        if (!dirty_res) return dirty_res;

        if (replica_write) return dirty_res;
        if (res = func(*CLEAN_DEVICE, CLEAN_SUBCMD); !res) return res;
        return res.value() + dirty_res.value();
    }
    if (replica_write) return res;

    // If we are degraded we need to process this differently depending on the BITMAP state
    if (IS_DEGRADED) {
        auto const chunk_size = be32toh(_sb->fields.bitmap.chunk_size);
        auto const totally_aligned = ((chunk_size <= len) && (0 == len % chunk_size) && (0 == addr % chunk_size));

        // If the address or length are not entirely aligned by the chunk size and there are dirty bits, then try
        // and dirty more pages, the recovery strategy will need to correct this later
        if ((!totally_aligned && _dirty_bitmap->is_dirty(addr, len)) ||
            0 < SISL_OPTIONS["no_write_to_dirty"].as< bool >()) {
            auto dirty_res = __dirty_pages(sub_cmd, addr, len, q, async_data);
            if (!dirty_res) return dirty_res;
            return res.value() + dirty_res.value();
        }
        // We will go ahead and attempt this WRITE on a known degraded device,
        // set this flag so we can clear any bits in the bitmap should is succeed
        sub_cmd = set_flags(sub_cmd, sub_cmd_flags::INTERNAL);
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

io_result Raid1Disk::__failover_read(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len) {
    auto const retry = is_retry(sub_cmd);
    if (retry) {
        _last_read = (0b1 & ((sub_cmd) >> _device_b->route_size())) ? read_route::DEVB : read_route::DEVA;
    } else
        sub_cmd = shift_route(sub_cmd, route_size());

    // Pick a device to read from
    auto route = read_route::DEVA;
    auto need_to_test{false};
    if (IS_DEGRADED && 0 < SISL_OPTIONS["no_read_from_dirty"].count()) { // LCOV_EXCL_START
        route = READ_ROUTE;
    } else { // LCOV_EXCL_STOP
        if (read_route::DEVB == _last_read) {
            if (read_route::DEVB == READ_ROUTE) need_to_test = true;
        } else {
            route = read_route::DEVB;
            if (read_route::DEVA == READ_ROUTE) need_to_test = true;
        }
    }

    // An optimization to allow READing from a degraded device and can be turned off with the read_from_dirty flag
    if (IS_DEGRADED && need_to_test) {
        if (_dirty_bitmap->is_dirty(addr, len)) {
            // We've already attempted this device...we don't want to re-attempt
            if (retry) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
            route = (read_route::DEVA == route) ? read_route::DEVB : read_route::DEVA;
        }
    }
    _last_read = route;

    // Attempt read on device; if it succeeds or we are degraded return the result
    auto device = (read_route::DEVA == route) ? _device_a : _device_b;
    if (auto res = func(*device, DEV_SUBCMD(device)); res || retry) { return res; }

    // Otherwise fail over the device and attempt the READ again marking this a retry
    sub_cmd = set_flags(sub_cmd, sub_cmd_flags::RETRIED);
    return __failover_read(sub_cmd, std::move(func), addr, len);
}

io_result Raid1Disk::handle_internal(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                                     uint32_t nr_vecs, uint64_t addr, int res) {
    sub_cmd = unset_flags(sub_cmd, sub_cmd_flags::INTERNAL);
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    if (0 == res) return __clean_pages(sub_cmd, addr, len, q, data);
    return __dirty_pages(sub_cmd, addr, len, q, data);
}

void Raid1Disk::idle_transition(ublksrv_queue const*, bool enter) {
    using namespace std::chrono_literals;
    if (enter) {
        RLOGT("Entering IDLE")
        auto cur_state = static_cast< uint8_t >(resync_state::PAUSE);
        while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::IDLE))) {
            if (static_cast< uint8_t >(resync_state::IDLE) == _resync_state.load()) return;
            std::this_thread::sleep_for(50ms);
        }
        RLOGT("IDLE entered")
    } else {
        RLOGT("Entering PAUSE")
        auto cur_state = static_cast< uint8_t >(resync_state::IDLE);
        while (!_resync_state.compare_exchange_weak(cur_state, static_cast< uint8_t >(resync_state::PAUSE))) {
            if (static_cast< uint8_t >(resync_state::ACTIVE) == _resync_state.load()) {
                RLOGT("Interrupting resync...")
                cur_state = static_cast< uint8_t >(resync_state::SLEEPING);
            }
            if (static_cast< uint8_t >(resync_state::PAUSE) == _resync_state.load()) break;
            std::this_thread::sleep_for(50ms);
        }
        RLOGT("IDLE exited")
    }
}

void Raid1Disk::collect_async(ublksrv_queue const* q, std::list< async_result >& results) {
    results.splice(results.end(), std::move(_pending_results[q]));
    if (!_device_a->uses_ublk_iouring) _device_a->collect_async(q, results);
    if (!_device_b->uses_ublk_iouring) _device_b->collect_async(q, results);
}

io_result Raid1Disk::handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                                    uint64_t addr) {
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("received DISCARD: [tag:{:x}] [lba:{:x}|len:{}] [vol:{}]", data->tag, lba, len, _str_uuid)

    if (static_cast< uint8_t >(resync_state::PAUSE) != _resync_state.load()) idle_transition(q, false);

    if (is_retry(sub_cmd)) [[unlikely]]
        return __handle_async_retry(sub_cmd, addr, len, q, data);

    return __replicate(
        sub_cmd,
        [q, data, len, addr](UblkDisk& d, sub_cmd_t scmd) {
            return d.handle_discard(q, data, scmd, len, addr + raid1::reserved_size);
        },
        addr, len, q, data);
}

io_result Raid1Disk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                               uint32_t nr_vecs, uint64_t addr) {
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("Received {}: [tag:{:x}] [lba:{:x}|len:{}] [sub_cmd:{}] [vol:{}]",
          ublksrv_get_op(data->iod) == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag, lba, len,
          ublkpp::to_string(sub_cmd), _str_uuid)

    if (static_cast< uint8_t >(resync_state::IDLE) != _resync_state.load()) idle_transition(q, false);

    // READs are a specisub_cmd that just go to one side we'll do explicitly
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

io_result Raid1Disk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("Received {}: [lba:{:x}|len:{}] [vol:{}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", lba, len, _str_uuid)

    if (static_cast< uint8_t >(resync_state::IDLE) != _resync_state.load()) idle_transition(nullptr, false);

    // READs are a specisub_cmd that just go to one side we'll do explicitly
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

static const uint8_t magic_bytes[16] = {0123, 045, 0377, 012, 064,  0231, 076, 0305,
                                        0147, 072, 0310, 027, 0111, 0256, 033, 0144};

constexpr auto SB_VERSION = 1;

// Read and load the RAID1 superblock off a device. If it is not set, meaning the Magic is missing, then initialize
// the superblock to the current version. Otherwise migrate any changes needed after version discovery.
static folly::Expected< std::pair< raid1::SuperBlock*, bool >, std::error_condition >
load_superblock(UblkDisk& device, boost::uuids::uuid const& uuid, uint32_t const chunk_size) {
    auto sb = read_superblock< raid1::SuperBlock >(device);
    if (!sb) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    bool was_new{false};
    if (memcmp(sb->header.magic, magic_bytes, sizeof(magic_bytes))) {
        RLOGW("Device does not have a valid raid1 superblock! Initializing! [{}] [vol:{}]", device, to_string(uuid))
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
    RLOGD("device has v{:0x} superblock [chunk_sz:{:x},{}] [vol:{}] ", be16toh(sb->header.version), chunk_size,
          (1 == sb->fields.clean_unmount) ? "Clean" : "Dirty", to_string(uuid))

    if (SB_VERSION > be16toh(sb->header.version)) { sb->header.version = htobe16(SB_VERSION); }
    return std::make_pair(sb, was_new);
}

namespace raid1 {
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

} // namespace ublkpp
