#include "ublkpp/raid/raid1.hpp"

#include <set>

#include <boost/uuid/uuid_io.hpp>
#include <ublksrv.h>
#include <ublksrv_utils.h>
#include <sisl/options/options.h>

#include "raid1_impl.hpp"
#include "superblock.hpp"

SISL_OPTION_GROUP(raid1,
                  (chunk_size, "", "chunk_size", "The desired chunk_size for new Raid1 devices",
                   cxxopts::value< std::uint32_t >()->default_value("32768"), "<io_size>"),
                  (read_from_dirty, "", "read_from_dirty", "Allow reads from a Dirty device", cxxopts::value< bool >(),
                   ""))

namespace ublkpp {
using raid1::read_route;

// SubCmd decoders
#define SEND_TO_A (sub_cmd & ((1U << sqe_tgt_data_width) - 2))
#define SEND_TO_B (sub_cmd | 0b1)

// Route routines
#define IS_DEGRADED (0 < _degraded_ops)
#define CLEAN_DEVICE (read_route::DEVB == _read_route ? _device_b : _device_a)
#define DIRTY_DEVICE (read_route::DEVB == _read_route ? _device_a : _device_b)
#define CLEAN_SUBCMD ((read_route::DEVB == _read_route) ? SEND_TO_B : SEND_TO_A)
#define DIRTY_SUBCMD ((read_route::DEVB == _read_route) ? SEND_TO_A : SEND_TO_B)

// If sub_cmd was for DevA switch Clean to B and vice-versa
#define SWITCH_TARGET(s_cmd)                                                                                           \
    _read_route = (0b1 & ((s_cmd) >> _device_b->route_size())) ? read_route::DEVA : read_route::DEVB;

static folly::Expected< raid1::SuperBlock*, std::error_condition >
load_superblock(UblkDisk& device, boost::uuids::uuid const& uuid, uint32_t const chunk_size);

struct free_page {
    void operator()(void* x) { free(x); }
};

Raid1Disk::Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a,
                     std::shared_ptr< UblkDisk > dev_b) :
        UblkDisk(),
        _str_uuid(boost::uuids::to_string(uuid)),
        _device_a(std::move(dev_a)),
        _device_b(std::move(dev_b)),
        _read_from_dirty(0 < SISL_OPTIONS["read_from_dirty"].count()) {
    direct_io = true;
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
        if (!device->uses_ublk_iouring) uses_ublk_iouring = false;
    }
    // Reserve space for the superblock/bitmap
    our_params.basic.dev_sectors -= raid1::reserved_sectors;
    if (can_discard())
        our_params.discard.discard_granularity = std::max(our_params.discard.discard_granularity, block_size());

    auto read_super = load_superblock(*_device_a, uuid, SISL_OPTIONS["chunk_size"].as< uint32_t >());
    if (!read_super)
        throw std::runtime_error(fmt::format("Could not read superblock! {}", read_super.error().message()));
    auto sb_a = std::shared_ptr< raid1::SuperBlock >(read_super.value(), free_page());
    read_super = load_superblock(*_device_b, uuid, SISL_OPTIONS["chunk_size"].as< uint32_t >());
    if (!read_super)
        throw std::runtime_error(fmt::format("Could not read superblock! {}", read_super.error().message()));
    auto sb_b = std::shared_ptr< raid1::SuperBlock >(read_super.value(), free_page());

    // We only keep the latest or if match and A unclean take B
    if (auto res_pair = pick_superblock(sb_a.get(), sb_b.get()); res_pair.first) {
        _sb = (res_pair.first == sb_a.get() ? std::move(sb_a) : std::move(sb_b));
        _read_route = res_pair.second;
    } else
        throw std::runtime_error("Could not find reasonable superblock!");

    // We mark the SB dirty here and clean in our destructor so we know if we _crashed_ at some instance later
    _sb->fields.clean_unmount = 0x0;

    // Start the degraded ops counter at 1
    _degraded_ops = (_sb->fields.bitmap.dirty ? 1 : 0);

    // If we Fail to write the SuperBlock to then CLEAN device we immediately dirty the bitmap and try to write to
    // DIRTY
    sub_cmd_t const sub_cmd = 0U;
    if (!write_superblock(*CLEAN_DEVICE, _sb.get())) {
        RLOGE("Failed writing SuperBlock to: [{}] becoming degraded. [vol:{}]", *CLEAN_DEVICE, _str_uuid)
        // If already degraded this is Fatal
        if (IS_DEGRADED) { throw std::runtime_error(fmt::format("Could not initialize superblocks!")); }
        // This will write the SB to DIRTY so we can skip this down below
        if (!__dirty_bitmap(CLEAN_SUBCMD, 0, 0)) {
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
    }
    // Write to DIRTY only if not degraded.
    if (IS_DEGRADED) return;
    if (write_superblock(*DIRTY_DEVICE, _sb.get())) return;
    RLOGE("Failed writing SuperBlock to: [{}] becoming degraded. [vol:{}] ", *DIRTY_DEVICE, _str_uuid)
    if (!__dirty_bitmap(DIRTY_SUBCMD, 0, 0)) {
        throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
    }
}

Raid1Disk::~Raid1Disk() {
    if (!_sb) return;
    _sb->fields.clean_unmount = 0x1;
    // Only update the superblock to clean devices
    write_superblock(*CLEAN_DEVICE, _sb.get());
    if (0 == _sb->fields.bitmap.dirty)
        if (!write_superblock(*DIRTY_DEVICE, _sb.get())) RLOGW("Write clean_unmount failed [vol:{}]", _str_uuid)
}

std::list< int > Raid1Disk::open_for_uring(int const iouring_device_start) {
    auto fds = (_device_a->open_for_uring(iouring_device_start));
    fds.splice(fds.end(), _device_b->open_for_uring(iouring_device_start + fds.size()));
    return fds;
}

io_result Raid1Disk::__dirty_bitmap(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                    ublk_io_data const* data) {
    io_result dirty_result{0};
    if (!IS_DEGRADED) {
        // We only update the AGE if we're not degraded already
        auto const orig_route = _read_route;
        SWITCH_TARGET(sub_cmd)
        ++_sb->fields.bitmap.age;
        _sb->fields.bitmap.dirty = 1;
        RLOGW("BITMAP becoming dirty [tag:{:x}] [sub_cmd:{:b}] [age:{}] [vol:{}] ", (data ? data->tag : INT_MAX),
              sub_cmd, (uint64_t)_sb->fields.bitmap.age, _str_uuid);
        // Must update age first; we do this synchronously to gate pending retry results
        if (auto sync_res = write_superblock(*CLEAN_DEVICE, _sb.get()); !sync_res) {
            // Rollback the failure to update the header
            _sb->fields.bitmap.dirty = 0;
            --_sb->fields.bitmap.age;
            _read_route = orig_route;
            RLOGE("Could not become degraded [vol:{}]: {}", _str_uuid, sync_res.error().message())
            return sync_res;
        }
        _degraded_ops = 1;
    }

    // FIXME: This is very unoptimized for the purposes of being correct initially...
    // Generate the blocks we need to write in order to record that this device has
    // data to copy to the DIRTY device when available once again and write it to
    // the corresponding area of the CLEAN device.
    auto new_cmd = set_flags(CLEAN_SUBCMD, sub_cmd_flags::INTERNAL);
    for (auto off = 0U; len > off;) {
        auto [page_offset, word_offset, shift_offset, sz] =
            raid1::calc_bitmap_region(addr + off, len - off, block_size(), be32toh(_sb->fields.bitmap.chunk_size));
        RLOGW("Making dirty: [pg:{}, word:{}, bit:{}, nr_bits:{}] [sub_cmd:{:b}] [volid:{}]", page_offset, word_offset,
              shift_offset, sz, sub_cmd, _str_uuid);

        uint64_t* cur_page;
        if (auto [it, happened] = _dirty_pages.emplace(std::make_pair(page_offset, nullptr)); happened) {
            void* new_page{nullptr};
            if (auto err = ::posix_memalign(&new_page, block_size(), block_size()); err)
                return folly::makeUnexpected(std::make_error_condition(std::errc::io_error)); // LCOV_EXCL_LINE
            it->second.reset(reinterpret_cast< uint64_t* >(new_page), free_page());
            cur_page = it->second.get();
        } else
            cur_page = it->second.get();
        auto cur_word = cur_page + word_offset;
        off += sz;
        (*cur_word) |= ((uint64_t)0b1 << shift_offset);

        auto iov = iovec{.iov_base = cur_page, .iov_len = block_size()};
        auto page_addr = (block_size() * page_offset) + raid1::SuperBlock::SIZE;

        if (auto res = data ? CLEAN_DEVICE->async_iov(q, data, new_cmd, &iov, 1, page_addr)
                            : CLEAN_DEVICE->sync_iov(UBLK_IO_OP_WRITE, &iov, 1, page_addr);
            !res)
            return res;
        else
            dirty_result = dirty_result.value() + res.value();
        // MUST Submit here since iov is on the stack!
        if (q) io_uring_submit(q->ring_ptr);
    }

    return dirty_result;
}

// Failed Async WRITEs all end up here and have the side-effect of dirtying the BITMAP
// on the working device. This blocks the final result going back from the original operation
// as we chain additional sub_cmds by returning a value > 0 including a new "result" for the
// original sub_cmd
io_result Raid1Disk::__handle_async_retry(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                          ublk_io_data const* async_data) {
    // No Synchronous operations retry
    DEBUG_ASSERT_NOTNULL(async_data, "Retry on an synchronous I/O!"); // LCOV_EXCL_LINE

    // If we're already degraded and failure was on CLEAN disk then treat this as a fatal
    if (IS_DEGRADED && (CLEAN_SUBCMD == sub_cmd))
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));

    // Record this degraded WRITE in the bitmap, result is # of async writes enqueued
    auto dirty_res = __dirty_bitmap(sub_cmd, addr, len, q, async_data);
    if (!dirty_res) return dirty_res;

    // Bitmap is marked dirty, queue a new asynchronous "reply" for this original cmd
    _pending_results[q].emplace_back(async_result{async_data, sub_cmd, (int)len});

    if (q) ublksrv_queue_send_event(q); // LCOV_EXCL_LINE
    return dirty_res.value() + 1;
}

/// This is the primary I/O handler call for RAID1
//
//  RAID1 is primary responsible for replicating mutations (e.g. Writes/Discards) to a pair of compatible devices.
//  READ operations need only go to one side. So they are handled separately.
io_result Raid1Disk::__replicate(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                 ublk_io_data const* async_data) {
    if (is_retry(sub_cmd)) [[unlikely]]
        return __handle_async_retry(sub_cmd, addr, len, q, async_data);

    // Apply our shift to the sub_cmd if it's not a retry
    sub_cmd = shift_route(sub_cmd, route_size());

    auto res = func(*CLEAN_DEVICE, CLEAN_SUBCMD);

    // If we are degraded we can just return here with the result of the CLEAN device. We will
    // attempt to come out of degraded mode after some period of _degraded_ops have passed which
    // is configurable
    if (IS_DEGRADED && ++_degraded_ops) return res;

    // If not-degraded and first sub_cmd failed immediately, dirty bitmap and return result of op on alternate-path
    if (!res) {
        auto dirty_res = __dirty_bitmap(CLEAN_SUBCMD, addr, len, q, async_data);
        if (!dirty_res) return dirty_res;
        if (res = func(*CLEAN_DEVICE, CLEAN_SUBCMD); !res) return res;
        return res.value() + dirty_res.value();
    }
    // Otherwise tag the replica sub_cmd so we don't include its value in the target result
    sub_cmd = set_flags(sub_cmd, sub_cmd_flags::REPLICATED);
    auto a_v = res.value();
    if (res = func(*DIRTY_DEVICE, DIRTY_SUBCMD); !res) {
        // If the replica sub_cmd fails immediately we can dirty the bitmap here and return result from firsub_cmd
        auto dirty_res = __dirty_bitmap(DIRTY_SUBCMD, addr, len, q, async_data);
        if (!dirty_res) return dirty_res;
        a_v += dirty_res.value();
    } else
        a_v += res.value();
    // Assuming all was successful, return the aggregate of the results
    return a_v;
}

io_result Raid1Disk::__failover_read(sub_cmd_t sub_cmd, auto&& func) {
    auto const retry = is_retry(sub_cmd);
    if (!retry)
        sub_cmd = shift_route(sub_cmd, route_size());
    else if (IS_DEGRADED && !_read_from_dirty)
        // If we are already degraded return error
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    else {
        // If Retry then rotate the devices
        SWITCH_TARGET(sub_cmd)
    }

    // Attempt read on device; if it succeeds or we are degraded return the result
    if (auto res = func(*CLEAN_DEVICE, CLEAN_SUBCMD); res || (IS_DEGRADED && !_read_from_dirty) || retry) {
        if (!retry && IS_DEGRADED) ++_degraded_ops;
        return res;
    }

    // Otherwise fail over the device and attempt the READ again marking this a retry
    sub_cmd = set_flags(sub_cmd, sub_cmd_flags::RETRIED);
    return func(*DIRTY_DEVICE, DIRTY_SUBCMD);
}

void Raid1Disk::collect_async(ublksrv_queue const* q, std::list< async_result >& results) {
    results.splice(results.end(), std::move(_pending_results[q]));
    if (!_device_a->uses_ublk_iouring) _device_a->collect_async(q, results);
    if (!_device_b->uses_ublk_iouring) _device_b->collect_async(q, results);
}

io_result Raid1Disk::handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                                    uint64_t addr) {
    RLOGT("received DISCARD: [tag:{:x}] ublk io [sector:{}|len:{}] [vol:{}] ", data->tag, addr >> SECTOR_SHIFT, len,
          _str_uuid)

    // Adjust for reserved area
    addr += raid1::reserved_size;
    return __replicate(
        sub_cmd,
        [q, data, len, addr](UblkDisk& d, sub_cmd_t scmd) { return d.handle_discard(q, data, scmd, len, addr); }, addr,
        len, q, data);
}

io_result Raid1Disk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                               uint32_t nr_vecs, uint64_t addr) {
    auto const len = __iovec_len(iovecs, iovecs + nr_vecs);
    RLOGT("Received {}: [tag:{:x}] ublk io [sector:{}|len:{}] [sub_cmd:{:b}] [vol:{}]",
          ublksrv_get_op(data->iod) == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag, addr >> SECTOR_SHIFT, len,
          sub_cmd, _str_uuid)
    // Adjust for reserved area
    addr += raid1::reserved_size;

    // READs are a specisub_cmd that just go to one side we'll do explicitly
    if (UBLK_IO_OP_READ == ublksrv_get_op(data->iod))
        return __failover_read(sub_cmd, [q, data, iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t scmd) {
            return d.async_iov(q, data, scmd, iovecs, nr_vecs, addr);
        });
    return __replicate(
        sub_cmd,
        [q, data, iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t scmd) {
            return d.async_iov(q, data, scmd, iovecs, nr_vecs, addr);
        },
        addr, len, q, data);
}

io_result Raid1Disk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    // Adjust for reserved area
    addr += raid1::reserved_size;

    // READs are a specisub_cmd that just go to one side we'll do explicitly
    if (UBLK_IO_OP_READ == op)
        return __failover_read(0U, [iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t) {
            return d.sync_iov(UBLK_IO_OP_READ, iovecs, nr_vecs, addr);
        });

    size_t res{0};
    if (auto io_res = __replicate(
            0U,
            [&res, op, iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t s) {
                auto p_res = d.sync_iov(op, iovecs, nr_vecs, addr);
                // Noramlly the target handles the result being duplicated for WRITEs, we handle it for sync_io here
                if (p_res && !test_flags(s, sub_cmd_flags::REPLICATED)) res += p_res.value();
                return p_res;
            },
            addr, __iovec_len(iovecs, iovecs + nr_vecs)); // LCOV_EXCL_LINE
        !io_res)
        return io_res;
    return res;
}

static const uint8_t magic_bytes[16] = {0123, 045, 0377, 012, 064,  0231, 076, 0305,
                                        0147, 072, 0310, 027, 0111, 0256, 033, 0144};

constexpr auto SB_VERSION = 1;

// Read and load the RAID1 superblock off a device. If it is not set, meaning the Magic is missing, then initialize
// the superblock to the current version. Otherwise migrate any changes needed after version discovery.
static folly::Expected< raid1::SuperBlock*, std::error_condition >
load_superblock(UblkDisk& device, boost::uuids::uuid const& uuid, uint32_t const chunk_size) {
    auto sb = read_superblock< raid1::SuperBlock >(device);
    if (!sb) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    if (memcmp(sb->header.magic, magic_bytes, sizeof(magic_bytes))) {
        RLOGW("Device does not have a valid raid1 superblock!: magic: {:x}\nread: {:x}\n Initializing! [vol:{}]",
              spdlog::to_hex(magic_bytes, magic_bytes + sizeof(magic_bytes)),
              spdlog::to_hex(sb->header.magic, sb->header.magic + sizeof(magic_bytes)), to_string(uuid))
        memset(sb, 0x00, raid1::SuperBlock::SIZE);
        memcpy(sb->header.magic, magic_bytes, sizeof(magic_bytes));
        memcpy(sb->header.uuid, uuid.data, sizeof(sb->header.uuid));
        sb->fields.clean_unmount = 1;
        sb->fields.bitmap.chunk_size = htobe32(chunk_size);
        sb->fields.bitmap.age = 0;
        sb->fields.bitmap.dirty = 0;
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
        RLOGW("Superblock was created with different chunk_size: [{}B] will not use runtime config of [{}B] [vol:{}] ",
              be32toh(sb->fields.bitmap.chunk_size), chunk_size, to_string(uuid))
    }
    RLOGD("device has v{:0x} superblock [chunk_sz:{:x},{}] [vol:{}] ", be16toh(sb->header.version), chunk_size,
          (1 == sb->fields.clean_unmount) ? "Clean" : "Dirty", to_string(uuid))

    if (SB_VERSION > be16toh(sb->header.version)) { sb->header.version = htobe16(SB_VERSION); }
    return sb;
}

namespace raid1 {
std::pair< raid1::SuperBlock*, read_route > pick_superblock(raid1::SuperBlock* dev_a, raid1::SuperBlock* dev_b) {
    auto a_fields = dev_a->fields;
    if (auto b_fields = dev_b->fields; a_fields.bitmap.age < b_fields.bitmap.age) {
        b_fields.bitmap.dirty = 1;
        return std::make_pair(dev_b, read_route::DEVB);
    } else if (a_fields.bitmap.age > b_fields.bitmap.age) {
        a_fields.bitmap.dirty = 1;
        return std::make_pair(dev_a, read_route::DEVA);
    } else if (a_fields.clean_unmount != b_fields.clean_unmount)
        return std::make_pair(a_fields.clean_unmount ? dev_a : dev_b, read_route::EITHER);

    // Otherwise this is a clean device, we can read from either side
    // Start new devices at 1
    a_fields.bitmap.age = std::max(a_fields.bitmap.age, 1UL);
    return std::make_pair(dev_a, read_route::EITHER);
}
} // namespace raid1

} // namespace ublkpp
