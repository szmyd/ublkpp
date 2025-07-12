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

#define CLEAN_DEVICE (read_route::DEVB == _read_route ? _device_b : _device_a)
#define DIRTY_DEVICE (read_route::DEVB == _read_route ? _device_a : _device_b)

// SubCmd decoders
#define SEND_TO_A (sub_cmd & ((1U << sqe_tgt_data_width) - 2))
#define SEND_TO_B (sub_cmd | 0b1)
#define CLEAN_SUBCMD ((read_route::DEVB == _read_route) ? SEND_TO_B : SEND_TO_A)
#define DIRTY_SUBCMD ((read_route::DEVB == _read_route) ? SEND_TO_A : SEND_TO_B)

#define IS_DEGRADED (0 < _degraded_ops)
#define NOT_DEGRADED (0 == _degraded_ops)

static folly::Expected< raid1::SuperBlock*, std::error_condition >
load_superblock(UblkDisk& device, boost::uuids::uuid const& uuid, uint32_t const chunk_size);

Raid1Disk::Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a,
                     std::shared_ptr< UblkDisk > dev_b) :
        UblkDisk(),
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

    auto sb_a = load_superblock(*_device_a, uuid, SISL_OPTIONS["chunk_size"].as< uint32_t >());
    if (!sb_a) throw std::runtime_error(fmt::format("Could not read superblock! {}", sb_a.error().message()));
    auto sb_b = load_superblock(*_device_b, uuid, SISL_OPTIONS["chunk_size"].as< uint32_t >());
    if (!sb_b) {
        free(sb_a.value());
        throw std::runtime_error(fmt::format("Could not read superblock! {}", sb_b.error().message()));
    }

    // We only keep the latest or if match and A unclean take B
    auto const a_fields = sb_a.value()->fields;
    if (auto const b_fields = sb_a.value()->fields; b_fields.bitmap.age > a_fields.bitmap.age) {
        free(sb_a.value());
        _sb = sb_b.value();
        _sb->fields.bitmap.dirty = 1;
        _read_route = read_route::DEVB;
    } else if ((a_fields.bitmap.age == b_fields.bitmap.age) && (!a_fields.clean_unmount && b_fields.clean_unmount)) {
        free(sb_a.value());
        _sb = sb_b.value();
        _read_route = read_route::DEVB;
    } else if ((a_fields.bitmap.age > b_fields.bitmap.age) || (a_fields.clean_unmount && !b_fields.clean_unmount)) {
        free(sb_b.value());
        _sb = sb_a.value();
        _read_route = read_route::DEVA;
    } else {
        // Otherwise this is a clean device, we can read from either side
        free(sb_b.value());
        _sb = sb_a.value();
        if (0 == _sb->fields.bitmap.age) {
            // This is a new Device!
            // Start age at 1 for brand new Device
            _sb->fields.bitmap.age = 1;
        }
    }
    // We mark the SB dirty here and clean in our destructor so we know if we _crashed_ at some instance later
    _sb->fields.clean_unmount = 0x0;

    // Start the degraded ops counter at 1
    _degraded_ops = (_sb->fields.bitmap.dirty ? 1 : 0);

    // If we Fail to write the SuperBlock to then CLEAN device we immediately dirty the bitmap and try to write to DIRTY
    sub_cmd_t const sub_cmd = 0U;
    if (!write_superblock(*CLEAN_DEVICE, _sb)) {
        RLOGD("Failed writing SuperBlock to: [{}] becoming degraded.", *CLEAN_DEVICE)
        // If already degraded this is Fatal
        if (IS_DEGRADED) {
            free(_sb);
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
        // This will write the SB to DIRTY so we can skip this down below
        if (!__dirty_bitmap(CLEAN_SUBCMD)) {
            free(_sb);
            throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
        }
    }
    // Write to DIRTY only if not degraded.
    if (IS_DEGRADED) return;
    if (write_superblock(*DIRTY_DEVICE, _sb)) return;
    RLOGD("Failed writing SuperBlock to: [{}] becoming degraded.", *DIRTY_DEVICE)
    if (!__dirty_bitmap(DIRTY_SUBCMD)) {
        free(_sb);
        throw std::runtime_error(fmt::format("Could not initialize superblocks!"));
    }
}

Raid1Disk::~Raid1Disk() {
    if (!_sb) return;
    _sb->fields.clean_unmount = 0x1;
    // Only update the superblock to clean devices
    write_superblock(*CLEAN_DEVICE, _sb);
    if (0 == _sb->fields.bitmap.dirty) write_superblock(*DIRTY_DEVICE, _sb);
    free(_sb);
}

std::list< int > Raid1Disk::open_for_uring(int const iouring_device_start) {
    auto fds = (_device_a->open_for_uring(iouring_device_start));
    fds.splice(fds.end(), _device_b->open_for_uring(iouring_device_start + fds.size()));
    return fds;
}

// If sub_cmd was for DevA switch Clean to B and vice-versa
#define SWITCH_CLEAN(s_cmd)                                                                                            \
    _read_route = (0b1 & ((s_cmd) >> _device_b->route_size())) ? read_route::DEVA : read_route::DEVB;

bool Raid1Disk::__dirty_bitmap(sub_cmd_t sub_cmd) {
    DEBUG_ASSERT(NOT_DEGRADED, "DIRTY_BITMAP on degraded device!")
    ++_sb->fields.bitmap.age;
    _sb->fields.bitmap.dirty = 1;
    // Rotate Clean Device
    SWITCH_CLEAN(sub_cmd)
    // Must update superblock!
    _degraded_ops = (write_superblock(*CLEAN_DEVICE, _sb) ? 1 : 0);
    return IS_DEGRADED;
}

/// This is the primary I/O handler call for RAID1
//
//  RAID1 is primary responsible for replicating mutations (e.g. Writes/Discards) to a pair of compatible devices.
//  READ operations need only go to one side. So they are handled separately.
io_result Raid1Disk::__replicate(sub_cmd_t sub_cmd, auto&& func) {
    if (is_retry(sub_cmd)) {
        // If we're already degraded and failure was on current disk then treat this as a failure!
        if (IS_DEGRADED && (sub_cmd == CLEAN_SUBCMD))
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        // If sub_cmd requires retry, and we're not degraded we always DIRTY the Bitmap
        if (NOT_DEGRADED && !__dirty_bitmap(sub_cmd))
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        // FIXME: We should be able to return 0 regardless of which device caused a retry
        if (test_flags(sub_cmd, sub_cmd_flags::REPLICATED)) return 0;
    } else [[likely]]
        // Apply our shift to the sub_cmd if it's not a retry
        sub_cmd = shift_route(sub_cmd, route_size());

    auto res = func(*CLEAN_DEVICE, CLEAN_SUBCMD);

    // If we are degraded we can just return here with the result of the CLEAN device. We will
    // attempt to come out of degraded mode after some period of _degraded_ops have passed which
    // is configurable
    if (IS_DEGRADED && ++_degraded_ops) return res;

    // If not-degraded and first sub_cmd failed immediately, dirty bitmap and return result of op on alternate-path
    if (!res) {
        if (!__dirty_bitmap(CLEAN_SUBCMD)) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        return func(*CLEAN_DEVICE, CLEAN_SUBCMD);
    }
    // Otherwise tag the replica sub_cmd so we don't include its value in the target result
    sub_cmd = set_flags(sub_cmd, sub_cmd_flags::REPLICATED);
    auto a_v = res.value();
    if (res = func(*DIRTY_DEVICE, DIRTY_SUBCMD); !res) {
        // If the replica sub_cmd fails immediately we can dirty the bitmap here and return result from firsub_cmd
        if (!__dirty_bitmap(DIRTY_SUBCMD)) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
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
        SWITCH_CLEAN(sub_cmd)
    }

    // Attempt read on device; if it succeeds or we are degraded return the result
    if (auto res = func(*CLEAN_DEVICE, CLEAN_SUBCMD); res || (IS_DEGRADED && !_read_from_dirty) || retry) {
        if (!retry && IS_DEGRADED) ++_degraded_ops;
        return res;
    }

    // Otherwise fail over the device and attempt the READ again marking this a retry
    SWITCH_CLEAN(CLEAN_SUBCMD);
    sub_cmd = set_flags(sub_cmd, sub_cmd_flags::RETRIED);
    return func(*CLEAN_DEVICE, CLEAN_SUBCMD);
}

void Raid1Disk::collect_async(ublksrv_queue const* q, std::list< async_result >& results) {
    if (!_device_a->uses_ublk_iouring) _device_a->collect_async(q, results);
    if (!_device_b->uses_ublk_iouring) _device_b->collect_async(q, results);
}

io_result Raid1Disk::handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                                    uint64_t addr) {
    RLOGT("Received DISCARD: [tag:{}] ublk io [sector:{}|len:{}]", data->tag, addr >> SECTOR_SHIFT, len)

    // Adjust for reserved area
    return __replicate(sub_cmd, [q, data, len, adj_addr = (addr + raid1::reserved_size)](UblkDisk& d, sub_cmd_t scmd) {
        return d.handle_discard(q, data, scmd, len, adj_addr);
    });
}

io_result Raid1Disk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                               uint32_t nr_vecs, uint64_t addr) {
    // Only replicate non-READs
    RLOGT("Received {}: [tag:{}] ublk io [sector:{}|len:{}] [sub_cmd:{:b}]",
          ublksrv_get_op(data->iod) == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag, addr >> SECTOR_SHIFT,
          __iovec_len(iovecs, iovecs + nr_vecs), sub_cmd)
    // Adjust for reserved area
    addr += raid1::reserved_size;

    // READs are a specisub_cmd that just go to one side we'll do explicitly
    if (UBLK_IO_OP_READ == ublksrv_get_op(data->iod))
        return __failover_read(sub_cmd, [q, data, iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t scmd) {
            return d.async_iov(q, data, scmd, iovecs, nr_vecs, addr);
        });
    return __replicate(sub_cmd, [q, data, iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t scmd) {
        return d.async_iov(q, data, scmd, iovecs, nr_vecs, addr);
    });
    return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
}

io_result Raid1Disk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    // This allows us to use the DIRTY_BITMAP and SWITCH_CLEAN macros below
    sub_cmd_t sub_cmd = 0U;

    // Adjust for reserved area
    addr += raid1::reserved_size;

    // READs are a specisub_cmd that just go to one side we'll do explicitly
    if (UBLK_IO_OP_READ == op)
        return __failover_read(sub_cmd, [iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t) {
            return d.sync_iov(UBLK_IO_OP_READ, iovecs, nr_vecs, addr);
        });

    // Noramlly the target handles the result being duplicated for WRITEs, we handle it for sync_io here
    size_t res{0};
    if (auto io_res = __replicate(sub_cmd,
                                  [&res, op, iovecs, nr_vecs, addr](UblkDisk& d, sub_cmd_t s) {
                                      auto p_res = d.sync_iov(op, iovecs, nr_vecs, addr);
                                      if (p_res && !test_flags(s, sub_cmd_flags::REPLICATED)) res += p_res.value();
                                      return p_res;
                                  });
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
        RLOGW("Device does not have a valid raid1 superblock!: magic: {:x}\nread: {:x}\n Initializing!",
              spdlog::to_hex(magic_bytes, magic_bytes + sizeof(magic_bytes)),
              spdlog::to_hex(sb->header.magic, sb->header.magic + sizeof(magic_bytes)))
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
        RLOGW("Superblock was created with different chunk_size: [{}B] will not use runtime config of [{}B]",
              be32toh(sb->fields.bitmap.chunk_size), chunk_size)
    }
    RLOGD("Device has v{:0x} superblock [chunk_sz:{:x},{}]", be16toh(sb->header.version), chunk_size,
          (1 == sb->fields.clean_unmount) ? "Clean" : "Dirty")

    if (SB_VERSION > be16toh(sb->header.version)) { sb->header.version = htobe16(SB_VERSION); }
    return sb;
}

} // namespace ublkpp
