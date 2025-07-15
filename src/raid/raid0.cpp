#include "ublkpp/raid/raid0.hpp"

#include <boost/uuid/uuid_io.hpp>
#include <folly/Expected.h>
#include <ublksrv.h>
#include <ublksrv_utils.h>

#include "raid0_impl.hpp"
#include "superblock.hpp"

namespace ublkpp {

class StripeDevice {
    struct destroy_sb {
        void operator()(raid0::SuperBlock* p) const {
            DEBUG_ASSERT_NOTNULL(p, "Freeing NULL ptr!")
            free(p);
        }
    };

public:
    StripeDevice(std::shared_ptr< UblkDisk > device, raid0::SuperBlock* super) :
            dev(std::move(device)), _sb(super, destroy_sb()) {}
    std::shared_ptr< UblkDisk > dev;
    std::unique_ptr< raid0::SuperBlock, destroy_sb > _sb;
};

static folly::Expected< raid0::SuperBlock*, std::error_condition > load_superblock(UblkDisk& device,
                                                                                   boost::uuids::uuid const& uuid,
                                                                                   uint32_t const stripe_size,
                                                                                   uint16_t const stripe_off);

Raid0Disk::Raid0Disk(boost::uuids::uuid const& uuid, uint32_t const stripe_size_bytes,
                     std::vector< std::shared_ptr< UblkDisk > >&& disks) :
        UblkDisk(), _stripe_size(stripe_size_bytes), _stride_width(_stripe_size * disks.size()) {
    // Discover overall Device parameters
    auto& our_params = *params();
    our_params.types |= UBLK_PARAM_TYPE_DISCARD;
    our_params.basic.dev_sectors = UINT64_MAX;
    direct_io = true;

    for (auto&& device : disks) {
        auto const& dev_params = *device->params();
        // We'll use dev_sectors to track the smallest array device we have
        our_params.basic.dev_sectors = std::min(our_params.basic.dev_sectors, dev_params.basic.dev_sectors);
        our_params.basic.logical_bs_shift =
            std::max(our_params.basic.logical_bs_shift, dev_params.basic.logical_bs_shift);
        our_params.basic.physical_bs_shift =
            std::max(our_params.basic.physical_bs_shift, dev_params.basic.physical_bs_shift);
        our_params.basic.max_sectors = std::min(our_params.basic.max_sectors,
                                                static_cast< uint32_t >(dev_params.basic.max_sectors * disks.size()));

        if (!device->can_discard()) our_params.types &= ~UBLK_PARAM_TYPE_DISCARD;
        if (!device->uses_ublk_iouring) uses_ublk_iouring = false;

        direct_io = direct_io ? device->direct_io : false;
        auto sb = load_superblock(*device, uuid, _stripe_size, _stripe_array.size());
        if (!sb) throw std::runtime_error(fmt::format("Could not read superblock! {}", sb.error().message()));
        _stripe_array.emplace_back(std::make_unique< StripeDevice >(std::move(device), sb.value()));
    }

    // Finally we'll calculate the volume size as a multiple of the smallest array device
    // and adjust to account for the superblock we will write at the HEAD of each array device.
    // To keep things simple, we'll just use the first chunk from each device for ourselves.
    our_params.basic.dev_sectors -= (_stripe_size >> SECTOR_SHIFT);
    our_params.basic.dev_sectors *= _stripe_array.size();

    if (can_discard())
        our_params.discard.discard_granularity = std::max(our_params.discard.discard_granularity, block_size());
}

Raid0Disk::~Raid0Disk() = default;

std::list< int > Raid0Disk::open_for_uring(int const iouring_device_start) {
    auto fds = std::list< int >();
    for (auto& stripe : _stripe_array) {
        fds.splice(fds.end(), stripe->dev->open_for_uring(iouring_device_start + fds.size()));
    }
    return fds;
}

void Raid0Disk::collect_async(ublksrv_queue const* q, std::list< async_result >& results) {
    for (auto const& stripe : _stripe_array) {
        if (!stripe->dev->uses_ublk_iouring) stripe->dev->collect_async(q, results);
    }
}

io_result Raid0Disk::handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) {
    bool const retry{is_retry(sub_cmd)};
    if (!retry) sub_cmd = shift_route(sub_cmd, route_size());
    auto cnt{0UL};
    auto stripe_off{0U};
    for (auto const& stripe : _stripe_array) {
        auto const new_sub_cmd = sub_cmd + (!retry ? stripe_off : 0U);
        auto res = stripe->dev->handle_flush(q, data, new_sub_cmd);
        if (!res) return res;
        cnt += res.value();
        ++stripe_off;
    }
    return cnt;
}

io_result Raid0Disk::handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                                    uint64_t addr) {
    bool const retry{is_retry(sub_cmd)};
    if (!retry) sub_cmd = shift_route(sub_cmd, route_size());

    // Adjust the address for our superblock area, do not use _addr_ beyond this.
    auto const lba = addr >> params()->basic.logical_bs_shift;
    addr += _stride_width;

    auto cnt{0U};
    for (auto const& [stripe_off, region] : raid0::merged_subcmds(_stride_width, _stripe_size, addr, len)) {
        auto const& [logical_off, logical_len] = region;
        auto const& device = _stripe_array[stripe_off]->dev;
        if (retry && (stripe_off != ((sub_cmd >> device->route_size()) & 0x0Fu))) [[unlikely]]
            continue;
        sub_cmd_t const new_sub_cmd = sub_cmd + (!retry ? stripe_off : 0);
        auto const logical_lba = logical_off >> params()->basic.logical_bs_shift;

        RLOGD("Received DISCARD: [tag:{:x}] ublk io [lba:{:x}|len:{}] -> "
              "[stripe_off:{}|logical_lba:{:x}|logical_len:{}|sub_cmd:{}]",
              data->tag, lba, len, stripe_off, logical_lba, logical_len, ublkpp::to_string(new_sub_cmd))
        auto res = device->handle_discard(q, data, new_sub_cmd, logical_len, logical_off);
        if (!res) return res;
        cnt += res.value();
    }
    return cnt;
}

/// This is the primary I/O handler call for RAID0
//
//  RAID0 is primary responsible for splitting an I/O request across several stripes. These operations can cross
//  stripe boundaries and even wrap around several strides. This routine handles this calculation and calls
//  the given routine `func` for each stripe that it has collected scatter (struct iovec) operations for.
io_result Raid0Disk::__distribute(iovec* iovecs, uint64_t addr, auto&& func, bool retry, sub_cmd_t sub_cmd) const {
    // Each thread has a 2-dimensional block of iovecs that we can split into
    thread_local auto sub_cmds =
        std::array< std::tuple< uint64_t, uint32_t, std::array< iovec, 16 > >, _max_stripe_cnt >();

    // Special case for single device
    if (1 == _stripe_array.size()) return func(0, sub_cmd, iovecs, 1, addr);

    DEBUG_ASSERT_LE(iovecs->iov_len, UINT32_MAX)
    auto const len = (uint32_t)iovecs->iov_len;
    uint32_t cnt{0};
    for (auto off = 0U; len > off;) {
        auto const [stripe_off, logical_off, sz] =
            raid0::next_subcmd(_stride_width, _stripe_size, addr + off, (len - off));

        // Ensure we advance here in case we _continue_ or anything later
        auto buf_cursor = (uint8_t*)iovecs->iov_base + off;
        off += sz;

        // Get the device
        auto const& device = _stripe_array[stripe_off]->dev;

        // If this is a retry, we only want to re-issue the operation whose route matches the one passed in
        if (retry) [[unlikely]] {
            // Mask off to get "our" portion of the original route and see if the device that processed this
            // operation matches the current RAID-0 sub-operation; if not then skip.
            if (stripe_off != ((sub_cmd >> device->route_size()) & 0x0Fu)) continue;
        }

        auto& [io_addr, alive_cmds, io_array] = sub_cmds[stripe_off];
        { // Fillout iovec
            auto& iov = io_array[alive_cmds++];
            iov.iov_base = (void*)buf_cursor;
            iov.iov_len = sz;
        }
        if (1 == alive_cmds) [[likely]]
            io_addr = logical_off;

        // Last sub_cmd for this device, issue now
        if ((_stride_width - _stripe_size) >= (len - off)) {
            DEBUG_ASSERT_LE(io_addr, UINT32_MAX)
            sub_cmd_t const new_sub_cmd = sub_cmd + (!retry ? (uint16_t)stripe_off : 0);
            DEBUG_ASSERT_LE(alive_cmds, UINT32_MAX)
            auto res = func(stripe_off, new_sub_cmd, io_array.data(), alive_cmds, (uint32_t)io_addr);
            // Set this back to zero so the next command can reuse
            alive_cmds = 0;
            if (!res) return res;
            cnt += res.value();
        }
    }
    return cnt;
}

io_result Raid0Disk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                               uint32_t nr_vecs, uint64_t addr) {
    // RAID-0 only supports not-scattered I/O currently!
    if (1 != nr_vecs) return folly::makeUnexpected(std::make_error_condition(std::errc::invalid_argument));

    bool const retry{is_retry(sub_cmd)};
    if (!retry) sub_cmd = shift_route(sub_cmd, route_size());
    auto const lba = addr >> params()->basic.logical_bs_shift;
    RLOGT("Received {}: [tag:{:x}] ublk io [lba:{:x}|len:{}] [sub_cmd:{}]",
          ublksrv_get_op(data->iod) == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag, lba, iovecs->iov_len,
          ublkpp::to_string(sub_cmd))

    // Adjust the address for our superblock area, do not use _addr_ beyond this.
    addr += _stride_width;

    return __distribute(
        iovecs, addr,
        [q, data, this](uint32_t stripe_off, sub_cmd_t new_sub_cmd, iovec* iov, uint32_t nr_iovs,
                        uint32_t logical_off) {
            auto const logical_lba = logical_off >> params()->basic.logical_bs_shift;
            RLOGT("Perform {}: [tag:{:x}] ublk aysnc_io -> "
                  "[stripe_off:{}|logical_lba:{:x}|logical_len:{}|sub_cmd:{}]",
                  ublksrv_get_op(data->iod) == UBLK_IO_OP_READ ? "READ" : "WRITE", data->tag, stripe_off, logical_lba,
                  __iovec_len(iov, iov + nr_iovs), ublkpp::to_string(new_sub_cmd))
            return _stripe_array[stripe_off]->dev->async_iov(q, data, new_sub_cmd, iov, nr_iovs, logical_off);
        },
        retry, sub_cmd);
}

io_result Raid0Disk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    // RAID-0 only supports not-scattered I/O currently!
    if (1 != nr_vecs) return folly::makeUnexpected(std::make_error_condition(std::errc::invalid_argument));

    // Adjust the address for our superblock area, do not use _addr_ beyond this.
    addr += _stride_width;

    return __distribute(iovecs, addr,
                        [op, this](uint32_t stripe_off, sub_cmd_t, iovec* iov, uint32_t nr_iovs, uint32_t logical_off) {
                            RLOGT("Perform {}: ublk sync_io -> "
                                  "[stripe_off:{}|logical_sector:{}|logical_len:{}]",
                                  op == UBLK_IO_OP_READ ? "READ" : "WRITE", stripe_off, logical_off >> SECTOR_SHIFT,
                                  __iovec_len(iov, iov + nr_iovs))
                            return _stripe_array[stripe_off]->dev->sync_iov(op, iov, nr_iovs, logical_off);
                        });
}

static const uint8_t magic_bytes[16] = {0127, 0345, 072,  0211, 0254, 033,  070,  0146,
                                        0125, 0377, 0204, 065,  0131, 0120, 0306, 047};
constexpr auto SB_VERSION = 1;

// Read and load the RAID0 superblock off a device. If it is not set, meaning the Magic is missing, then initialize
// the superblock to the current version. Otherwise migrate any changes needed after version discovery.
static folly::Expected< raid0::SuperBlock*, std::error_condition > load_superblock(UblkDisk& device,
                                                                                   boost::uuids::uuid const& uuid,
                                                                                   uint32_t const stripe_size,
                                                                                   uint16_t const stripe_off) {
    auto sb = read_superblock< raid0::SuperBlock >(device);
    if (!sb) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));

    // Check for MAGIC, initialize SB if missing
    if (memcmp(sb->header.magic, magic_bytes, sizeof(magic_bytes))) {
        RLOGW("Device does not have a valid raid0 superblock!: magic: {:x}\nread: {:x}\n Initializing!",
              spdlog::to_hex(magic_bytes, magic_bytes + sizeof(magic_bytes)),
              spdlog::to_hex(sb->header.magic, sb->header.magic + sizeof(magic_bytes)))
        memset(sb, 0x00, raid0::SuperBlock::SIZE);
        memcpy(sb->header.magic, magic_bytes, sizeof(magic_bytes));
        memcpy(sb->header.uuid, uuid.data, sizeof(sb->header.uuid));
        sb->fields.stripe_off = htobe16(stripe_off);
        sb->fields.stripe_size = htobe32(stripe_size);
    }

    // Verify some details in the superblock
    auto read_uuid = boost::uuids::uuid();
    memcpy(read_uuid.data, sb->header.uuid, sizeof(sb->header.uuid));
    if (uuid != read_uuid) {
        RLOGE("Superblock did not have a matching UUID expected: {} read: {}", to_string(uuid), to_string(read_uuid))
        return folly::makeUnexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    if ((stripe_size != be32toh(sb->fields.stripe_size)) || (stripe_off != be16toh(sb->fields.stripe_off))) {
        RLOGE("Superblock does not match given array parameters: Expected [stripe_sz:{:x},stripe_off:{}] != Found "
              "[stripe_sz:{:x},stripe_off:{}]",
              stripe_size, stripe_off, be32toh(sb->fields.stripe_size), be16toh(sb->fields.stripe_off))
        free(sb);
        return folly::makeUnexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    RLOGD("Device has v{:0x} superblock [stripe_sz:{:x},stripe_off:{}]", be16toh(sb->header.version), stripe_size,
          stripe_off)

    // Migrating to latest version
    if (SB_VERSION > be16toh(sb->header.version)) {
        sb->header.version = htobe16(SB_VERSION);
        if (!write_superblock(device, sb)) {
            free(sb);
            return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
        }
    }
    return sb;
}

} // namespace ublkpp
