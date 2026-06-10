#include "ublkpp/raid.hpp"

#include <bit>
#include <boost/uuid/uuid_io.hpp>
#include <ublksrv.h>
#include <ublksrv_utils.h>

#include <ublkpp/lib/cqe_state.hpp>
#include <ublkpp/lib/ublk_disk.hpp>

#include "raid0_impl.hpp"
#include "lib/logging.hpp"

namespace ublkpp {
constexpr uint32_t _max_stripe_cnt{64};
// max(max_io_size) / min(stripe_size) = 1 MiB / 64 KiB = 16
constexpr uint32_t k_max_iovecs_per_stripe{16};

struct StripeAccum {
    uint64_t io_addr{0};
    uint32_t nr_vecs{0};
    std::array< iovec, k_max_iovecs_per_stripe > io_array{};
};

class StripeDevice {
    struct destroy_sb {
        void operator()(raid0::SuperBlock* p) const {
            DEBUG_ASSERT_NOTNULL(p, "Freeing NULL ptr!") // LCOV_EXCL_LINE
            free(p);
        }
    };

public:
    StripeDevice(std::shared_ptr< ublk_disk > device, raid0::SuperBlock* super) :
            disk(std::move(device)), _sb(super, destroy_sb()) {}
    std::shared_ptr< ublk_disk > disk;
    std::unique_ptr< raid0::SuperBlock, destroy_sb > _sb;
};

static raid0::SuperBlock* read_superblock(ublk_disk& device);
static io_result write_superblock(ublk_disk& device, raid0::SuperBlock* sb);
static std::expected< raid0::SuperBlock*, std::error_condition >
load_superblock(ublk_disk& device, boost::uuids::uuid const& uuid, uint32_t& stripe_size, uint16_t const stripe_off);

// File-local concrete ublk_disk; constructed only via the make_raid0_disk factory below. The
// public header exposes only the factory + raid0:: free functions; consumers operate against
// the ublk_disk virtual interface.
class Raid0Disk : public ublk_disk {
    std::vector< std::unique_ptr< StripeDevice > > _stripe_array;

    uint32_t _stripe_size{0};
    uint64_t _stride_width{0}; // L1: widened to uint64_t; large configs (e.g. 128MiB × 64) overflow uint32_t

    io_result __distribute(std::array< StripeAccum, _max_stripe_cnt >& sub_cmds, iovec* iov, uint64_t addr,
                           auto&& func) const;

public:
    Raid0Disk(boost::uuids::uuid const& uuid, uint32_t const stripe_size_bytes,
              std::vector< std::shared_ptr< ublk_disk > >&& disks);
    ~Raid0Disk() override;

    std::shared_ptr< ublk_disk > get_device(uint32_t stripe_offset) const noexcept;
    uint32_t stripe_size() const noexcept { return _stripe_size; }

    std::string id() const noexcept override { return "RAID0"; }
    prepare_result prepare(ublksrv_queue const*, int const iouring_device) override;

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override;

    void probe_tick(ublksrv_queue const* q) noexcept override;

    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
};

Raid0Disk::Raid0Disk(boost::uuids::uuid const& uuid, uint32_t const stripe_size_bytes,
                     std::vector< std::shared_ptr< ublk_disk > >&& disks) :
        ublk_disk(), _stripe_size(stripe_size_bytes), _stride_width(_stripe_size * disks.size()) {
    if (disks.empty()) throw std::invalid_argument("Raid0Disk: at least one disk required");
    if (stripe_size_bytes == 0) throw std::invalid_argument("Raid0Disk: stripe_size_bytes must be non-zero");
    // L2: ilog2 rounds down for non-power-of-2 inputs, producing wrong geometry silently.
    // E.g. stripe_size=6KiB would use ilog2→12, treating it as 4KiB. Reject early.
    if (stripe_size_bytes & (stripe_size_bytes - 1))
        throw std::invalid_argument(
            fmt::format("Raid0Disk: stripe_size_bytes ({}) must be a power of 2", stripe_size_bytes));
    if (disks.size() > _max_stripe_cnt)
        throw std::invalid_argument(
            fmt::format("Raid0Disk: too many disks ({}), max is {}", disks.size(), _max_stripe_cnt));
    // Discover overall Device parameters
    auto& our_params = *params();
    our_params.types |= UBLK_PARAM_TYPE_DISCARD;
    our_params.basic.dev_sectors = UINT64_MAX;
    _direct_io = true;

    auto alt_stripe = false;
    our_params.basic.physical_bs_shift = ilog2(stripe_size_bytes);
    our_params.basic.io_opt_shift = ilog2(_stride_width);
    for (auto&& device : disks) {
        // We'll use dev_sectors to track the smallest array device we have
        our_params.basic.dev_sectors =
            std::min< uint64_t >(our_params.basic.dev_sectors, device->capacity() >> SECTOR_SHIFT);
        our_params.basic.logical_bs_shift =
            std::max(our_params.basic.logical_bs_shift, static_cast< uint8_t >(ilog2(device->block_size())));
        our_params.basic.max_sectors = std::min(
            our_params.basic.max_sectors, static_cast< uint32_t >((device->max_tx() >> SECTOR_SHIFT) * disks.size()));

        if (!device->can_discard()) our_params.types &= ~UBLK_PARAM_TYPE_DISCARD;

        _direct_io = _direct_io ? device->direct_io() : false;

        auto this_alt_stripe = _stripe_size;
        auto sb = load_superblock(*device, uuid, this_alt_stripe, _stripe_array.size());
        if (_stripe_size != this_alt_stripe) {
            if (!alt_stripe) {
                alt_stripe = true;
                _stripe_size = this_alt_stripe;
            } else
                throw std::runtime_error(fmt::format("Could not read superblock! Mismatched Stripe Sizes!"));
        }
        if (!sb) throw std::runtime_error(fmt::format("Could not read superblock! {}", sb.error().message()));
        _stripe_array.emplace_back(std::make_unique< StripeDevice >(std::move(device), sb.value()));
    }

    // Recompute _stride_width in case load_superblock corrected _stripe_size from the on-disk
    // superblock value (the initializer-list computed it from the caller-supplied stripe_size_bytes
    // before any superblock was read, so it may be stale).
    if (_stripe_size == 0)
        throw std::runtime_error("Raid0Disk: on-disk superblock delivered zero stripe_size (possible data corruption)");
    _stride_width = static_cast< uint64_t >(_stripe_size) * _stripe_array.size(); // L1: keep uint64_t arithmetic
    // Recompute io_opt_shift to match the corrected stride width.
    our_params.basic.io_opt_shift = ilog2(_stride_width);

    // Finally we'll calculate the volume size as a multiple of the smallest array device
    // and adjust to account for the superblock we will write at the HEAD of each array device.
    // To keep things simple, we'll just use the first chunk from each device for ourselves.

    // H2: guard against device capacity <= stripe_size which would underflow the unsigned subtraction.
    auto const stripe_size_sectors = static_cast< uint64_t >(_stripe_size >> SECTOR_SHIFT);
    // H3: floor the smallest leg to a whole number of stripes before striping. RAID0 can only use
    // stripes that are fully present on EVERY leg; a partial trailing stripe (when a leg's capacity
    // is not a multiple of stripe_size) is unusable. Without flooring, that remainder stays in the
    // per-leg term and is then multiplied by the leg count, so the array over-reports its size by
    // (leg_capacity % stripe_size) * leg_count. I/O into that phantom tail maps to a per-leg offset
    // past the end of the backing device, returning EIO. This was latent for years because leg
    // capacities happened to be stripe-aligned; raid1 SuperBlock v2 dropped the 512 KiB user-data
    // alignment that masked it, exposing top-of-device read failures on RAID10 arrays.
    our_params.basic.dev_sectors -= (our_params.basic.dev_sectors % stripe_size_sectors);
    if (our_params.basic.dev_sectors <= stripe_size_sectors)
        throw std::runtime_error(
            fmt::format("Raid0Disk: device capacity ({} sectors) is too small for stripe_size ({} sectors)",
                        our_params.basic.dev_sectors, stripe_size_sectors));
    our_params.basic.dev_sectors -= stripe_size_sectors;
    our_params.basic.dev_sectors *= _stripe_array.size();
    // M2: guard against division by zero if max_sectors is 0 (e.g. child device reported max_tx()==0).
    if (our_params.basic.max_sectors == 0)
        throw std::runtime_error("Raid0Disk: max_sectors is zero; child device reported max_tx() == 0");
    if ((max_tx() + _stripe_size - 1) / _stripe_size > k_max_iovecs_per_stripe)
        throw std::invalid_argument(
            fmt::format("Raid0Disk: ceil(max_io_size/stripe_size)={} exceeds k_max_iovecs_per_stripe={}; "
                        "reduce max_io_size or increase stripe_size",
                        (max_tx() + _stripe_size - 1) / _stripe_size, k_max_iovecs_per_stripe));
    // Align size to max_sector size
    our_params.basic.dev_sectors -= (our_params.basic.dev_sectors % our_params.basic.max_sectors);

    if (can_discard()) {
        our_params.discard.discard_granularity = std::max(our_params.discard.discard_granularity, block_size());
        // Propagate the tightest per-stripe max_discard_sectors across all child devices, scaled
        // by stripe count (a top-level discard of N sectors distributes ~N/stripes per child).
        uint32_t child_min = UINT32_MAX;
        for (auto const& s : _stripe_array)
            child_min = std::min(child_min, s->disk->max_discard_sectors());
        our_params.discard.max_discard_sectors = static_cast< uint32_t >(
            std::min< uint64_t >(static_cast< uint64_t >(child_min) * _stripe_array.size(), UINT32_MAX));
    }
}

Raid0Disk::~Raid0Disk() = default;

std::shared_ptr< ublk_disk > Raid0Disk::get_device(uint32_t stripe_offset) const noexcept {
    if (auto const width = _stripe_array.size(); width <= stripe_offset) {
        RLOGW("Stripe offset [{}] larger than array width [{}]", stripe_offset, width)
        return nullptr;
    }
    return _stripe_array[stripe_offset]->disk;
}

// Upper bound on distinct stripes touched by a single I/O.
// +1 for start-alignment: an unaligned start can spill into one extra stripe.
// Denominator is stripe_size (not stride_width): an io_size < stride_width can still touch
// multiple per-disk stripes if it crosses a stripe boundary. Capped by device count because
// __distribute gathers all iovecs per device into one task.
static inline size_t stripes_for_io(size_t io_size, size_t stripe_size, size_t nr_stripes) {
    return std::min((io_size + stripe_size - 1) / stripe_size + 1, nr_stripes);
}

Raid0Disk::prepare_result Raid0Disk::prepare(ublksrv_queue const* q, int const iouring_device_start) {
    prepare_result result;
    result.max_sqes_per_io = 0;
    // At most k stripes are active concurrently per max-size I/O. Each active stripe dispatches
    // one child async_iov that submits child.max_sqes_per_io SQEs into the shared pool. Sum the
    // first k contributions (homogeneous arrays: all equal, so order is irrelevant).
    auto const k = stripes_for_io(max_tx(), _stripe_size, _stripe_array.size());
    size_t counted = 0;
    for (auto& stripe : _stripe_array) {
        auto child = stripe->disk->prepare(q, iouring_device_start + static_cast< int >(result.fds.size()));
        result.fds.insert(result.fds.end(), child.fds.begin(), child.fds.end());
        if (counted++ < k) result.max_sqes_per_io += child.max_sqes_per_io;
    }
    return result;
}

void Raid0Disk::probe_tick(ublksrv_queue const* q) noexcept {
    for (auto const& stripe : _stripe_array) {
        stripe->disk->probe_tick(q);
    }
}

/// This is the primary I/O handler call for RAID0
//
//  RAID0 is primarily responsible for splitting an I/O request across several stripes. These operations can cross
//  stripe boundaries and even wrap around several strides. This routine handles this calculation and calls
//  the given routine `func` for each stripe that it has collected scatter (struct iovec) operations for.
io_result Raid0Disk::__distribute(std::array< StripeAccum, _max_stripe_cnt >& sub_cmds, iovec* iovecs, uint64_t addr,
                                  auto&& func) const {
    DEBUG_ASSERT_GT(_stride_width, 0ULL) // LCOV_EXCL_LINE

    if (1 == _stripe_array.size()) return func(0, iovecs, 1, addr);

    DEBUG_ASSERT_LE(iovecs->iov_len, UINT32_MAX) // LCOV_EXCL_LINE
    auto const len = static_cast< uint32_t >(iovecs->iov_len);
    uint32_t cnt{0};
    for (auto off = 0U; len > off;) {
        auto const [stripe_off, logical_off, sz] =
            raid0::next_subcmd(_stride_width, _stripe_size, addr + off, len - off);
        auto buf_cursor = static_cast< uint8_t* >(iovecs->iov_base) + off;
        off += sz;

        auto& acc = sub_cmds[stripe_off];
        if (!acc.nr_vecs) acc.io_addr = logical_off;
        DEBUG_ASSERT_LT(acc.nr_vecs, k_max_iovecs_per_stripe) // LCOV_EXCL_LINE
        acc.io_array[acc.nr_vecs++] = {buf_cursor, sz};

        // Dispatch once the remaining bytes fit within a single (N-1)-stripe remainder,
        // guaranteeing this stripe cannot accumulate more iovecs in the same call.
        if ((_stride_width - _stripe_size) >= (len - off)) {
            auto res = func(stripe_off, acc.io_array.data(), acc.nr_vecs, acc.io_addr);
            if (!res) return res;
            cnt += res.value();
        }
    }
    return cnt;
}

io_result Raid0Disk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    // RAID-0 only supports not-scattered I/O currently!
    if (1 > nr_vecs) return std::unexpected(std::make_error_condition(std::errc::invalid_argument));

    // Adjust the address for our superblock area, do not use _addr_ beyond this.
    addr += _stride_width;

    std::array< StripeAccum, _max_stripe_cnt > sub_cmds{};
    return __distribute(sub_cmds, iovecs, addr,
                        [op, this](uint32_t stripe_off, iovec* iov, uint32_t nr_iovs, uint64_t logical_off) {
                            RLOGT("Perform {}: ublk sync_io -> "
                                  "[stripe_off:{}|logical_sector:{}|logical_len:{:#0x}]",
                                  op == UBLK_IO_OP_READ ? "READ" : "WRITE", stripe_off, logical_off >> SECTOR_SHIFT,
                                  iovec_len(iov, iov + nr_iovs))
                            return _stripe_array[stripe_off]->disk->sync_iov(op, iov, nr_iovs, logical_off);
                        });
}

disk_task< int > Raid0Disk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                                      uint64_t addr) {
    auto const op = ublksrv_get_op(data->iod);

    if (op == UBLK_IO_OP_FLUSH) co_return 0;

    addr += _stride_width;

    // Eagerly start each child task so all SQEs are in-flight before the first co_await,
    // preserving kernel parallelism. All tasks must be drained even on error to avoid
    // dangling _waiter handles in cqe_state.
    std::vector< hot_task< int > > stripe_tasks;
    try {
        stripe_tasks.reserve(stripes_for_io(iovec_len(iovecs, iovecs + nr_vecs), _stripe_size, _stripe_array.size()));
    } catch (std::bad_alloc const&) { co_return -EAGAIN; } // LCOV_EXCL_LINE

    // sub_cmds is declared at function scope (not inside the else block) so its lifetime extends
    // past the if/else and covers the co_await loop below; iovec pointers into io_array remain
    // valid for the lifetime of all child tasks.
    std::array< StripeAccum, _max_stripe_cnt > sub_cmds{};

    if (op == UBLK_IO_OP_DISCARD || op == UBLK_IO_OP_WRITE_ZEROES) {
        uint32_t const len = (nr_vecs > 0) ? static_cast< uint32_t >(iovecs[0].iov_len) : 0;

        // No data buffer: contiguous stripe ranges can be coalesced rather than scattered per-stripe.
        for (auto const& [stripe_off, region] : raid0::merged_subcmds(_stride_width, _stripe_size, addr, len)) {
            auto const& [logical_off, logical_len] = region;
            // stripe_iov is a loop-local variable; start() advances async_iov past the iov_len
            // read before suspending, so the stack variable is safe.
            auto stripe_iov = iovec{.iov_base = nullptr, .iov_len = logical_len};
            stripe_tasks.push_back(
                _stripe_array[stripe_off]->disk->async_iov(q, data, &stripe_iov, 1, logical_off).start());
        }
    } else {
        // READ / WRITE: fan out across stripes via __distribute.
        auto res = __distribute(
            sub_cmds, iovecs, addr,
            [q, data, &stripe_tasks, this](uint32_t stripe_off, iovec* iov, uint32_t nr_iovs,
                                           uint64_t logical_off) -> io_result {
                stripe_tasks.push_back(
                    _stripe_array[stripe_off]->disk->async_iov(q, data, iov, nr_iovs, logical_off).start());
                return 1;
            });

        // INVARIANT: the lambda always returns io_result{1} so !res is unreachable today.
        // If the lambda ever becomes fallible, stripe_tasks must be drained here before returning
        // to avoid dangling cqe_state::_waiter handles in the CQE ring.
        if (!res) co_return -EIO;
    }

    int total = 0;
    int err = 0;
    for (auto& t : stripe_tasks) {
        auto r = co_await t;
        if (r < 0 && !err)
            err = r;
        else
            total += r;
    }
    co_return err ? err : total;
}

static const uint8_t magic_bytes[16] = {0127, 0345, 072,  0211, 0254, 033,  070,  0146,
                                        0125, 0377, 0204, 065,  0131, 0120, 0306, 047};
using raid0::k_sb_version;

static raid0::SuperBlock* read_superblock(ublk_disk& device) {
    auto const sb_size = sizeof(raid0::SuperBlock);
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
    return static_cast< raid0::SuperBlock* >(iov.iov_base);
}

static io_result write_superblock(ublk_disk& device, raid0::SuperBlock* sb) {
    auto const sb_size = sizeof(raid0::SuperBlock);
    RLOGT("Writing Superblock to: [{}]", device)
    DEBUG_ASSERT_EQ(0, sb_size % device.block_size(), "Device {} blocksize does not support alignment of [{}B]", device,
                    sb_size)
    auto iov = iovec{.iov_base = sb, .iov_len = sb_size};
    auto res = device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 0UL);
    if (!res) RLOGE("Error writing Superblock to: [{}]!", device, res.error().message())
    return res;
}

// Read and load the RAID0 superblock off a device. If it is not set, meaning the Magic is missing, then initialize
// the superblock to the current version. Otherwise migrate any changes needed after version discovery.
static std::expected< raid0::SuperBlock*, std::error_condition >
load_superblock(ublk_disk& device, boost::uuids::uuid const& uuid, uint32_t& stripe_size, uint16_t const stripe_off) {
    auto sb = read_superblock(device);
    if (!sb) return std::unexpected(std::make_error_condition(std::errc::io_error));

    // Check for MAGIC, initialize SB if missing
    if (memcmp(sb->header.magic, magic_bytes, sizeof(magic_bytes))) {
        RLOGI("Initializing RAID-0 on {} [stripe_size:{}KiB, uuid:{}]", device, stripe_size / Ki, to_string(uuid))
        memset(sb, 0x00, sizeof(raid0::SuperBlock));
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
        free(sb);
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    if (stripe_off != be16toh(sb->fields.stripe_off)) {
        RLOGE("Superblock does not match given array parameters: Expected [stripe_off:{}] != Found [stripe_off:{}]",
              stripe_off, be16toh(sb->fields.stripe_off))
        free(sb);
        return std::unexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    auto const read_stripe_size = be32toh(sb->fields.stripe_size);
    if (stripe_size != read_stripe_size) {
        RLOGW(
            "Superblock does not match given array parameters: Expected [stripe_sz:{:#0x}] != Found [stripe_sz:{:#0x}]",
            stripe_size, read_stripe_size)
        stripe_size = read_stripe_size;
    }
    auto const sb_ver = be16toh(sb->header.version);
    RLOGI("Loaded v{:#0x} superblock [stripe_sz:{}Ki, stripe_off:{}, uuid:{}] from: {}", sb_ver, stripe_size / Ki,
          stripe_off, to_string(uuid), device)
    // M3: reject superblocks written by a newer version of this code. A future format may add
    // fields before the existing ones, so silently processing would cause data corruption.
    if (sb_ver > k_sb_version) {
        RLOGE("Superblock version {:#0x} is newer than supported {:#0x} — refusing to open", sb_ver, k_sb_version)
        free(sb);
        return std::unexpected(std::make_error_condition(std::errc::not_supported));
    }

    // Migrating to latest version
    if (k_sb_version > sb_ver) {
        sb->header.version = htobe16(k_sb_version);
        if (!write_superblock(device, sb)) {
            free(sb);
            return std::unexpected(std::make_error_condition(std::errc::io_error));
        }
    }
    return sb;
}

std::shared_ptr< ublk_disk > make_raid0_disk(boost::uuids::uuid const& uuid, uint32_t stripe_size_bytes,
                                             std::vector< std::shared_ptr< ublk_disk > >&& disks) {
    return std::make_shared< Raid0Disk >(uuid, stripe_size_bytes, std::move(disks));
}

namespace raid0 {

std::shared_ptr< ublk_disk > get_device(ublk_disk const& disk, uint32_t stripe_offset) noexcept {
    auto const* r0 = dynamic_cast< Raid0Disk const* >(&disk);
    if (!r0) return nullptr;
    return r0->get_device(stripe_offset);
}

} // namespace raid0
} // namespace ublkpp
