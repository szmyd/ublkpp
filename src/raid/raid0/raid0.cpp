#include "ublkpp/raid.hpp"

#include <bit>
#include <boost/uuid/uuid_io.hpp>
#include <ublksrv.h>
#include <ublksrv_utils.h>

#include <ublkpp/lib/disk_task.hpp>
#include <ublkpp/lib/ublk_disk.hpp>

#include "raid0_impl.hpp"
#include "lib/logging.hpp"

namespace ublkpp {
constexpr uint32_t _max_stripe_cnt{64};

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
    uint32_t _stride_width{0};

    io_result __distribute(iovec* iov, uint64_t addr, auto&& func) const;

public:
    Raid0Disk(boost::uuids::uuid const& uuid, uint32_t const stripe_size_bytes,
              std::vector< std::shared_ptr< ublk_disk > >&& disks);
    ~Raid0Disk() override;

    std::shared_ptr< ublk_disk > get_device(uint32_t stripe_offset) const noexcept;
    uint32_t stripe_size() const noexcept { return _stripe_size; }

    std::string id() const noexcept override { return "RAID0"; }
    std::vector< int > prepare(ublksrv_queue const*, int const iouring_device) override;

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override;

    void probe_tick(ublksrv_queue const* q) noexcept override;

    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
};

Raid0Disk::Raid0Disk(boost::uuids::uuid const& uuid, uint32_t const stripe_size_bytes,
                     std::vector< std::shared_ptr< ublk_disk > >&& disks) :
        ublk_disk(), _stripe_size(stripe_size_bytes), _stride_width(_stripe_size * disks.size()) {
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

    // Finally we'll calculate the volume size as a multiple of the smallest array device
    // and adjust to account for the superblock we will write at the HEAD of each array device.
    // To keep things simple, we'll just use the first chunk from each device for ourselves.
    our_params.basic.dev_sectors -= (_stripe_size >> SECTOR_SHIFT);
    our_params.basic.dev_sectors *= _stripe_array.size();
    // Align size to max_sector size
    our_params.basic.dev_sectors -= (our_params.basic.dev_sectors % our_params.basic.max_sectors);

    if (can_discard())
        our_params.discard.discard_granularity = std::max(our_params.discard.discard_granularity, block_size());
}

Raid0Disk::~Raid0Disk() = default;

std::shared_ptr< ublk_disk > Raid0Disk::get_device(uint32_t stripe_offset) const noexcept {
    if (auto const width = _stripe_array.size(); width <= stripe_offset) {
        RLOGW("Stripe offset [{}] larger than array width [{}]", stripe_offset, width)
        return nullptr;
    }
    return _stripe_array[stripe_offset]->disk;
}

std::vector< int > Raid0Disk::prepare(ublksrv_queue const* q, int const iouring_device_start) {
    auto fds = std::vector< int >();
    for (auto& stripe : _stripe_array) {
        auto child = stripe->disk->prepare(q, iouring_device_start + fds.size());
        fds.insert(fds.end(), child.begin(), child.end());
    }
    return fds;
}

void Raid0Disk::probe_tick(ublksrv_queue const* q) noexcept {
    for (auto const& stripe : _stripe_array) {
        stripe->disk->probe_tick(q);
    }
}

// Upper bound on distinct stripes touched by a single I/O.
// +1 for start-alignment: an unaligned start can spill into one extra stripe.
// Capped by device count because __distribute gathers all iovecs per device into one task.
static inline size_t stripes_for_io(size_t io_size, size_t stride_width, size_t nr_stripes) {
    return std::min((io_size + stride_width - 1) / stride_width + 1, nr_stripes);
}

/// This is the primary I/O handler call for RAID0
//
//  RAID0 is primarily responsible for splitting an I/O request across several stripes. These operations can cross
//  stripe boundaries and even wrap around several strides. This routine handles this calculation and calls
//  the given routine `func` for each stripe that it has collected scatter (struct iovec) operations for.
io_result Raid0Disk::__distribute(iovec* iovecs, uint64_t addr, auto&& func) const {
    // We gather all the pieces of each I/O intended to dispatch using this structure
    struct StripeAccum {
        uint64_t io_addr{0};                // Starting address
        uint32_t nr_vecs{0};                // How many iovecs are valid
        std::array< iovec, 16 > io_array{}; // The scatter-list
    };
    static_assert(_max_stripe_cnt == 64, "dirty_mask must be exactly uint64_t");

    // Then when allocated (once) an array of accumulators for the thread (1-thread per i/o queue)
    thread_local auto sub_cmds = std::array< StripeAccum, _max_stripe_cnt >();

    // Reset only touched stripes on exit; a failed call leaves non-zero nr_vecs that would corrupt the next I/O.
    uint64_t dirty_mask{0};
    struct DirtyGuard {
        decltype(sub_cmds)& cmds;
        uint64_t& mask;
        ~DirtyGuard() noexcept {
            while (mask) {
                cmds[std::countr_zero(mask)].nr_vecs = 0;
                mask &= mask - 1; // clear lowest set bit
            }
        }
    } guard{sub_cmds, dirty_mask};

    if (1 == _stripe_array.size()) return func(0, iovecs, 1, addr);

    DEBUG_ASSERT_LE(iovecs->iov_len, UINT32_MAX) // LCOV_EXCL_LINE
    auto const len = static_cast< uint32_t >(iovecs->iov_len);
    uint32_t cnt{0};
    for (auto off = 0U; len > off;) {
        auto const [stripe_off, logical_off, sz] =
            raid0::next_subcmd(_stride_width, _stripe_size, addr + off, len - off);
        auto buf_cursor = static_cast< uint8_t* >(iovecs->iov_base) + off;
        off += sz;

        dirty_mask |= 1ULL << stripe_off;
        auto& acc = sub_cmds[stripe_off];
        if (!acc.nr_vecs) acc.io_addr = logical_off;
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

    return __distribute(iovecs, addr,
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
        stripe_tasks.reserve(stripes_for_io(iovec_len(iovecs, iovecs + nr_vecs), _stride_width, _stripe_array.size()));
    } catch (std::bad_alloc const&) { co_return -ENOMEM; }

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
            iovecs, addr,
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

    // Submit now so the kernel snapshots leaf-staged iovecs that point into thread_local sub_cmds
    // (in __distribute) before this coroutine suspends. Without this submit, sibling Raid0 IOs on
    // the same thread would overwrite sub_cmds before the queue loop's submit_and_wait_timeout
    // hands the SQEs to the kernel, corrupting in-flight writev/readv. One extra syscall per
    // multi-stripe RAID0 IO; FSDisk-only and single-stripe paths are unaffected.
    if (q && q->ring_ptr) io_uring_submit(q->ring_ptr);

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
constexpr auto SB_VERSION = 1;

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

    // Migrating to latest version
    if (SB_VERSION > sb_ver) {
        sb->header.version = htobe16(SB_VERSION);
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
