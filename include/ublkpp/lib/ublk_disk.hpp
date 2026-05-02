#pragma once

#include <expected>
#include <list>
#include <memory>
#include <string>

#include <fmt/format.h>

#include "common.hpp"
#include "disk_task.hpp"

struct iovec;
struct ublk_io_data;
struct ublksrv_queue;
struct ublk_params;

namespace ublkpp {

using io_result = std::expected< size_t, std::error_condition >;
class UblkDisk : public std::enable_shared_from_this< UblkDisk > {
    std::unique_ptr< ublk_params > _params;

public:
    bool direct_io{false};

    // True if this disk's async_iov stages SQEs into the queue's io_uring (q->ring_ptr),
    // so completion is driven by the queue thread's submit_and_wait_timeout loop. Set
    // false by drivers that perform IO out-of-band (private threads, separate event
    // loops, sync syscalls). Composite (non-leaf) disks aggregate this from their
    // leaves with ANY semantics: true if at least one leaf stages SQEs into q->ring_ptr,
    // since one io_uring leaf is enough to require a flush before reusing per-IO scratch.
    //
    // Caveat: this flag is computed at construction. Composite disks that swap leaves
    // at runtime (Raid1Disk::swap_device, etc.) do not currently re-aggregate it. Safe
    // today only because no live consumer branches on this per-IO; revive aggregate
    // refresh through swap when introducing such a consumer (e.g. a Raid0 submit guard
    // or a tgt-level autocomplete fast-path).
    bool uses_queue_uring{true};

    UblkDisk();
    virtual ~UblkDisk();

    // Constant parameters for device
    // ================
    virtual uint32_t block_size() const noexcept;
    virtual uint32_t max_tx() const noexcept;
    virtual bool can_discard() const noexcept;
    virtual uint64_t capacity() const noexcept;
    // ================

    virtual ublk_params* params() noexcept { return _params.get(); }
    virtual ublk_params const* params() const noexcept { return _params.get(); }
    virtual std::string id() const noexcept = 0;

    std::string to_string() const;

    // Async I/O with explicit scatter-gather list and address. Called when the operation targets
    // a sub-range or offset that differs from what ublk_io_data describes — the caller owns the
    // buffer layout and address computation. All concrete leaf disks must override this.
    // For DISCARD, iovecs[0].iov_len is the length.
    virtual disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                       uint32_t nr_vecs, uint64_t addr) = 0;

    virtual io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept = 0;

    // Initialize Device for io_uring
    virtual std::list< int > prepare(ublksrv_queue const*, int const) { return {}; }

    // I/O has become idle event
    virtual void idle_transition(ublksrv_queue const*, bool) {};
};

inline auto format_as(UblkDisk const& device) { return fmt::format("{}", device.to_string()); }
inline auto format_as(std::shared_ptr< UblkDisk > const& p) { return fmt::format("{}", *p); }

class DefunctDisk : public UblkDisk {
public:
    DefunctDisk();

    std::string id() const noexcept override;

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override;

    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
};

} // namespace ublkpp
