#pragma once

#include <expected>
#include <memory>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <sisl/logging/logging.h>

#include "disk_task.hpp"
#include <ublkpp/lib/ublk_params.hpp>

struct iovec;
struct ublk_io_data;
struct ublksrv_queue;

namespace ublkpp {

namespace detail {
// Forward declaration only. The actual accessor is a static member of this struct, defined in
// src/lib/internal/ublkpp_int.hpp (not installed) and used exclusively by ublksrv handshake
// code in src/target. Befriending a forward-declared struct keeps the accessor's signature out
// of the public header entirely.
struct params_access;
} // namespace detail

using io_result = std::expected< size_t, std::error_condition >;

class ublk_disk;
using disk_handle = std::shared_ptr< ublk_disk >;

// Base class for every disk in the stack (leaf drivers and RAID composites alike).
//
// Construction is restricted to subclasses; consumers obtain instances via the factory
// functions in <ublkpp/drivers.hpp> and <ublkpp/raid.hpp>. For a placeholder representing
// an absent mirror leg, use ublkpp::make_missing_disk(); never default-construct directly.
class ublk_disk {
    std::unique_ptr< ublk_params > _params;

    friend struct detail::params_access;

protected:
    bool _is_missing{false};
    bool _direct_io{false};

    ublk_disk();

    // Subclass ctors mutate _params here to declare the disk's geometry.
    ublk_params* params() noexcept { return _params.get(); }
    ublk_params const* params() const noexcept { return _params.get(); }

public:
    virtual ~ublk_disk();

    // Constant parameters for device
    // ================
    uint32_t block_size() const noexcept;
    uint32_t physical_block_size() const noexcept;
    uint32_t max_tx() const noexcept;
    bool can_discard() const noexcept;
    uint32_t discard_granularity() const noexcept;
    uint64_t capacity() const noexcept;
    bool direct_io() const noexcept { return _direct_io; }
    bool is_missing() const noexcept { return _is_missing; }
    // ================

    // Defaults below are safety nets for the missing-disk case. Real subclasses MUST override.
    // Asserting in debug catches subclasses that forgot to implement; release returns a benign
    // failure so a missing leg routed-to by mistake just errors instead of crashing.

    virtual std::string id() const noexcept {
        RELEASE_ASSERT(_is_missing, "id() called on a non-missing ublk_disk that did not override");
        return "~MISSING~";
    }

    // Async I/O with explicit scatter-gather list and address. Called when the operation targets
    // a sub-range or offset that differs from what ublk_io_data describes; the caller owns the
    // buffer layout and address computation.
    // For DISCARD, iovecs[0].iov_len is the length.
    // INVARIANT: implementations must copy iov_len out of every iovec before their first co_await.
    // Callers (e.g. Raid0Disk) may pass a pointer to a loop-local iovec that is destroyed after
    // start() returns; any access past the first suspension point is a use-after-free.
    virtual disk_task< int > async_iov(ublksrv_queue const* /*q*/, ublk_io_data const* /*data*/, iovec* /*iovecs*/,
                                       uint32_t /*nr_vecs*/, uint64_t /*addr*/) {
        RELEASE_ASSERT(_is_missing, "async_iov() called on a non-missing ublk_disk that did not override");
        co_return -EIO;
    }

    virtual io_result sync_iov(uint8_t /*op*/, iovec* /*iovecs*/, uint32_t /*nr_vecs*/, off_t /*addr*/) noexcept {
        RELEASE_ASSERT(_is_missing, "sync_iov() called on a non-missing ublk_disk that did not override");
        return std::unexpected(std::make_error_condition(std::errc::io_error));
    }

    // Returns the raw backing file descriptor for use by out-of-band io_uring rings (e.g. the
    // resync task's dedicated ring). Returns -1 for composite disks and test mocks that have
    // no single underlying fd; callers fall back to sync_iov in that case.
    virtual int backend_fd() const noexcept { return -1; }

    // Returned by prepare(). Carries the file descriptors to register in the queue's io_uring
    // fixed-file table and the maximum number of SQEs this disk may submit for a single user I/O.
    // The target uses max_sqes_per_io to pre-reserve async_io::_pool at queue-init time so that
    // push_back during I/O never reallocates and cqe_state* pointers in SQE user_data stay stable.
    struct prepare_result {
        std::vector< int > fds{};
        size_t max_sqes_per_io{1};
    };

    // Called once per queue at startup. Returns file descriptors to register in the queue's
    // io_uring fixed-file table (kernel assigns indices starting at iouring_device_start) and the
    // SQE ceiling for pre-reserving the per-tag cqe_state pool. Composite drivers propagate to
    // children, concatenating fds and combining max_sqes_per_io.
    // Default: no FDs, 1 SQE (subclasses that submit 0 or >1 SQEs per I/O must override).
    virtual prepare_result prepare(ublksrv_queue const* /*q*/, int const /*iouring_device_start*/) { return {}; }

    // Called by run_queue_loop when a probe timeout CQE fires. Probes ALL mirrors on every tick,
    // not only unavail ones; so silent healthy-to-failed transitions are detected within
    // k_io_idle_secs, not only after the next user I/O hits the failed device. No-op when
    // is_degraded: the resync task owns health monitoring in that state.
    // Composite drivers propagate to children. Default: no-op.
    virtual void probe_tick(ublksrv_queue const* /*q*/) noexcept {}
};

inline auto format_as(ublk_disk const& device) {
    constexpr uint64_t kMi = 1ULL << 20;
    constexpr uint64_t kGi = 1ULL << 30;
    constexpr uint64_t kTi = 1ULL << 40;
    auto const cap = device.capacity();
    auto const cap_denom = cap >= kTi ? kGi : kMi;
    return fmt::format("[{}, size={}{}, lbs={:#0x}]", device.id(), cap / cap_denom, cap_denom == kGi ? "Gi" : "Mi",
                       device.block_size());
}
inline auto format_as(std::shared_ptr< ublk_disk > const& p) { return format_as(*p); }

} // namespace ublkpp
