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

    // If this is `true` the tgt will expect the Device to call `ublksrv_complete_io`
    bool uses_ublk_iouring{true};

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

    std::string to_string() const;

    // Async entry-point: called by __handle_io_async.
    // Implementations submit all SQEs upfront then co_await *state for each result.
    virtual disk_task< int > handle_io_async(ublksrv_queue const* q, ublk_io_data const* data);

    // Async I/O with explicit scatter-gather list and address. Called when the operation targets
    // a sub-range or offset that differs from what ublk_io_data describes — the caller owns the
    // buffer layout and address computation. All concrete leaf disks must override this.
    // For DISCARD, iovecs[0].iov_len is the length.
    virtual disk_task< int > handle_iov_async(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs,
                                              uint32_t nr_vecs, uint64_t addr) = 0;

    virtual std::string id() const noexcept = 0;

    /// Device Specific I/O Handlers
    virtual std::list< int > open_for_uring(ublksrv_queue const*, int const) { return {}; }

    virtual void idle_transition(ublksrv_queue const*, bool) {};

    virtual io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept = 0;
};

inline auto format_as(UblkDisk const& device) { return fmt::format("{}", device.to_string()); }
inline auto format_as(std::shared_ptr< UblkDisk > const& p) { return fmt::format("{}", *p); }

class DefunctDisk : public UblkDisk {
public:
    DefunctDisk();

    std::string id() const noexcept override;

    disk_task< int > handle_iov_async(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                                      uint64_t addr) override;

    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
};

} // namespace ublkpp
