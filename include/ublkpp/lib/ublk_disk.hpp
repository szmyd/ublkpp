#pragma once

#include <expected>
#include <list>
#include <memory>
#include <string>

#include "common.hpp"
#include "sub_cmd.hpp"

struct iovec;
struct ublk_io_data;
struct ublksrv_queue;
struct ublk_params;

namespace ublkpp {

struct async_result {
    ublk_io_data const* io;
    sub_cmd_t sub_cmd;
    int result;
};

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

    // Target entry-point for I/O
    io_result queue_tgt_io(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd);

    // Internal result response
    io_result queue_internal_resp(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, int res);

    virtual std::string id() const noexcept = 0;

    /// Device Specific I/O Handlers
    virtual std::list< int > open_for_uring(int const) { return {}; }

    // Number of bits for sub_cmd routing in the sqe user_data
    virtual uint8_t route_size() const noexcept { return 0; }

    // Async replies collected here
    virtual void collect_async(ublksrv_queue const*, std::list< async_result >&) {} // LCOV_EXCL_LINE

    virtual void idle_transition(ublksrv_queue const*, bool) {};

    // Called when an I/O completes - allows devices to track their own metrics
    virtual void on_io_complete(ublk_io_data const*, sub_cmd_t, int) {}

    virtual io_result handle_internal(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd,
                                      iovec* iovecs, uint32_t nr_vecs, uint64_t addr, int res);

    virtual io_result handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) = 0;

    virtual io_result handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                                     uint64_t addr) = 0;

    virtual io_result async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                                uint32_t nr_vecs, uint64_t addr) = 0;

    virtual io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept = 0;

    /// Deprecated Sync I/O calls
    io_result handle_rw(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, void* buf,
                        uint32_t const len, uint64_t const addr);
    io_result sync_io(uint8_t op, void* buf, size_t len, off_t addr) noexcept;
    ///
};

inline auto format_as(UblkDisk const& device) { return fmt::format("{}", device.to_string()); }
inline auto format_as(std::shared_ptr< UblkDisk > const& p) { return fmt::format("{}", *p); }

class DefunctDisk : public UblkDisk {
public:
    DefunctDisk();

    std::string id() const noexcept override;

    io_result handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) override;
    io_result handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                             uint64_t addr) override;

    io_result async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                        uint32_t nr_vecs, uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
};

} // namespace ublkpp
