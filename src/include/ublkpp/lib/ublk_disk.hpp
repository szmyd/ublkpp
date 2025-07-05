#pragma once

#include <list>
#include <memory>
#include <string>

#include <folly/Expected.h>
#include <ublk_cmd.h>

#include "common.hpp"
#include "sub_cmd.hpp"

struct iovec;
struct ublk_io_data;
struct ublksrv_queue;

namespace ublkpp {

using io_result = folly::Expected< size_t, std::error_condition >;
class UblkDisk : public std::enable_shared_from_this< UblkDisk > {
    ublk_params _params;

public:
    bool direct_io{false};

    explicit UblkDisk();
    virtual ~UblkDisk() = default;

    // Constant parameters for device
    uint32_t block_size() const { return 1 << _params.basic.logical_bs_shift; }
    bool can_discard() const { return _params.types & UBLK_PARAM_TYPE_DISCARD; }

    ublk_params* params() { return &_params; }
    ublk_params const* params() const { return &_params; }
    uint64_t capacity() const { return _params.basic.dev_sectors << SECTOR_SHIFT; }

    // Target entry-point for I/O
    io_result queue_tgt_io(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd);

    virtual std::string type() const = 0;

    /// Device Specific I/O Handlers
    virtual std::list< int > open_for_uring(int const iouring_device_start) = 0;

    // Number of bits for sub_cmd routing in the sqe user_data
    virtual uint8_t route_size() const { return 0; }

    virtual io_result handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) = 0;

    virtual io_result handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                                     uint64_t addr) = 0;

    virtual io_result async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                                uint32_t nr_vecs, uint64_t addr) = 0;

    virtual io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept = 0;

    /// Deprecated Sync I/O calls
    io_result handle_rw(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, void* buf,
                        uint32_t const len, uint64_t const addr);
    io_result sync_io(uint8_t op, void* buf, size_t len, off_t addr);
    ///
};

} // namespace ublkpp

namespace fmt {
template <>
struct formatter< ublkpp::UblkDisk > {
    template < typename ParseContext >
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template < typename FormatContext >
    auto format(ublkpp::UblkDisk const& device, FormatContext& ctx) {
        return fmt::format_to(ctx.out(), "{}: params:[cap={},lbs={},pbs={},discard={},direct={}]", device.type(),
                              device.capacity(), device.block_size(), 1 << device.params()->basic.physical_bs_shift,
                              device.can_discard(), device.direct_io);
    }
};

template <>
struct formatter< std::shared_ptr< ublkpp::UblkDisk > > {
    template < typename ParseContext >
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template < typename FormatContext >
    auto format(std::shared_ptr< ublkpp::UblkDisk > const& device_ptr, FormatContext& ctx) {
        return fmt::format_to(ctx.out(), "{}", *device_ptr);
    }
};
} // namespace fmt
