#pragma once

#include "ublkpp/lib/ublk_disk.hpp"

#include <ublksrv.h>

using ::ublkpp::ilog2;
using ::ublkpp::Ki;

struct TestParams {
    uint64_t capacity{0};
    uint32_t l_size{ublkpp::DEFAULT_BLOCK_SIZE};
    uint32_t p_size{ublkpp::DEFAULT_BLOCK_SIZE};
    uint32_t max_io{512 * Ki};
    bool can_discard{true};
    bool direct_io{true};
};

namespace ublkpp {

class TestDisk : public UblkDisk {
public:
    explicit TestDisk(TestParams const& test_params) : UblkDisk() {
        auto& our_params = *params();
        our_params.basic.dev_sectors = test_params.capacity >> SECTOR_SHIFT;
        our_params.basic.logical_bs_shift = ilog2(test_params.l_size);
        our_params.basic.physical_bs_shift = ilog2(test_params.p_size);
        our_params.basic.max_sectors = test_params.max_io >> SECTOR_SHIFT;
        if (!test_params.can_discard)
            our_params.types &= ~UBLK_PARAM_TYPE_DISCARD;
        else
            our_params.types |= UBLK_PARAM_TYPE_DISCARD;
        direct_io = test_params.direct_io;
    }
    std::string type() const override { return std::string("TestDisk"); }

    MOCK_METHOD(std::list< int >, open_for_uring, (int const), (override));
    MOCK_METHOD(void, collect_async, (ublksrv_queue const*, std::list< async_result >&), (override));
    MOCK_METHOD(io_result, handle_flush, (ublksrv_queue const*, ublk_io_data const*, sub_cmd_t), (override));
    MOCK_METHOD(io_result, handle_discard, (ublksrv_queue const*, ublk_io_data const*, sub_cmd_t, uint32_t, uint64_t),
                (override));

    MOCK_METHOD(io_result, async_iov,
                (ublksrv_queue const*, ublk_io_data const*, sub_cmd_t, iovec*, uint32_t, uint64_t), (override));

    MOCK_METHOD(io_result, sync_iov, (uint8_t, iovec*, uint32_t, off_t offset), (override, noexcept));

    uint8_t route_size() const override { return 0; }
};

}; // namespace ublkpp

inline ublk_io_data make_io_data(uint32_t op_flags, uint32_t len = 0, uint64_t start = 0) {
    static int rolling_tag = 0;
    return ublk_io_data{
        .tag = ++rolling_tag,
        .pad = 0,
        .iod =
            new ublksrv_io_desc{
                .op_flags = op_flags,
                .nr_sectors = len >> ublkpp::SECTOR_SHIFT,
                .start_sector = start >> ublkpp::SECTOR_SHIFT,
                .addr = 0,
            },
        .private_data = nullptr,
    };
}

inline void remove_io_data(ublk_io_data& data) {
    if (data.iod) delete data.iod;
}
