#pragma once

#include "ublkpp/lib/cqe_state.hpp"
#include "ublkpp/lib/ublk_disk.hpp"

#include <sisl/logging/logging.h>
#include <ublksrv.h>

using ::ublkpp::ilog2;
using ::ublkpp::Ki;

struct TestParams {
    uint64_t capacity{0};
    std::string const id = "TestDisk";
    uint32_t l_size{ublkpp::DEFAULT_BLOCK_SIZE};
    uint32_t p_size{ublkpp::DEFAULT_BLOCK_SIZE};
    uint32_t max_io{512 * Ki};
    bool can_discard{true};
    bool direct_io{true};
    bool is_slot_b{false};
};

namespace ublkpp {

class TestDisk : public UblkDisk {
public:
    std::string my_id;
    explicit TestDisk(TestParams const& test_params) : UblkDisk(), my_id(test_params.id) {
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
    std::string id() const noexcept override { return my_id; }

    MOCK_METHOD(std::list< int >, open_for_uring, (ublksrv_queue const*, int const), (override));
    MOCK_METHOD(void, idle_transition, (ublksrv_queue const*, bool), (override));

    MOCK_METHOD(io_result, async_iov, (ublksrv_queue const*, ublk_io_data const*, iovec*, uint32_t, uint64_t));

    MOCK_METHOD(io_result, sync_iov, (uint8_t, iovec*, uint32_t, off_t offset), (override, noexcept));

    disk_task< int > handle_iov_async(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                                      uint64_t addr) override {
        auto* io = reinterpret_cast< async_io* >(data->private_data);
        auto res = async_iov(q, data, iovecs, nr_vecs, addr);
        if (!res) co_return -static_cast< int >(res.error().value());
        if (res.value() == 0) co_return 0;
        io_uring_submit(q->ring_ptr);
        co_return co_await *io->next_state();
    }
};

class AsyncTestDisk : public TestDisk {
public:
    explicit AsyncTestDisk(TestParams const& p) : TestDisk(p) {}
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
