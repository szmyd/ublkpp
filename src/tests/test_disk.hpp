#pragma once

#include "ublkpp/lib/cqe_state.hpp"
#include "ublkpp/lib/ublk_disk.hpp"

#include <sisl/logging/logging.h>
#include <ublksrv.h>

#include "lib/common.hpp"

using ::ublkpp::ilog2;
using ::ublkpp::Ki;

struct TestParams {
    uint64_t capacity{0};
    std::string const id = "TestDisk";
    uint32_t l_size{ublkpp::DEFAULT_BLOCK_SIZE};
    uint32_t p_size{ublkpp::DEFAULT_BLOCK_SIZE};
    uint32_t max_io{512 * Ki};
    uint32_t max_discard_sectors{UINT32_MAX >> 9}; // UINT_MAX >> SECTOR_SHIFT default
    bool can_discard{true};
    bool direct_io{true};
    bool is_slot_b{false};
};

namespace ublkpp {

class TestDisk : public ublk_disk {
public:
    std::string my_id;
    explicit TestDisk(TestParams const& test_params) : ublk_disk(), my_id(test_params.id) {
        auto& our_params = *params();
        our_params.basic.dev_sectors = test_params.capacity >> SECTOR_SHIFT;
        our_params.basic.logical_bs_shift = ilog2(test_params.l_size);
        our_params.basic.physical_bs_shift = ilog2(test_params.p_size);
        our_params.basic.max_sectors = test_params.max_io >> SECTOR_SHIFT;
        if (!test_params.can_discard)
            our_params.types &= ~UBLK_PARAM_TYPE_DISCARD;
        else
            our_params.types |= UBLK_PARAM_TYPE_DISCARD;
        our_params.discard.max_discard_sectors = test_params.max_discard_sectors;
        _direct_io = test_params.direct_io;
    }
    std::string id() const noexcept override { return my_id; }

    MOCK_METHOD(prepare_result, prepare, (ublksrv_queue const*, int const), (override));
    MOCK_METHOD(void, probe_tick, (ublksrv_queue const*), (noexcept, override));

    MOCK_METHOD(io_result, submit_iov, (ublksrv_queue const*, ublk_io_data const*, iovec*, uint32_t, uint64_t));

    MOCK_METHOD(io_result, sync_iov, (uint8_t, iovec*, uint32_t, off_t offset), (override, noexcept));

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override {
        auto* io = reinterpret_cast< async_io* >(data->private_data);
        auto res = submit_iov(q, data, iovecs, nr_vecs, addr);
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
