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
        _direct_io = test_params.direct_io;
    }
    std::string id() const noexcept override { return my_id; }

    MOCK_METHOD(prepare_result, prepare, (ublk_rings const*, int const), (override));
    MOCK_METHOD(void, probe_tick, (ublksrv_queue const*), (noexcept, override));

    MOCK_METHOD(io_result, submit_iov, (ublksrv_queue const*, ublk_io_data const*, iovec*, uint32_t, uint64_t));

    MOCK_METHOD(io_result, sync_iov, (uint8_t, iovec*, uint32_t, off_t offset), (override, noexcept));

    // Synchronous adapter: delegates to the submit_iov mock and returns immediately.
    // Tests exercise business logic (Phase 1/2 conflict, skip_from, bitmap updates);
    // actual io_uring mechanics are tested separately via async_resync_iouring.cpp.
    disk_task< int > async_iov(ublksrv_queue const*, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override {
        auto res = submit_iov(nullptr, data, iovecs, nr_vecs, addr);
        co_return res ? static_cast< int >(res.value()) : -static_cast< int >(res.error().value());
    }
};

class AsyncTestDisk : public TestDisk {
public:
    explicit AsyncTestDisk(TestParams const& p) : TestDisk(p) {}

    // Async override that drives the cqe_state machinery.
    // • Normal IO slots (_tag >= 0): allocates a cqe_state and suspends; inject_cqe()
    //   delivers the result, which unblocks the RAID1 write coroutine.
    // • Resync slots (_tag == -1): calls sync_iov() directly and marks the state
    //   ready immediately so the resync coroutine completes without suspending.
    disk_task< int > async_iov(ublksrv_queue const*, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override {
        auto* io = reinterpret_cast< async_io* >(data->private_data);
        auto [state, _unused] = build_cqe_state_data(data);
        if (io->_tag == -1) {
            auto res = sync_iov(ublksrv_get_op(data->iod), iovecs, nr_vecs, static_cast< off_t >(addr));
            state->_result = res ? static_cast< int >(res.value()) : -static_cast< int >(res.error().value());
            state->_result_ready = true;
        }
        co_return co_await *state;
    }
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
