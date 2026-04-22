#pragma once

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
    bool is_slot_b{false}; // True if device is in RAID1 slot B (expects sub_cmd bit=1)
};

namespace ublkpp {

class TestDisk : public UblkDisk {
public:
    std::string my_id;
    bool expected_slot_b; // True if this device is in RAID1 slot B
    explicit TestDisk(TestParams const& test_params) :
            UblkDisk(), my_id(test_params.id), expected_slot_b(test_params.is_slot_b) {
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

private:
    // Validate that sub_cmd bit matches expected slot for RAID1 devices
    // Slot A devices should ALWAYS see bit=0, slot B should ALWAYS see bit=1
    void validate_slot(sub_cmd_t sub_cmd, const char* method) const {
        if (route_size() == 0) return; // Not a RAID device, skip validation

        bool const sub_cmd_is_slot_b = (sub_cmd & 0b1) != 0;
        RELEASE_ASSERT_EQ(
            expected_slot_b, sub_cmd_is_slot_b, "Device {} (slot {}) received {} with wrong sub_cmd bit={} (slot {})",
            my_id, expected_slot_b ? "B" : "A", method, sub_cmd_is_slot_b ? "1" : "0", sub_cmd_is_slot_b ? "B" : "A");
    }

public:
    // Override on_io_complete to validate slot routing (only place with the bug)
    void on_io_complete(ublk_io_data const*, sub_cmd_t sub_cmd, int) override {
        validate_slot(sub_cmd, "on_io_complete");
    }

    MOCK_METHOD(std::list< int >, open_for_uring, (ublksrv_queue const*, int const), (override));
    MOCK_METHOD(io_result, handle_internal,
                (ublksrv_queue const*, ublk_io_data const*, sub_cmd_t, iovec*, uint32_t, uint64_t, int), (override));
    MOCK_METHOD(void, collect_async, (ublksrv_queue const*, std::list< async_result >&), (override));
    MOCK_METHOD(void, idle_transition, (ublksrv_queue const*, bool), (override));
    MOCK_METHOD(io_result, handle_flush, (ublksrv_queue const*, ublk_io_data const*, sub_cmd_t), (override));
    MOCK_METHOD(io_result, handle_discard, (ublksrv_queue const*, ublk_io_data const*, sub_cmd_t, uint32_t, uint64_t),
                (override));

    MOCK_METHOD(io_result, async_iov,
                (ublksrv_queue const*, ublk_io_data const*, sub_cmd_t, iovec*, uint32_t, uint64_t), (override));

    MOCK_METHOD(io_result, sync_iov, (uint8_t, iovec*, uint32_t, off_t offset), (override, noexcept));

    uint8_t route_size() const noexcept override { return 0; }
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
