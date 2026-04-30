#pragma once

#include <boost/uuid/random_generator.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <ublksrv.h>

#include "ublkpp/lib/cqe_state.hpp"
#include "ublkpp/raid/raid0.hpp"
#include "raid/raid0/raid0_impl.hpp"
#include "tests/mock_ublksrv/mock_ublksrv.hpp"
#include "tests/test_disk.hpp"

using ::testing::_;
using ::testing::Return;
using ::ublkpp::Gi;
using ::ublkpp::io_result;
using ::ublkpp::Ki;
using ::ublkpp::UblkDisk;

// Default async_iov action: registers the CqeState in the pool without submitting a real SQE.
// Tests call inject_cqe() to deliver synthetic results stripe-by-stripe.
inline auto make_async_iov_action() {
    return [](ublksrv_queue const*, ublk_io_data const* data, ublkpp::sub_cmd_t sub_cmd, iovec*, uint32_t,
              uint64_t) -> io_result {
        ublkpp::build_cqe_state_data(data, static_cast< uint64_t >(sub_cmd));
        return 1;
    };
}

struct AsyncRaid0Fixture : public ::testing::Test {
    static constexpr uint32_t k_stripe_size = 32 * Ki;
    static constexpr uint64_t k_disk_cap = 1 * Gi;

    std::shared_ptr< ublkpp::AsyncTestDisk > disk_a, disk_b, disk_c;
    std::shared_ptr< ublkpp::Raid0Disk > raid;
    std::unique_ptr< ublkpp::MockUblksrv > mock;

    void SetUp() override {
        TestParams const p{.capacity = k_disk_cap};
        disk_a = std::make_shared< ::testing::NiceMock< ublkpp::AsyncTestDisk > >(p);
        disk_b = std::make_shared< ::testing::NiceMock< ublkpp::AsyncTestDisk > >(p);
        disk_c = std::make_shared< ::testing::NiceMock< ublkpp::AsyncTestDisk > >(p);
        for (auto& d : {disk_a, disk_b, disk_c}) {
            ON_CALL(*d, sync_iov(_, _, _, _))
                .WillByDefault([](uint8_t op, iovec* iovecs, uint32_t, off_t) -> io_result {
                    if (op == UBLK_IO_OP_READ && iovecs && iovecs->iov_base)
                        memset(iovecs->iov_base, 0, iovecs->iov_len);
                    return sizeof(ublkpp::raid0::SuperBlock);
                });
            ON_CALL(*d, async_iov(_, _, _, _, _, _)).WillByDefault(make_async_iov_action());
            ON_CALL(*d, handle_flush(_, _, _)).WillByDefault(Return(0));
            ON_CALL(*d, handle_discard(_, _, _, _, _)).WillByDefault(Return(1));
        }
        raid =
            std::make_shared< ublkpp::Raid0Disk >(boost::uuids::random_generator()(), k_stripe_size,
                                                  std::vector< std::shared_ptr< UblkDisk > >{disk_a, disk_b, disk_c});
        mock = std::make_unique< ublkpp::MockUblksrv >(raid);
    }
};
