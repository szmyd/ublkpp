#pragma once

#include <boost/uuid/random_generator.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <ublksrv.h>

#include "ublkpp/lib/cqe_state.hpp"
#include "ublkpp/lib/ublk_disk.hpp"
#include "ublkpp/raid.hpp"
#include "raid/raid0/raid0_impl.hpp"
#include "raid/tests/raid_test_common.hpp"
#include "tests/mock_ublksrv/mock_ublksrv.hpp"
#include "tests/test_disk.hpp"

using ::testing::_;
using ::testing::Return;
using ::ublkpp::Gi;
using ::ublkpp::io_result;
using ::ublkpp::Ki;
using ::ublkpp::ublk_disk;
using ::ublkpp::test::make_async_iov_action;

struct AsyncRaid0Fixture : public ::testing::Test {
    static constexpr uint32_t k_stripe_size = 32 * Ki;
    static constexpr uint64_t k_disk_cap = 1 * Gi;

    std::shared_ptr< ublkpp::AsyncTestDisk > disk_a, disk_b, disk_c;
    std::shared_ptr< ublkpp::ublk_disk > raid;
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
            ON_CALL(*d, submit_iov(_, _, _, _, _)).WillByDefault(make_async_iov_action());
        }
        raid = ublkpp::make_raid0_disk(boost::uuids::random_generator()(), k_stripe_size,
                                       std::vector< std::shared_ptr< ublk_disk > >{disk_a, disk_b, disk_c});
        mock = std::make_unique< ublkpp::MockUblksrv >(raid);
    }
};
