#pragma once

#include <boost/uuid/random_generator.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <ublksrv.h>

#include "ublkpp/raid/raid0.hpp"
#include "raid/raid0_impl.hpp"
#include "raid/superblock.hpp"
#include "tests/test_disk.hpp"

using ::testing::_;
using ::testing::Return;
using ::ublkpp::Gi;
using ::ublkpp::io_result;
using ::ublkpp::UblkDisk;

#define EXPECT_SYNC_OP(OP, device, fail, sz, off)                                                                      \
    EXPECT_CALL(*(device), sync_iov(OP, _, _, _))                                                                      \
        .Times(1)                                                                                                      \
        .WillOnce([op = (OP), f = (fail), s = (sz), o = (off)](uint8_t, iovec* iovecs, uint32_t nr_vecs,               \
                                                               off_t addr) -> io_result {                              \
            EXPECT_EQ(1U, nr_vecs);                                                                                    \
            EXPECT_EQ(s, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));                                               \
            EXPECT_EQ(o, addr);                                                                                        \
            if (f) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));                       \
            if (UBLK_IO_OP_READ == op && nullptr != iovecs->iov_base) memset(iovecs->iov_base, 000, iovecs->iov_len);  \
            return s;                                                                                                  \
        });

#define EXPECT_SB_OP(OP, device, fail) EXPECT_SYNC_OP(OP, device, fail, ublkpp::raid0::SuperBlock::SIZE, 0UL);

#define CREATE_DISK_F(params, no_read, fail_read, no_write, fail_write)                                                \
    [] {                                                                                                               \
        auto device = std::make_shared< ublkpp::TestDisk >((params));                                                  \
        if (!no_read) { EXPECT_SB_OP(UBLK_IO_OP_READ, device, fail_read) }                                             \
        if (!no_write && !fail_read) { EXPECT_SB_OP(UBLK_IO_OP_WRITE, device, fail_write) }                            \
        return device;                                                                                                 \
    }()

#define CREATE_DISK(params) CREATE_DISK_F((params), false, false, false, false)

