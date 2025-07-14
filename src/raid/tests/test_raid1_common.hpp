#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/uuid/random_generator.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>
#include <ublksrv.h>

#include "ublkpp/raid/raid1.hpp"
#include "raid/raid1_impl.hpp"
#include "raid/superblock.hpp"
#include "tests/test_disk.hpp"

#define ENABLED_OPTIONS logging, raid1

SISL_LOGGING_INIT(ublk_raid)
SISL_OPTIONS_ENABLE(ENABLED_OPTIONS)

using ::testing::_;
using ::testing::Return;
using ::ublkpp::Gi;
using ::ublkpp::io_result;

// NOTE: All RAID1 tests have to account for the RESERVED area of the RAID1 device itself,
// this is a section at the HEAD of each backing device that holds a SuperBlock and BitMap
// used for Recovery purposes. The size of this area is CONSTANT in the code (though also
// written to the superblock itself for migration purposes.) so we can make assumptions in
// the tests based on this constant.
using ::ublkpp::raid1::reserved_size;

#define EXPECT_SYNC_OP_REPEAT(OP, CNT, device, fail, sz, off)                                                          \
    EXPECT_CALL(*(device), sync_iov(OP, _, _, _))                                                                      \
        .Times((CNT))                                                                                                  \
        .WillRepeatedly([op = (OP), f = (fail), s = (sz), o = (off)](uint8_t, iovec* iovecs, uint32_t nr_vecs,         \
                                                                     off_t addr) -> io_result {                        \
            EXPECT_EQ(1U, nr_vecs);                                                                                    \
            EXPECT_EQ(s, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));                                               \
            EXPECT_EQ(o, addr);                                                                                        \
            if (f) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));                       \
            if (UBLK_IO_OP_READ == op && nullptr != iovecs->iov_base) memset(iovecs->iov_base, 000, iovecs->iov_len);  \
            return s;                                                                                                  \
        });

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

#define EXPECT_SB_OP(OP, device, fail) EXPECT_SYNC_OP(OP, device, fail, ublkpp::raid1::SuperBlock::SIZE, 0UL);
#define EXPECT_TO_WRITE_SB_F(device, fail) EXPECT_SB_OP(UBLK_IO_OP_WRITE, device, fail)
#define EXPECT_TO_WRITE_SB(device) EXPECT_TO_WRITE_SB_F(device, false)

#define EXPECT_ASYNC_OP(OP, device, fail, sz)                                                                          \
    EXPECT_CALL(*(device), async_iov(_, _, _, _, _, _))                                                                \
        .Times(1)                                                                                                      \
        .WillOnce([op = (OP), f = (fail), s = (sz)](ublksrv_queue const*, ublk_io_data const* data,                    \
                                                    ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t nr_vecs,        \
                                                    uint64_t addr) -> io_result {                                      \
            EXPECT_TRUE(ublkpp::is_retry(sub_cmd));                                                                    \
            EXPECT_TRUE(test_flags(sub_cmd, ublkpp::sub_cmd_flags::INTERNAL));                                         \
            EXPECT_EQ(1U, nr_vecs);                                                                                    \
            EXPECT_EQ(s, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));                                               \
            EXPECT_GE(addr, ublkpp::raid1::SuperBlock::SIZE); /* Expect write to bitmap!*/                             \
            EXPECT_LT(addr, reserved_size);                   /* Expect write to bitmap!*/                             \
            if (f) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));                       \
            auto const op = ublksrv_get_op(data->iod);                                                                 \
            if (UBLK_IO_OP_READ == op && nullptr != iovecs->iov_base) memset(iovecs->iov_base, 000, iovecs->iov_len);  \
            return 1;                                                                                                  \
        });

#define EXPECT_SB_ASYNC_OP(OP, device, fail) EXPECT_ASYNC_OP(OP, device, fail, ublkpp::raid1::SuperBlock::SIZE);
#define EXPECT_TO_WRITE_SB_ASYNC_F(device, fail) EXPECT_SB_ASYNC_OP(UBLK_IO_OP_WRITE, device, fail)
#define EXPECT_TO_WRITE_SB_ASYNC(device) EXPECT_TO_WRITE_SB_ASYNC_F(device, false)

#define EXPECT_TO_READ_SB_F(device, fail) EXPECT_SB_OP(UBLK_IO_OP_READ, device, fail)

#define CREATE_DISK_F(params, no_read, fail_read, no_write, fail_write)                                                \
    [] {                                                                                                               \
        auto device = std::make_shared< ublkpp::TestDisk >((params));                                                  \
        /* Expect to load and write clean_unmount bit */                                                               \
        if (!no_read) { EXPECT_TO_READ_SB_F(device, fail_read) }                                                       \
        if (!no_write && !fail_read) { EXPECT_TO_WRITE_SB_F(device, fail_write) }                                      \
        return device;                                                                                                 \
    }()

#define CREATE_DISK(params) CREATE_DISK_F((params), false, false, false, false)
