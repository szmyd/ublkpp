#pragma once

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <ublksrv.h>

#include "ublkpp/raid/raid0.hpp"
#include "raid/raid0/raid0_impl.hpp"
#include "tests/test_disk.hpp"

using ::testing::_;
using ::testing::Return;
using ::ublkpp::Gi;
using ::ublkpp::io_result;
using ::ublkpp::UblkDisk;

// This RAID0 header is copied to simulate loading a previous clean device
static const ublkpp::raid0::SuperBlock normal_superblock =
    {.header = {.magic = {0127, 0345, 072, 0211, 0254, 033, 070, 0146, 0125, 0377, 0204, 065, 0131, 0120, 0306, 047},
                .version = htobe16(1),
                .uuid = {0xad, 0xa4, 0x07, 0x37, 0x30, 0xe3, 0x49, 0xfe, 0x99, 0x42, 0x5a, 0x28, 0x7d, 0x71, 0xeb,
                         0x3f}},
     .fields =
         {
             .stripe_off = 0,
             .stripe_size = htobe32(128 * Ki),
         },
     ._reserved = {0x00}};

static std::string const test_uuid("ada40737-30e3-49fe-9942-5a287d71eb3f");

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

#define EXPECT_SB_OP(OP, device, fail) EXPECT_SYNC_OP(OP, device, fail, sizeof(ublkpp::raid0::SuperBlock), 0UL);

#define CREATE_DISK_F(params, no_read, fail_read, no_write, fail_write)                                                \
    [] {                                                                                                               \
        auto device = std::make_shared< ublkpp::TestDisk >((params));                                                  \
        if (!no_read) { EXPECT_SB_OP(UBLK_IO_OP_READ, device, fail_read) }                                             \
        if (!no_write && !fail_read) { EXPECT_SB_OP(UBLK_IO_OP_WRITE, device, fail_write) }                            \
        return device;                                                                                                 \
    }()

#define CREATE_DISK(params) CREATE_DISK_F((params), false, false, false, false)
