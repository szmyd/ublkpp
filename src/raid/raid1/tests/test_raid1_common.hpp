#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/uuid/string_generator.hpp>
#include <ublksrv.h>

#include "ublkpp/raid/raid1.hpp"
#include "raid/raid1/raid1_impl.hpp"
#include "tests/test_disk.hpp"

using ::testing::_;
using ::testing::Return;
using ::ublkpp::Gi;
using ::ublkpp::io_result;
using ::ublkpp::Ki;
using ::ublkpp::Mi;

// NOTE: All RAID1 tests have to account for the RESERVED area of the RAID1 device itself,
// this is a section at the HEAD of each backing device that holds a SuperBlock and BitMap
// used for Recovery purposes. The size of this area is CONSTANT in the code (though also
// written to the superblock itself for migration purposes.) so we can make assumptions in
// the tests based on this constant.
using ::ublkpp::raid1::reserved_size;

// This RAID1 header is copied to simulate loading a previous clean device
static const ublkpp::raid1::SuperBlock normal_superblock = {
    .header = {.magic = {0x53, 0x25, 0xff, 0x0a, 0x34, 0x99, 0x3e, 0xc5, 0x67, 0x3a, 0xc8, 0x17, 0x49, 0xae, 0x1b,
                         0x64},
               .version = htobe16(1),
               .uuid = {0xad, 0xa4, 0x07, 0x37, 0x30, 0xe3, 0x49, 0xfe, 0x99, 0x42, 0x5a, 0x28, 0x7d, 0x71, 0xeb,
                        0x3f}},
    .fields = {.clean_unmount = 1,
               .read_route = static_cast< uint8_t >(ublkpp::raid1::read_route::EITHER),
               .device_b = 0,
               .bitmap = {.uuid = {0xa7, 0x59, 0x97, 0x84, 0xd0, 0xae, 0x45, 0x5c, 0x91, 0x3c, 0x86, 0x50, 0x21, 0x01,
                                   0xd9, 0xd3},
                          .chunk_size = htobe32(32 * Ki),
                          .age = 0}},
    ._reserved = {0x00}};

static std::string const test_uuid("ada40737-30e3-49fe-9942-5a287d71eb3f");

#define EXPECT_SYNC_OP_REPEAT(OP, CNT, device, dev_b, fail, sz, off)                                                   \
    EXPECT_CALL(*(device), sync_iov(OP, _, _, _))                                                                      \
        .Times((CNT))                                                                                                  \
        .WillRepeatedly([op = (OP), side_b = (dev_b), f = (fail), s = (sz),                                            \
                         o = (off)](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {               \
            EXPECT_EQ(1U, nr_vecs);                                                                                    \
            EXPECT_EQ(s, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));                                               \
            EXPECT_EQ(o, addr);                                                                                        \
            if (f) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));                       \
            if (UBLK_IO_OP_READ == op && nullptr != iovecs->iov_base) {                                                \
                memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);                              \
                if (side_b) static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b = 1;          \
            }                                                                                                          \
            return s;                                                                                                  \
        });

#define EXPECT_SYNC_OP(OP, device, dev_b, fail, sz, off)                                                               \
    EXPECT_CALL(*(device), sync_iov(OP, _, _, _))                                                                      \
        .Times(1)                                                                                                      \
        .WillOnce([op = (OP), side_b = (dev_b), f = (fail), s = (sz),                                                  \
                   o = (off)](uint8_t, iovec* iovecs, uint32_t nr_vecs, off_t addr) -> io_result {                     \
            EXPECT_EQ(1U, nr_vecs);                                                                                    \
            EXPECT_EQ(s, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));                                               \
            EXPECT_EQ(o, addr);                                                                                        \
            if (f) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));                       \
            if (UBLK_IO_OP_READ == op && nullptr != iovecs->iov_base) {                                                \
                memcpy(iovecs->iov_base, &normal_superblock, ublkpp::raid1::k_page_size);                              \
                if (side_b) static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b = 1;          \
            } else if (UBLK_IO_OP_WRITE == op && side_b && nullptr != iovecs->iov_base && 0U == o) {                   \
                EXPECT_EQ(1, static_cast< ublkpp::raid1::SuperBlock* >(iovecs->iov_base)->fields.device_b);            \
            }                                                                                                          \
            return s;                                                                                                  \
        });

#define EXPECT_SB_OP(OP, device, dev_b, fail) EXPECT_SYNC_OP(OP, device, dev_b, fail, ublkpp::raid1::k_page_size, 0UL);
#define EXPECT_TO_WRITE_SB_F(device, fail) EXPECT_SB_OP(UBLK_IO_OP_WRITE, device, false, fail)
#define EXPECT_TO_WRITE_SB(device) EXPECT_TO_WRITE_SB_F(device, false)

#define EXPECT_ASYNC_OP(OP, device, fail, sz)                                                                          \
    EXPECT_CALL(*(device), async_iov(_, _, _, _, _, _))                                                                \
        .Times(1)                                                                                                      \
        .WillOnce([op = (OP), f = (fail), s = (sz)](ublksrv_queue const*, ublk_io_data const* data,                    \
                                                    ublkpp::sub_cmd_t sub_cmd, iovec* iovecs, uint32_t nr_vecs,        \
                                                    uint64_t addr) -> io_result {                                      \
            EXPECT_TRUE(ublkpp::is_retry(sub_cmd));                                                                    \
            EXPECT_TRUE(ublkpp::is_dependent(sub_cmd));                                                                \
            EXPECT_EQ(1U, nr_vecs);                                                                                    \
            EXPECT_EQ(s, ublkpp::__iovec_len(iovecs, iovecs + nr_vecs));                                               \
            EXPECT_GE(addr, ublkpp::raid1::k_page_size); /* Expect write to bitmap!*/                                  \
            EXPECT_LT(addr, reserved_size);              /* Expect write to bitmap!*/                                  \
            if (f) return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));                       \
            auto const op = ublksrv_get_op(data->iod);                                                                 \
            if (UBLK_IO_OP_READ == op && nullptr != iovecs->iov_base) memset(iovecs->iov_base, 000, iovecs->iov_len);  \
            return 1;                                                                                                  \
        });

#define EXPECT_SB_ASYNC_OP(OP, device, fail) EXPECT_ASYNC_OP(OP, device, fail, ublkpp::raid1::k_page_size);
#define EXPECT_TO_WRITE_SB_ASYNC_F(device, fail) EXPECT_SB_ASYNC_OP(UBLK_IO_OP_WRITE, device, fail)
#define EXPECT_TO_WRITE_SB_ASYNC(device) EXPECT_TO_WRITE_SB_ASYNC_F(device, false)

#define EXPECT_TO_READ_SB_F(device, dev_b, fail) EXPECT_SB_OP(UBLK_IO_OP_READ, device, dev_b, fail)

#define CREATE_DISK_F(params, dev_b, no_read, fail_read, no_write, fail_write)                                         \
    [] {                                                                                                               \
        auto device = std::make_shared< ublkpp::TestDisk >((params));                                                  \
        /* Expect to load and write clean_unmount bit */                                                               \
        if (!no_read) { EXPECT_TO_READ_SB_F(device, (dev_b), fail_read) }                                              \
        if (!no_write && !fail_read) { EXPECT_TO_WRITE_SB_F(device, fail_write) }                                      \
        return device;                                                                                                 \
    }()

#define CREATE_DISK(params, dev_b) CREATE_DISK_F((params), (dev_b), false, false, false, false)
#define CREATE_DISK_A(params) CREATE_DISK((params), false)
#define CREATE_DISK_B(params) CREATE_DISK((params), true)
