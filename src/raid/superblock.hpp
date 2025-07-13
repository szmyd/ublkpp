#pragma once

#include <cstdlib>

#include <ublk_cmd.h>

#include "ublkpp/lib/ublk_disk.hpp"
#include "lib/logging.hpp"

namespace ublkpp {

template < class SB >
inline SB* read_superblock(UblkDisk& device) noexcept {
    RLOGT("Reading Superblock from: [{}] {}%{} == {}", device, SB::SIZE, device.block_size(),
          SB::SIZE % device.block_size())
    DEBUG_ASSERT_EQ(0, SB::SIZE % device.block_size(), "Device [{}] blocksize does not support alignment of [{}B]",
                    device, SB::SIZE)
    auto iov = iovec{.iov_base = nullptr, .iov_len = SB::SIZE};
    if (auto err = ::posix_memalign(&iov.iov_base, device.block_size(), SB::SIZE); 0 != err || nullptr == iov.iov_base)
        [[unlikely]] { // LCOV_EXCL_START
        if (EINVAL == err) RLOGE("Invalid Argument while reading superblock!")
        RLOGE("Out of Memory while reading superblock!")
        return nullptr;
    } // LCOV_EXCL_STOP
    if (auto res = device.sync_iov(UBLK_IO_OP_READ, &iov, 1, 0UL); !res) {
        RLOGE("Could not read SuperBlock of [sz:{}] [res:{}]", SB::SIZE, res.error().message())
        free(iov.iov_base);
        return nullptr;
    }
    return static_cast< SB* >(iov.iov_base);
}

template < typename SB >
inline io_result write_superblock(UblkDisk& device, SB* sb) noexcept {
    RLOGT("Writing Superblock to: [{}]", device)
    DEBUG_ASSERT_EQ(0, SB::SIZE % device.block_size(), "Device [{}] blocksize does not support alignment of [{}B]",
                    device, SB::SIZE)
    auto iov = iovec{.iov_base = sb, .iov_len = SB::SIZE};
    auto res = device.sync_iov(UBLK_IO_OP_WRITE, &iov, 1, 0UL);
    if (!res) RLOGE("Error writing Superblock to: [{}]!", device, res.error().message())
    return res;
}

template < typename SB > // LCOV_EXCL_START
inline io_result write_superblock_async(UblkDisk& device, SB* sb, ublksrv_queue const* q, ublk_io_data const* data,
                                        sub_cmd_t sub_cmd) noexcept {
    RLOGT("Writing Superblock to: [{}]", device)
    DEBUG_ASSERT_EQ(0, SB::SIZE % device.block_size(), "Device [{}] blocksize does not support alignment of [{}B]",
                    device, SB::SIZE)
    auto iov = iovec{.iov_base = sb, .iov_len = SB::SIZE};
    auto res = device.async_iov(q, data, sub_cmd, &iov, 1, 0UL);
    // MUST Submit here since iov is on the stack!
    if (q) io_uring_submit(q->ring_ptr);
    return res;
}
// LCOV_EXCL_STOP

} // namespace ublkpp
