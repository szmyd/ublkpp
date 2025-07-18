#include "ublkpp/drivers/fs_disk.hpp"

extern "C" {
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
}

#include <fstream>

#include <sisl/logging/logging.h>
#include <ublksrv_utils.h>
#include <ublksrv.h>

#include "fs_disk_impl.hpp"
#include "lib/logging.hpp"

namespace ublkpp {

FSDisk::FSDisk(std::filesystem::path const& path) : UblkDisk(), _path(path) {
    auto const str_path = _path.native();
    _fd = open(str_path.c_str(), O_RDWR);
    if (_fd < 0) {
        DLOGE("backing file {} can't be opened: {}", str_path, strerror(errno))
        throw std::runtime_error("Open Failed!");
    }

    struct stat st{};
    if (fstat(_fd, &st) < 0) {
        DLOGE("fstat({}) failed: ", str_path, strerror(errno))
        throw std::runtime_error("fstat Failed!");
    }

    uint64_t bytes{0};
    _block_device = S_ISBLK(st.st_mode);
    auto& our_params = *params();
    if (_block_device) {
        uint32_t lbs;
        uint32_t pbs;
        if (ioctl(_fd, BLKGETSIZE64, &bytes) != 0 || ioctl(_fd, BLKSSZGET, &lbs) != 0 ||
            ioctl(_fd, BLKPBSZGET, &pbs) != 0)
            throw std::runtime_error("ioctl Failed!");
        if (block_has_unmap(_path))
            our_params.types |= UBLK_PARAM_TYPE_DISCARD;
        else
            DLOGW("Block Device does not support DISCARD! Will fall back to fallocate() calls.")
        our_params.basic.logical_bs_shift = static_cast< uint8_t >(ilog2(lbs));
        our_params.basic.physical_bs_shift = static_cast< uint8_t >(ilog2(pbs));
        DLOGD("Backing is a block device [{}:{}:{}]!", str_path, lbs, pbs)

    } else if (S_ISREG(st.st_mode)) {
        bytes = st.st_size;
        our_params.types |= UBLK_PARAM_TYPE_DISCARD;
        auto const lbs = static_cast< uint32_t >(st.st_blksize);
        our_params.basic.logical_bs_shift = static_cast< uint8_t >(ilog2(lbs));
        our_params.basic.physical_bs_shift = our_params.basic.logical_bs_shift;
        DLOGD("Backing is a not block device [{}:{}:{}]!", str_path, lbs, lbs)
    } else {
        DLOGE("fstat({}) returned non-block/non-regular file!", str_path, strerror(errno))
        throw std::runtime_error("Bad file!");
    }
    if (st.st_blksize && can_discard()) our_params.discard.discard_granularity = static_cast< uint32_t >(st.st_blksize);

    // in case of buffered io, use common bs/pbs so that all FS
    // image can be supported
    if (!fcntl(_fd, F_SETFL, O_DIRECT))
        direct_io = true;
    else
        DLOGD("Unable to support DIRECT I/O, using BUFFERED.")
    our_params.basic.dev_sectors = bytes >> SECTOR_SHIFT;
    if (UINT32_MAX == our_params.discard.discard_granularity) {
        our_params.discard.discard_granularity = 0;
        our_params.types &= ~UBLK_PARAM_TYPE_DISCARD;
    }
}

FSDisk::~FSDisk() {
    if (0 >= _fd) {
        if (!direct_io) fdatasync(_fd);
        close(_fd);
    }
}

std::list< int > FSDisk::open_for_uring(int const iouring_device_start) {
    RELEASE_ASSERT_GT(_fd, -1, "FileDescriptor invalid {}", _fd)
    _uring_device = iouring_device_start;
    // We duplicate the FD here so ublksrv doesn't close it before we're ready
    return {dup(_fd)};
}

static inline auto next_sqe(ublksrv_queue const* q) {
    auto r = q->ring_ptr;
    if (0 == io_uring_sq_space_left(r)) [[unlikely]]
        io_uring_submit(r);
    auto sqe = io_uring_get_sqe(r);
    if (sqe) [[likely]]
        io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
    return sqe;
}

void FSDisk::collect_async(ublksrv_queue const*, std::list< async_result >&) {}

io_result FSDisk::handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) {

    DLOGT("Flush {} : [tag:{:x}] ublk io [sub_cmd:{}]", _path.native(), data->tag, ublkpp::to_string(sub_cmd))
    if (direct_io) return 0;
    auto sqe = next_sqe(q);
    io_uring_prep_fsync(sqe, _uring_device, IORING_FSYNC_DATASYNC);

    sqe->user_data = build_tgt_sqe_data(data->tag, ublksrv_get_op(data->iod), sub_cmd);
    return 1;
}

io_result FSDisk::handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                                 uint64_t addr) {
    auto const lba = addr >> params()->basic.logical_bs_shift;
    DLOGD("DISCARD {}: [tag:{:x}] ublk io [lba:{:x}|len:{}|sub_cmd:{}]", _path.native(), data->tag, lba, len,
          ublkpp::to_string(sub_cmd))
    if (!_block_device) {
        auto sqe = next_sqe(q);
        io_uring_prep_fallocate(sqe, _uring_device, discard_to_fallocate(data->iod), addr, len);

        sqe->user_data = build_tgt_sqe_data(data->tag, ublksrv_get_op(data->iod), sub_cmd);
        return 1;
    }

    // Submit all queued I/O
    io_uring_submit(q->ring_ptr);

    uint64_t r[2]{addr, len};
    auto res = ioctl(q->dev->tgt.fds[_uring_device], BLKDISCARD, &r);
    if (0 == res) [[likely]]
        return 0;
    DEBUG_ASSERT_LT(res, 0, "Positive ioctl")
    if (0 < res) {
        DLOGE("ioctl BLKDISCARD on {} returned postive result: {}", _path.native(), res)
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    }
    DLOGE("ioctl BLKDISCARD on {} returned error: {}", _path.native(), strerror(errno))
    return folly::makeUnexpected(std::make_error_condition(static_cast< std::errc >(errno)));
}

io_result FSDisk::async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                            uint32_t nr_vecs, uint64_t addr) {
    auto const op = ublksrv_get_op(data->iod);
    auto const lba = addr >> params()->basic.logical_bs_shift;
    DLOGT("{} {} : [tag:{:x}] ublk io [lba:{:x}|len:{}|sub_cmd:{}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE",
          _path.native(), data->tag, lba, __iovec_len(iovecs, iovecs + nr_vecs), ublkpp::to_string(sub_cmd))
    auto sqe = next_sqe(q);

    if (UBLK_IO_OP_READ == op) {
        if (1 == nr_vecs)
            io_uring_prep_rw(IORING_OP_READ, sqe, _uring_device, iovecs->iov_base, iovecs->iov_len, addr);
        else
            io_uring_prep_readv(sqe, _uring_device, iovecs, nr_vecs, addr);
    } else {
        if (1 == nr_vecs)
            io_uring_prep_rw(IORING_OP_WRITE, sqe, _uring_device, iovecs->iov_base, iovecs->iov_len, addr);
        else
            io_uring_prep_writev(sqe, _uring_device, iovecs, nr_vecs, addr);
    }

    // Set ForceUnitAccess bit to bypass caches
    if (UBLK_IO_OP_READ != op && (data->iod->op_flags & UBLK_IO_F_FUA)) sqe->rw_flags |= RWF_DSYNC;

    sqe->user_data = build_tgt_sqe_data(data->tag, op, sub_cmd);
    return 1;
}

io_result FSDisk::sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t addr) noexcept {
    if (0 > _fd) {
        DLOGE("Direct read on un-opened device!")
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    }
    auto const lba = addr >> params()->basic.logical_bs_shift;
    DLOGT("{} {} : [INTERNAL] ublk io [lba:{:x}|len:{}]", op == UBLK_IO_OP_READ ? "READ" : "WRITE", _path.native(), lba,
          __iovec_len(iovecs, iovecs + nr_vecs))
    auto res = ssize_t{-1};
    switch (op) {
    case UBLK_IO_OP_READ: {
        res = preadv2(_fd, iovecs, nr_vecs, addr, RWF_HIPRI);
    } break;
    case UBLK_IO_OP_WRITE: {
        res = pwritev2(_fd, iovecs, nr_vecs, addr, RWF_DSYNC | RWF_HIPRI);
    } break;
    default:
        return folly::makeUnexpected(std::make_error_condition(std::errc::invalid_argument));
    }
    if (0 > res) {
        DLOGE("{} {} : {}", op == UBLK_IO_OP_READ ? "preadv" : "pwritev", _path.native(), strerror(errno))
        return folly::makeUnexpected(std::make_error_condition(std::errc::io_error));
    }
    return res;
}

} // namespace ublkpp
