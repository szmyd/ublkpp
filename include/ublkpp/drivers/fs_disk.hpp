#pragma once

#include <filesystem>

#include <ublkpp/lib/ublk_disk.hpp>

namespace ublkpp {

class FSDisk : public UblkDisk {
    std::filesystem::path _path;
    int _fd{-1};
    int _uring_device{-1};
    bool _block_device{false};

public:
    explicit FSDisk(std::filesystem::path const& path);
    ~FSDisk() override;

    std::string id() const override { return _path.native(); }
    std::list< int > open_for_uring(int const iouring_device) override;

    void collect_async(ublksrv_queue const*, std::list< async_result >& compl_list) override;
    io_result handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) override;
    io_result handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                             uint64_t addr) override;

    io_result async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                        uint32_t nr_vecs, uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
};
} // namespace ublkpp
