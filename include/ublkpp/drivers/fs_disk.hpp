#pragma once

#include <filesystem>
#include <memory>

#include <ublkpp/lib/ublk_disk.hpp>

namespace ublkpp {

struct CqeState;
class UblkFSDiskMetrics;

class FSDisk : public UblkDisk {
    std::filesystem::path _path;
    int _fd{-1};
    bool _block_device{false};
    std::unique_ptr< UblkFSDiskMetrics > _metrics;

public:
    // Constructor: creates metrics internally using parent_id
    explicit FSDisk(std::filesystem::path const& path, std::string const& parent_id = "");
    ~FSDisk() override;

    std::string id() const noexcept override { return _path.native(); }

    disk_task< int > handle_io_async(ublksrv_queue const* q, ublk_io_data const* data) override;
    disk_task< int > handle_iov_async(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                                      uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;

private:
    std::pair< io_result, CqeState* > handle_discard(ublksrv_queue const* q, ublk_io_data const* data, uint32_t len,
                                                     uint64_t addr);
};
} // namespace ublkpp
