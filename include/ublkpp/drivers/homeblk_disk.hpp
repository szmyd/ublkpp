#pragma once

#include <mutex>

#include <boost/uuid/uuid.hpp>
#include <ublkpp/lib/ublk_disk.hpp>

namespace homeblocks {
class Volume;
class VolumeManager;
} // namespace homeblocks

namespace ublkpp {

class HomeBlkDisk : public UblkDisk {
    boost::uuids::uuid const _vol_id;
    std::shared_ptr< homeblocks::VolumeManager > const _hb_vol_if;
    std::shared_ptr< homeblocks::Volume > _hb_volume;

    std::mutex pending_results_lck;
    std::list< async_result > pending_results;

public:
    HomeBlkDisk(boost::uuids::uuid const& homeblk_vol_id, uint64_t capacity,
                std::shared_ptr< homeblocks::VolumeManager > hb_vol_if, uint32_t const _max_tx);
    ~HomeBlkDisk() override;

    std::string id() const override { return "HomeBlkDisk"; }
    bool contains(std::string const& ) const override { return false; }
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
