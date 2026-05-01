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

public:
    HomeBlkDisk(boost::uuids::uuid const& homeblk_vol_id, uint64_t capacity,
                std::shared_ptr< homeblocks::VolumeManager > hb_vol_if, uint32_t const _max_tx);
    ~HomeBlkDisk() override;

    std::string id() const noexcept override { return "HomeBlkDisk"; }

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
};
} // namespace ublkpp
