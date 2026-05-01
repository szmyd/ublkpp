#pragma once

#include <memory>

#include <boost/uuid/uuid.hpp>
#include <sisl/utility/enum.hpp>
#include <ublkpp/lib/ublk_disk.hpp>

namespace ublkpp {

class UblkRaidMetrics;

namespace raid1 {
class Raid1DiskImpl;
ENUM(replica_state, uint8_t, CLEAN = 0, SYNCING = 1, ERROR = 2, UNAVAIL = 3);
struct array_state {
    replica_state device_a;
    replica_state device_b;
    uint64_t bytes_to_sync;
};
} // namespace raid1
class Raid1Disk : public UblkDisk {
public:
    Raid1Disk(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a, std::shared_ptr< UblkDisk > dev_b,
              std::string const& parent_id = "");
    ~Raid1Disk() override;

    /// Raid1Disk API
    /// =============
    std::shared_ptr< UblkDisk > swap_device(std::string const& old_device_id, std::shared_ptr< UblkDisk > new_device);
    raid1::array_state replica_states() const noexcept;
    uint64_t reserved_size() const noexcept;
    std::pair< std::shared_ptr< UblkDisk >, std::shared_ptr< UblkDisk > > replicas() const noexcept;
    void toggle_resync(bool t);
    /// =============

    /// UBlkDisk Interface Overrides
    /// ============================
    uint32_t block_size() const noexcept override;
    bool can_discard() const noexcept override;
    uint64_t capacity() const noexcept override;
    // ================

    ublk_params* params() noexcept override;
    ublk_params const* params() const noexcept override;
    std::string id() const noexcept override;
    std::list< int > prepare(ublksrv_queue const*, int const iouring_device) override;

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override;

    void idle_transition(ublksrv_queue const*, bool) override;

    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
    /// ============================

private:
    std::unique_ptr< raid1::Raid1DiskImpl > _impl;
};
} // namespace ublkpp
