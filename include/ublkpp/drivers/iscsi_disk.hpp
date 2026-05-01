#pragma once

#include <mutex>

#include <ublkpp/lib/ublk_disk.hpp>

struct iscsi_context;

namespace ublkpp {

struct iscsi_session;
class iSCSIDisk : public UblkDisk {
    std::unique_ptr< iscsi_session > _session;

    void async_complete(ublksrv_queue const* q, int tag, int result);

public:
    explicit iSCSIDisk(std::string const& url);
    ~iSCSIDisk() override;

    std::string id() const noexcept override;
    std::list< int > open_for_uring(ublksrv_queue const*, int const) override;

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                                      uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;

    static void __iscsi_rw_cb(iscsi_context*, int, void*, void*);
};
} // namespace ublkpp
