#pragma once

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include <ublkpp/lib/ublk_disk.hpp>

struct iscsi_context;

namespace ublkpp {

struct iscsi_session;
struct queue_service;

class iSCSIDisk : public UblkDisk {
    // Setup-time iSCSI session: used for sync_iov (called by Raid1 resync_task on its own thread).
    // Stays alive across the disk lifetime; libiscsi contexts are not thread-safe so this session
    // is independent of the per-queue async sessions.
    std::unique_ptr< iscsi_session > _sync_session;
    int _lun{0};

    // Per-queue async services. Created lazily in prepare() and indexed by ublksrv_queue::q_id.
    // Each queue_service owns its own iscsi_context, eventfd, and service-loop coroutine handle;
    // see iscsi_disk.cpp for the dispatch protocol. Multi-queue forward-compat: nr_hw_queues=1
    // today but each queue gets its own session so partitioning across queues later is a config
    // change, not a refactor.
    mutable std::shared_mutex _services_mtx;
    std::unordered_map< int, std::unique_ptr< queue_service > > _services;

    queue_service* __get_service(ublksrv_queue const* q) const;

public:
    explicit iSCSIDisk(std::string const& url);
    ~iSCSIDisk() override;

    std::string id() const noexcept override;
    std::list< int > prepare(ublksrv_queue const*, int const) override;

    disk_task< int > async_iov(ublksrv_queue const* q, ublk_io_data const* data, iovec* iovecs, uint32_t nr_vecs,
                               uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;

    static void __iscsi_rw_cb(iscsi_context*, int, void*, void*);
};
} // namespace ublkpp
