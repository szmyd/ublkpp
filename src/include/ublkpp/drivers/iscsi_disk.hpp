#pragma once

#include <list>
#include <mutex>

#include <ublkpp/lib/ublk_disk.hpp>

struct iscsi_context;

namespace ublkpp {

struct iscsi_session;
class iSCSIDisk : public UblkDisk {
    std::unique_ptr< iscsi_session > _session;
    int _iscsi_evfd{-1};

    std::mutex pending_results_lck;
    std::list< async_result > pending_results;

    void async_complete(ublksrv_queue const* q, async_result const& result);

public:
    explicit iSCSIDisk(std::string const& url);
    ~iSCSIDisk() override;

    std::string type() const override { return "iSCSIDisk"; }
    std::list< int > open_for_uring(int const) override;

    void collect_async(ublksrv_queue const*, std::list< async_result >& compl_list) override;
    io_result handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) override;
    io_result handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                             uint64_t addr) override;

    io_result async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                        uint32_t nr_vecs, uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;

    static void __iscsi_rw_cb(iscsi_context*, int, void*, void*);
};
} // namespace ublkpp
