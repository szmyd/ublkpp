#pragma once

extern "C" {
#include <iscsi/iscsi.h>
}

#include <deque>
#include <mutex>

#include <ublkpp/lib/ublk_disk.hpp>

namespace ublkpp {

struct iscsi_session;
class iSCSIDisk : public UblkDisk {
    iscsi_url _url;
    std::unique_ptr< iscsi_session > _session;

    struct req_result {
        int tag;
        int result;
    };

    std::mutex pending_results_lck;
    std::deque< req_result > pending_results;

public:
    iSCSIDisk(iscsi_url const& url);
    ~iSCSIDisk() override;

    std::string type() const override { return "iSCSIDisk"; }
    std::list< int > open_for_uring(int const) override { return {}; }

    void handle_event(ublksrv_queue const* q) override;
    io_result handle_flush(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd) override;
    io_result handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                             uint64_t addr) override;

    io_result async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                        uint32_t nr_vecs, uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;

    // Interal
    void __rw_async_cb(ublksrv_queue const* q, int tag, int status, int res);
};
} // namespace ublkpp
