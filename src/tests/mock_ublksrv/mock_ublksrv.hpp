#pragma once

#include <chrono>
#include <memory>
#include <vector>

#include <liburing.h>
#include <ublksrv.h>

#include "ublkpp/lib/sub_cmd.hpp"
#include "ublkpp/lib/ublk_disk.hpp"

namespace ublkpp {

/// Drives UblkDisk::queue_tgt_io() directly with a real io_uring, bypassing
/// ublkpp_tgt coroutines and the ublk kernel module.  Suitable for CI where
/// the ublk module is unavailable.
///
/// RAID1 INTERNAL sub_cmds (bitmap page writes) are handled automatically in
/// poll(): when a CQE with is_internal(sub_cmd) arrives, queue_internal_resp()
/// is called so RAID1 can respond and optionally enqueue more SQEs.
class MockUblksrv {
public:
    struct Completion {
        int tag;
        int result;
    };

    /// @param disk       Disk to drive (FSDisk, Raid0Disk, Raid1Disk, …)
    /// @param q_depth    Number of concurrent I/O slots (must be >= fio iodepth)
    /// @param nr_queues  Number of simulated queue threads (calls open_for_uring once per queue)
    explicit MockUblksrv(std::shared_ptr< UblkDisk > disk, int q_depth = 128, int nr_queues = 1);
    ~MockUblksrv();

    MockUblksrv(MockUblksrv const&) = delete;
    MockUblksrv& operator=(MockUblksrv const&) = delete;

    /// Populate iod fields, call disk->queue_tgt_io(), then io_uring_submit.
    /// @param tag          Slot index [0, q_depth)
    /// @param op           UBLK_IO_OP_READ / _WRITE / _FLUSH / _DISCARD
    /// @param start_sector Byte offset >> SECTOR_SHIFT
    /// @param nr_sectors   Byte length >> SECTOR_SHIFT
    /// @param buf          Sector-aligned buffer (caller owns lifetime)
    /// @returns sub_cmd count queued, or error
    io_result submit_io(int tag, uint8_t op, uint64_t start_sector, uint32_t nr_sectors, void* buf);

    /// Drain io_uring CQEs until at least @p min_completions are collected or
    /// @p timeout expires.  INTERNAL sub_cmds are handled transparently.
    std::vector< Completion > poll(int min_completions, std::chrono::milliseconds timeout);

    /// Per-tag sector-aligned I/O buffer (max_io_size = DEF_BUF_SIZE bytes).
    /// Useful when caller does not supply its own buffer.
    void* io_buf(int tag);

    int q_depth() const noexcept { return _q_depth; }
    int nr_queues() const noexcept { return static_cast< int >(_queues.size()); }
    ublksrv_queue const* queue(int q_id = 0) const noexcept { return &_queues[q_id]; }
    uint64_t capacity_sectors() const noexcept;

private:
    struct TagState {
        ublksrv_io_desc iod{};
        ublk_io_data data{};
        int sub_cmds_remaining{0};
        int result{0};
    };

    void process_cqe(io_uring_cqe* cqe, std::vector< Completion >& out);

    int _q_depth;
    ublksrv_dev _dev{};
    std::vector< ublksrv_queue > _queues;
    io_uring _ring{};
    std::shared_ptr< UblkDisk > _disk;
    std::vector< TagState > _tags;

    // Aligned I/O buffers — one per tag, DEF_BUF_SIZE bytes each
    std::vector< uint8_t > _io_buf_storage;
    std::vector< void* > _io_buf_ptrs;
};

} // namespace ublkpp
