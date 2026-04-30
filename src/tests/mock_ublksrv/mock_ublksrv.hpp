#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <vector>

#include <liburing.h>
#include <ublksrv.h>

#include "ublkpp/lib/cqe_state.hpp"
#include "ublkpp/lib/disk_task.hpp"
#include "ublkpp/lib/sub_cmd.hpp"
#include "ublkpp/lib/ublk_disk.hpp"

namespace ublkpp {

// Drives UblkDisk I/O directly with a real io_uring, bypassing ublkpp_tgt coroutines
// and the ublk kernel module. Suitable for CI where the ublk module is unavailable.
//
// Supports two dispatch paths:
//   Old path (uses_async_api() == false): calls queue_tgt_io, submits SQEs, polls CQEs.
//   New path (uses_async_api() == true):  starts handle_io_async disk_task, uses inject_cqe
//     to deliver synthetic results without requiring real io_uring round-trips.
//
// RAID1 INTERNAL sub_cmds (bitmap page writes) are handled automatically in poll().
class MockUblksrv {
public:
    struct Completion {
        int tag;
        int result;
    };

    // disk: disk to drive (FSDisk, Raid0Disk, Raid1Disk, ...)
    // q_depth: number of concurrent I/O slots (must be >= fio iodepth)
    // nr_queues: number of simulated queue threads (calls open_for_uring once per queue)
    explicit MockUblksrv(std::shared_ptr< UblkDisk > disk, int q_depth = 128, int nr_queues = 1);
    ~MockUblksrv();

    MockUblksrv(MockUblksrv const&) = delete;
    MockUblksrv& operator=(MockUblksrv const&) = delete;

    // Old path: populate iod fields, call disk->queue_tgt_io(), then io_uring_submit.
    // New path: start handle_io_async disk_task; task runs until first co_await CqeAwaitable.
    // tag: slot index [0, q_depth), op: UBLK_IO_OP_READ / _WRITE / _FLUSH / _DISCARD
    // start_sector: byte offset >> SECTOR_SHIFT, nr_sectors: byte length >> SECTOR_SHIFT
    // buf: sector-aligned buffer (caller owns lifetime)
    io_result submit_io(int tag, uint8_t op, uint64_t start_sector, uint32_t nr_sectors, void* buf);

    // Drain io_uring CQEs until at least min_completions are collected or timeout expires.
    // INTERNAL sub_cmds are handled transparently (old path only).
    std::vector< Completion > poll(int min_completions, std::chrono::milliseconds timeout);

    // New async path only. Deliver a synthetic result to the CqeState currently suspended
    // in the disk_task for the given tag. Resumes the task; returns a completion when the
    // task runs to completion. Call once per awaited stripe for multi-stripe IOs.
    std::vector< Completion > inject_cqe(int tag, int result);

    // Per-tag sector-aligned I/O buffer (max_io_size = DEF_BUF_SIZE bytes).
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

    // async_io pool — one per tag, mirrors what init_queue does via placement new
    std::vector< async_io > _io_states;

    // disk_task handles for the async API path — one optional slot per tag
    std::vector< std::optional< disk_task< int > > > _async_tasks;

    // Aligned I/O buffers — one per tag, DEF_BUF_SIZE bytes each
    std::vector< uint8_t > _io_buf_storage;
    std::vector< void* > _io_buf_ptrs;
};

} // namespace ublkpp
