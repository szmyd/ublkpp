#pragma once

extern "C" {
#include <endian.h>
}

#include <sisl/logging/logging.h>
#include "ublkpp/raid/raid1.hpp"

namespace ublkpp {

namespace raid1 {
constexpr auto const k_bits_in_byte = 8UL;
//  Cap some array parameters so we can make simple assumptions later
constexpr auto k_max_dev_size = 32 * Ti;
constexpr auto k_min_chunk_size = 32 * Ki;
constexpr auto k_max_bitmap_chunks = k_max_dev_size / k_min_chunk_size;
// Use a single bit to represent each chunk
constexpr auto k_max_bitmap_size = k_max_bitmap_chunks / k_bits_in_byte;
constexpr auto k_page_size = 4 * Ki;

#ifdef __LITTLE_ENDIAN
struct __attribute__((__packed__)) SuperBlock {
    struct {
        uint8_t magic[16]; // This is a static set of 128bits to confirm existing superblock
        uint16_t version;
        uint8_t uuid[16]; // This is a user UUID that is assigned when the array is created
    } header;
    struct {
        // was cleanly unmounted, position in RAID1 and current Healthy device
        uint8_t clean_unmount : 1, read_route : 2, device_b : 1, : 0;
        struct {
            uint8_t uuid[16];    // This is a BITMAP UUID that is assigned when the array is created
            uint32_t chunk_size; // Number of bytes each bit represents
            uint64_t age;
        } bitmap;
    } fields;
    uint8_t _reserved[k_page_size - (sizeof(header) + sizeof(fields))];
};
static_assert(k_page_size == sizeof(SuperBlock), "Size of raid1::SuperBlock does not match SIZE!");
#else
#error "Big Endian not supported!"
#endif

constexpr uint64_t reserved_size = sizeof(SuperBlock) + k_max_bitmap_size;

extern SuperBlock* pick_superblock(SuperBlock* dev_a, raid1::SuperBlock* dev_b);

class Bitmap;
struct MirrorDevice;

ENUM(read_route, uint8_t, EITHER = 0, DEVA = 1, DEVB = 2);

class Raid1DiskImpl : public UblkDisk {
    boost::uuids::uuid const _uuid;
    std::string const _str_uuid;

    std::shared_ptr< MirrorDevice > _device_a;
    std::shared_ptr< MirrorDevice > _device_b;

    // Persistent state
    std::atomic_flag _is_degraded;
    std::shared_ptr< raid1::SuperBlock > _sb;
    std::unique_ptr< raid1::Bitmap > _dirty_bitmap;

    // For implementing round-robin reads
    raid1::read_route _last_read{raid1::read_route::DEVB};

    // Active Re-Sync Task
    std::thread _resync_task;
    std::atomic< uint8_t > _resync_state;

    // Asynchronous replies that did not go through io_uring
    std::map< ublksrv_queue const*, std::list< async_result > > _pending_results;

    // Internal routines
    io_result __become_clean();
    io_result __become_degraded(sub_cmd_t sub_cmd);
    io_result __clean_pages(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                            ublk_io_data const* data);
    io_result __dirty_pages(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                            ublk_io_data const* data);
    io_result __failover_read(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len);
    io_result __handle_async_retry(sub_cmd_t sub_cmd, uint64_t addr, uint32_t len, ublksrv_queue const* q,
                                   ublk_io_data const* async_data);
    io_result __replicate(sub_cmd_t sub_cmd, auto&& func, uint64_t addr, uint32_t len, ublksrv_queue const* q = nullptr,
                          ublk_io_data const* async_data = nullptr);
    void __resync_task();

    std::unique_ptr< Raid1DiskImpl > _impl;

public:
    Raid1DiskImpl(boost::uuids::uuid const& uuid, std::shared_ptr< UblkDisk > dev_a, std::shared_ptr< UblkDisk > dev_b);
    ~Raid1DiskImpl() override;

    /// Raid1Disk API
    /// =============
    std::shared_ptr< UblkDisk > swap_device(std::string const& old_device_id, std::shared_ptr< UblkDisk > new_device);
    /// =============

    /// UBlkDisk Interface Overrides
    /// ============================
    std::string id() const override { return "RAID1"; }
    std::list< int > open_for_uring(int const iouring_device) override;

    uint8_t route_size() const override { return 1; }

    void idle_transition(ublksrv_queue const*, bool) override;

    io_result handle_internal(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovec,
                              uint32_t nr_vecs, uint64_t addr, int res) override;
    void collect_async(ublksrv_queue const*, std::list< async_result >& compl_list) override;
    // RAID-1 Devices can not sit on-top of non-O_DIRECT devices, so there's nothing to flush
    io_result handle_flush(ublksrv_queue const*, ublk_io_data const*, sub_cmd_t) override { return 0; }
    io_result handle_discard(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, uint32_t len,
                             uint64_t addr) override;

    io_result async_iov(ublksrv_queue const* q, ublk_io_data const* data, sub_cmd_t sub_cmd, iovec* iovecs,
                        uint32_t nr_vecs, uint64_t addr) override;
    io_result sync_iov(uint8_t op, iovec* iovecs, uint32_t nr_vecs, off_t offset) noexcept override;
    /// ============================
};

} // namespace raid1

} // namespace ublkpp
