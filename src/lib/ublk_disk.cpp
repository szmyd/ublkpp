#include "ublkpp/lib/ublk_disk.hpp"

#include <ublksrv.h>
#include <ublk_cmd.h>

#include "logging.hpp"
namespace ublkpp {

UblkDisk::UblkDisk() :
        _params(std::make_unique< ublk_params >(ublk_params{
            .len = 0,
            .types = UBLK_PARAM_TYPE_BASIC | UBLK_PARAM_TYPE_DMA_ALIGN,
            .basic =
                {
                    .attrs = UBLK_ATTR_VOLATILE_CACHE | UBLK_ATTR_FUA,
                    .logical_bs_shift = DEFAULT_BS_SHIFT, // 4KiB by default (derived can override)
                    .physical_bs_shift = DEFAULT_BS_SHIFT,
                    .io_opt_shift = DEFAULT_BS_SHIFT,
                    .io_min_shift = DEFAULT_BS_SHIFT,
                    .max_sectors = DEF_BUF_SIZE >> SECTOR_SHIFT,
                    .chunk_sectors = 0,
                    .dev_sectors = UINT64_MAX,
                    .virt_boundary_mask = 0,
                },

            .discard =
                {
                    .discard_alignment = 0,
                    .discard_granularity = 0,
                    .max_discard_sectors = UINT_MAX >> SECTOR_SHIFT,
                    .max_write_zeroes_sectors = 0,
                    .max_discard_segments = 1,
                    .reserved0 = 0,
                },
            .devt = {0, 0, 0, 0},
            .zoned = {0, 0, 0, {0}},
            .dma =
                {
                    .alignment = 511,
                    .pad = {0},
                },
        })) {}

UblkDisk::~UblkDisk() = default;

std::string UblkDisk::to_string() const {
    auto const cap_denom = capacity() >= Ti ? Gi : Mi;
    return fmt::format("[{}, size={}{}, lbs={:#0x}]", id(), capacity() / cap_denom, cap_denom == Gi ? "Gi" : "Mi",
                       block_size());
}
uint32_t UblkDisk::block_size() const noexcept { return 1 << _params->basic.logical_bs_shift; }
uint32_t UblkDisk::max_tx() const noexcept { return _params->basic.max_sectors << SECTOR_SHIFT; }
bool UblkDisk::can_discard() const noexcept { return _params->types & UBLK_PARAM_TYPE_DISCARD; }
uint64_t UblkDisk::capacity() const noexcept { return _params->basic.dev_sectors << SECTOR_SHIFT; }

DefunctDisk::DefunctDisk() : UblkDisk() {
    direct_io = true;
    auto& our_params = *params();
    our_params.types |= UBLK_PARAM_TYPE_DISCARD;
    our_params.basic.logical_bs_shift = 9;
    our_params.basic.physical_bs_shift = 9;
}

std::string DefunctDisk::id() const noexcept { return "~DEFUNCT~"; }

disk_task< int > DefunctDisk::async_iov(ublksrv_queue const*, ublk_io_data const*, iovec*, uint32_t, uint64_t) {
    co_return -EIO; // LCOV_EXCL_LINE
}

io_result DefunctDisk::sync_iov(uint8_t, iovec*, uint32_t, off_t) noexcept {
    return std::unexpected(std::make_error_condition(std::errc::io_error));
}

} // namespace ublkpp
