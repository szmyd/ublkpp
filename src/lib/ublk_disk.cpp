#include "ublkpp/lib/ublk_disk.hpp"

#include <ublksrv.h>
#include <ublk_cmd.h>

#include "logging.hpp"
#include "common.hpp"
namespace ublkpp {

ublk_disk::ublk_disk() :
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

ublk_disk::~ublk_disk() = default;

uint32_t ublk_disk::block_size() const noexcept { return 1 << _params->basic.logical_bs_shift; }
uint32_t ublk_disk::physical_block_size() const noexcept { return 1 << _params->basic.physical_bs_shift; }
uint32_t ublk_disk::max_tx() const noexcept { return _params->basic.max_sectors << SECTOR_SHIFT; }
bool ublk_disk::can_discard() const noexcept { return _params->types & UBLK_PARAM_TYPE_DISCARD; }
uint32_t ublk_disk::discard_granularity() const noexcept { return _params->discard.discard_granularity; }
uint32_t ublk_disk::max_discard_sectors() const noexcept { return _params->discard.max_discard_sectors; }
uint64_t ublk_disk::capacity() const noexcept { return _params->basic.dev_sectors << SECTOR_SHIFT; }

namespace {
// Implementation shim for make_missing_disk(). Lives entirely in this TU; callers see only
// the base ublk_disk through the shared_ptr returned by the factory.
struct missing_disk_impl : ublk_disk {
    missing_disk_impl() noexcept {
        _is_missing = true;
        _direct_io = true;
        auto& our_params = *params();
        our_params.types |= UBLK_PARAM_TYPE_DISCARD;
        our_params.basic.logical_bs_shift = 9;
        our_params.basic.physical_bs_shift = 9;
    }
};
} // namespace

std::shared_ptr< ublk_disk > make_missing_disk() { return std::make_shared< missing_disk_impl >(); }

} // namespace ublkpp
