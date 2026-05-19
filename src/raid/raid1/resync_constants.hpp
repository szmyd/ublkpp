#pragma once

#include <cstdint>

namespace ublkpp::raid1 {

// Number of concurrent READ+WRITE slots in the io_uring resync pipeline.
constexpr uint32_t k_resync_slots = 8;

// Ring depth: k_resync_slots READs + k_resync_slots WRITEs + 1 CANCEL SQE.
constexpr uint32_t k_resync_ring_depth = k_resync_slots * 2 + 1;

// user_data tag that marks a WRITE SQE. Slot index is stored in the lower byte;
// OR this tag onto it so process_cqes() can distinguish READ CQEs from WRITE CQEs.
constexpr uint64_t k_resync_write_tag = 0x100ULL;

// user_data sentinel for the cancel-all SQE submitted by drain_and_exit().
// Never a valid slot index (k_resync_slots is always < 256).
constexpr uint64_t k_resync_cancel_tag = 0xFFFFFFFFULL;

} // namespace ublkpp::raid1
