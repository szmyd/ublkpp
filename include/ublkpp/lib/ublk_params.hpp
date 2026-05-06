#pragma once

// Re-exports the kernel UAPI ublk_params types: ublk_params, ublk_param_basic,
// ublk_param_discard, ublk_param_devt, ublk_param_zoned, ublk_param_dma_align,
// and their associated UBLK_PARAM_TYPE_* / UBLK_ATTR_* constants.
//
// Include this in external ublk_disk subclasses that need to call params() in
// their constructors to configure disk geometry.
#include <ublk_cmd.h>
