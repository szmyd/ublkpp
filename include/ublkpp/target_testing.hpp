#pragma once
// Non-installed companion header for unit-testing ublkpp_tgt without kernel infrastructure.
// Include this file ONLY in test targets. It is not part of the installed ublkpp package
// (excluded from the conan package() step).

#include "ublkpp/target.hpp"
#include "metrics/ublk_io_metrics.hpp"

namespace ublkpp {

// Test peer: declared as friend in ublkpp_tgt, providing access to internal state for tests.
// All methods are implemented in ublkpp_tgt.cpp. Do not use outside of test binaries.
struct ublkpp_tgt_test_peer {
    // Creates an ublkpp_tgt with the given device and no ublksrv state. Queue handlers are
    // empty, so begin_shutdown() destroys the backing device synchronously. Calling run(),
    // remove(), or device_id() on the returned target is undefined behaviour.
    static ublkpp_tgt make(disk_handle dev);

    // Triggers the drain check queue threads perform after each counter decrement. Use to
    // exercise the non-idle drain path: increment a counter via metrics(), call
    // begin_shutdown(), decrement the counter, then call this. Idempotent via CAS.
    static void try_drain(ublkpp_tgt& tgt);

    // Returns the I/O metrics for direct counter manipulation in tests.
    static UblkIOMetrics& metrics(ublkpp_tgt& tgt);
};

} // namespace ublkpp
