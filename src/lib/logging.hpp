#pragma once

#include <sisl/logging/logging.h>

SISL_LOGGING_DECL(ublk_tgt)
#define TLOGT(...) LOGTRACEMOD(ublk_tgt, ##__VA_ARGS__)
#define TLOGD(...) LOGDEBUGMOD(ublk_tgt, ##__VA_ARGS__)
#define TLOGI(...) LOGINFOMOD(ublk_tgt, ##__VA_ARGS__)
#define TLOGW(...) LOGWARNMOD(ublk_tgt, ##__VA_ARGS__)
#define TLOGE(...) LOGERRORMOD(ublk_tgt, ##__VA_ARGS__)

SISL_LOGGING_DECL(ublk_raid)
#define RLOGT(...) LOGTRACEMOD(ublk_raid, ##__VA_ARGS__)
#define RLOGD(...) LOGDEBUGMOD(ublk_raid, ##__VA_ARGS__)
#define RLOGI(...) LOGINFOMOD(ublk_raid, ##__VA_ARGS__)
#define RLOGW(...) LOGWARNMOD(ublk_raid, ##__VA_ARGS__)
#define RLOGE(...) LOGERRORMOD(ublk_raid, ##__VA_ARGS__)

/// Driver logging (e.g. FSDisk)
SISL_LOGGING_DECL(ublk_drivers)
#define DLOGT(...) LOGTRACEMOD(ublk_drivers, ##__VA_ARGS__)
#define DLOGD(...) LOGDEBUGMOD(ublk_drivers, ##__VA_ARGS__)
#define DLOGI(...) LOGINFOMOD(ublk_drivers, ##__VA_ARGS__)
#define DLOGW(...) LOGWARNMOD(ublk_drivers, ##__VA_ARGS__)
#define DLOGE(...) LOGERRORMOD(ublk_drivers, ##__VA_ARGS__)

#define UBLK_LOG_MODS ublk_tgt, ublk_raid, ublk_drivers
