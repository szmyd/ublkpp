#include <cstdarg>
#include <cstdio>

#include <sisl/logging/logging.h>

SISL_LOGGING_DEF(ublksrv)
SISL_LOGGING_DEF(ublk_tgt)
SISL_LOGGING_DEF(ublk_raid)
SISL_LOGGING_DEF(ublk_drivers)

// These override the logging methods provided by libulksrv.a which
// was using syslog by default. Here we trap and forward into SISL
// logging instead
extern "C" {
static void va_log(const char* msg, va_list& ap) {
    char* log_mesg;
    if (0 >= vasprintf(&log_mesg, msg, ap)) {
        LOGCRITICAL("Could not allocate memory for logging!")
        return;
    }
    sisl::logging::GetLogger()->warn("{}", log_mesg);
    free(log_mesg);
}

extern void ublk_dbg(int, const char* msg, ...) {
    if (!LEVELCHECK(ublksrv, spdlog::level::level_enum::trace)) return;
    va_list ap;
    va_start(ap, msg);
    va_log(msg, ap);
    va_end(ap);
}

extern void ublk_ctrl_dbg(int, const char* msg, ...) {
    if (!LEVELCHECK(ublksrv, spdlog::level::level_enum::trace)) return;
    va_list ap;
    va_start(ap, msg);
    va_log(msg, ap);
    va_end(ap);
}

extern void ublk_err(const char* msg, ...) {
    if (!LEVELCHECK(ublksrv, spdlog::level::level_enum::err)) return;
    va_list ap;
    va_start(ap, msg);
    va_log(msg, ap);
    va_end(ap);
}

extern void ublk_log(const char* msg, ...) {
    if (!LEVELCHECK(ublksrv, spdlog::level::level_enum::info)) return;
    va_list ap;
    va_start(ap, msg);
    va_log(msg, ap);
    va_end(ap);
}
}
