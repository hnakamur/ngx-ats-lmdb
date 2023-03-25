#ifndef NAL_LOG_H
#define NAL_LOG_H

// #include <inttypes.h>

#if !defined(nal_printflike)
#if defined(__GNUC__) || defined(__clang__)
#define nal_printflike(fmt_index, arg_index)                                   \
    __attribute__((format(printf, fmt_index, arg_index)))
#else
#define nal_printflike(fmt_index, arg_index)
#endif
#endif

#if NAL_LOG_ATS

#include "tslog.h"
#define nal_log_debug(tag, ...) TSDebug((tag), __VA_ARGS__)
#define nal_log_status(...) TSStatus(__VA_ARGS__)
#define nal_log_note(...) TSNote(__VA_ARGS__)
#define nal_log_warning(...) TSWarning(__VA_ARGS__)
#define nal_log_error(...) TSError(__VA_ARGS__)

#elif NAL_LOG_NGX

#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_log.h"
void nal_log_ngx_core(ngx_uint_t level, ngx_log_t *log, const char *fmt, ...)
    nal_printflike(3, 4);
void nal_log_ngx_debug(ngx_log_t *log, const char *func, const char *file,
                       int line, const char *tag, const char *fmt, ...)
    nal_printflike(6, 7);
extern volatile ngx_cycle_t *ngx_cycle;
#define nal_log_debug(tag, ...)                                                \
    if (ngx_cycle->log->log_level & NGX_LOG_DEBUG)                             \
    nal_log_ngx_debug(ngx_cycle->log, __func__, __FILE__, __LINE__, tag,       \
                      __VA_ARGS__)
#define nal_log_status(...)                                                    \
    if (ngx_cycle->log->log_level & NGX_LOG_INFO)                              \
    nal_log_ngx_core(NGX_LOG_INFO, ngx_cycle->log, __VA_ARGS__)
#define nal_log_note(...)                                                      \
    if (ngx_cycle->log->log_level & NGX_LOG_NOTICE)                            \
    nal_log_ngx_core(NGX_LOG_NOTICE, ngx_cycle->log, __VA_ARGS__)
#define nal_log_warning(...)                                                   \
    if (ngx_cycle->log->log_level & NGX_LOG_WARN)                              \
    nal_log_ngx_core(NGX_LOG_WARN, ngx_cycle->log, __VA_ARGS__)
#define nal_log_error(...)                                                     \
    if (ngx_cycle->log->log_level & NGX_LOG_ERR)                               \
    nal_log_ngx_core(NGX_LOG_ERR, ngx_cycle->log, __VA_ARGS__)

#elif NAL_LOG_NOP

#define nal_log_debug(tag, ...)
#define nal_log_status(...)
#define nal_log_note(...)
#define nal_log_warning(...)
#define nal_log_error(...)

#else

void nal_log_stderr(const char *level, const char *fmt, ...)
    nal_printflike(2, 3);
void nal_log_stderr_debug(const char *func, const char *file, int line,
                          const char *tag, const char *fmt, ...)
    nal_printflike(5, 6);
#define nal_log_debug(tag, ...)                                                \
    nal_log_stderr_debug(__func__, __FILE__, __LINE__, tag, __VA_ARGS__)
#define nal_log_status(...) nal_log_stderr("STATUS", __VA_ARGS__)
#define nal_log_note(...) nal_log_stderr("NOTE", __VA_ARGS__)
#define nal_log_warning(...) nal_log_stderr("WARNING", __VA_ARGS__)
#define nal_log_error(...) nal_log_stderr("ERROR", __VA_ARGS__)

#endif

#endif /* NAL_LOG_H */
