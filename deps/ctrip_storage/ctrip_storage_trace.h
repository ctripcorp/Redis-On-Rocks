#ifndef __CTRIP_STORAGE_TRACE_H__
#define __CTRIP_STORAGE_TRACE_H__
#include "server.h"
typedef struct  {
    int intention;
    monotime swap_lock_time;
    monotime swap_dispatch_time;
    monotime swap_process_time;
    monotime swap_notify_time;
    monotime swap_callback_time;
} swapTrace;

void swapTraceLock(swapTrace *trace);
typedef struct  {
    int swap_cnt;
    int finished_swap_cnt;
    monotime swap_submitted_time;
    monotime swap_finished_time;
    swapTrace *swap_traces;
} swapCmdTrace;

swapCmdTrace *createSwapCmdTrace(void);
void initSwapTraces(swapCmdTrace *swap_cmd, int swap_cnt);
void swapCmdTraceFree(swapCmdTrace *trace);
void swapCmdSwapSubmitted(swapCmdTrace *swap_cmd);
void swapTraceLock(swapTrace *trace);
void swapTraceDispatch(swapTrace *trace);
void swapTraceProcess(swapTrace *trace);
void swapTraceNotify(swapTrace *trace, int intention);
void swapTraceCallback(swapTrace *trace);
void swapCmdSwapFinished(swapCmdTrace *swap_cmd);
void attachSwapTracesToSlowlog(void *ptr, swapCmdTrace *swap_cmd);
#endif /* __CTRIP_STORAGE_TRACE_H__ */