#include "ctrip_storage_trace.h"
#include <assert.h>

inline void swapTraceLock(swapTrace *trace) {
    trace->swap_lock_time = getMonotonicUs();
}
void initSwapTraces(swapCmdTrace *swap_cmd, int swap_cnt) {
    assert(NULL == swap_cmd->swap_traces && 0 == swap_cmd->swap_cnt);
    swap_cmd->swap_cnt = swap_cnt;
    swap_cmd->swap_traces = zcalloc(swap_cnt * sizeof(swapTrace));
}
void swapCmdTraceFree(swapCmdTrace *trace) {
    if (trace->swap_traces) zfree(trace->swap_traces);
    zfree(trace);
}

inline void swapTraceDispatch(swapTrace *trace) {
    trace->swap_dispatch_time = getMonotonicUs();
}

inline void swapTraceNotify(swapTrace *trace, int intention) {
    trace->intention = intention;
    trace->swap_notify_time = getMonotonicUs();
}


inline void swapTraceProcess(swapTrace *trace) {
    trace->swap_process_time = getMonotonicUs();
}

inline void swapTraceCallback(swapTrace *trace) {
    trace->swap_callback_time = getMonotonicUs();
}
