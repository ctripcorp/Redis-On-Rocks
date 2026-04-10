#include "ctrip_storage_request.h"
#include "ctrip_storage_thread.h"
static int parallelSwapProcess(swapEntry *e) {
    if (e->inprogress) {
        char c;
        if (read(e->pipe_read_fd, &c, 1) != 1) {
            serverLog(LL_WARNING, "wait swap entry failed: %s",
                    strerror(errno));
            return C_ERR;
        }
        serverAssert(c == 'x');
        swapRequestBatchCallback(e->reqs);
        swapRequestBatchFree(e->reqs);
        e->reqs = NULL;
        e->inprogress = 0;
    }
    return C_OK;
}

void parallelSyncSwapNotifyCallback(swapRequestBatch *reqs, void *pd) {
    swapEntry *e = pd;
    UNUSED(reqs);
    /* Notify svr to progress */
    if (write(e->pipe_write_fd, "x", 1) < 1 && errno != EAGAIN) {
        static mstime_t prev_log;
        if (server.mstime - prev_log >= 1000) {
            prev_log = server.mstime;
            serverLog(LL_NOTICE,
                    "[rocks] notify rio finish failed: %s",
                    strerror(errno));
        }
    }
}

/* Submit one swap (task). swap will start and finish in submit order. */
int parallelSyncSwapRequestBatchSubmit(swapRequestBatch *reqs, int idx) {
    listNode *ln;
    swapEntry *e;
    parallelSync *ps = server.storage.swap_parallel_sync;
    /* wait and handle previous swap */
    if (!(ln = listFirst(ps->entries))) return C_ERR;
    e = listNodeValue(ln);
    if (parallelSwapProcess(e)) return C_ERR;
    listRotateHeadToTail(ps->entries);
    /* submit */
    reqs->notify_cb = parallelSyncSwapNotifyCallback;
    reqs->notify_pd = e;
    e->reqs = reqs;
    e->inprogress = 1;
    swapThreadsDispatch(reqs,idx);
    return C_OK;
}