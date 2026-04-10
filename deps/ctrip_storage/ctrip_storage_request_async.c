#include "ctrip_storage_request.h"
#include "ctrip_storage_thread.h"
#include "ctrip_storage_debug.h"
#include "ctrip_storage_metric.h"
#ifdef HAVE_EVENT_FD
#include <sys/eventfd.h>
#endif
void asyncCompleteQueueAppend(asyncCompleteQueue *cq, swapRequestBatch *reqs) {
    uint64_t val = 1;
    pthread_mutex_lock(&cq->lock);
    listAddNodeTail(cq->complete_queue, reqs);
    pthread_mutex_unlock(&cq->lock);
    if (write(cq->eventfd, &val, sizeof(val)) < 0 && errno != EAGAIN) {
        static mstime_t prev_log;
        if (server.mstime - prev_log >= 1000) {
            prev_log = server.mstime;
            serverLog(LL_NOTICE, "[rocks] notify rio finish failed: %s",
                    strerror(errno));
        }
    }
}
void asyncSwapRequestNotifyCallback(swapRequestBatch *reqs, void *pd) {
    UNUSED(pd);
    asyncCompleteQueueAppend(server.storage.swap_CQ, reqs);
}

void asyncSwapRequestBatchSubmit(swapRequestBatch *reqs, int idx) {
    reqs->notify_cb = asyncSwapRequestNotifyCallback;
    reqs->notify_pd = NULL;
    swapThreadsDispatch(reqs, idx);
}




/* --- Async rocks io start --- */
int asyncCompleteQueueProcess(asyncCompleteQueue *cq) {
    int processed;
    listIter li;
    listNode *ln;
    list *processing_reqs = listCreate();
    monotime process_timer = 0;
    if (server.storage.swap_debug_trace_latency) elapsedStart(&process_timer);

    pthread_mutex_lock(&cq->lock);
    listRewind(cq->complete_queue, &li);
    while ((ln = listNext(&li))) {
        listAddNodeTail(processing_reqs, listNodeValue(ln));
        listDelNode(cq->complete_queue, ln);
    }
    pthread_mutex_unlock(&cq->lock);

    listRewind(processing_reqs, &li);
    while ((ln = listNext(&li))) {
        swapRequestBatch *reqs = listNodeValue(ln);
        if (reqs->notify_queue_timer) {
            metricDebugInfo(SWAP_DEBUG_NOTIFY_QUEUE_WAIT, elapsedUs(reqs->notify_queue_timer));
        }
        swapRequestBatchCallback(reqs);
        swapRequestBatchFree(reqs);
    }

    processed = listLength(processing_reqs);
    listRelease(processing_reqs);
    if (server.storage.swap_debug_trace_latency) {
        metricDebugInfo(SWAP_DEBUG_NOTIFY_QUEUE_HANDLES, processed);
        metricDebugInfo(SWAP_DEBUG_NOTIFY_QUEUE_HANDLE_TIME, elapsedUs(process_timer));
    }
    return processed;
}

/* read before unlink clients so that main thread won't miss notify event:
 * rocksb thread: 1. link req; 2. send notify byte;
 * main thread: 1. read notify bytes; 2. unlink req;
 * if main thread read less notify bytes than unlink clients num (e.g. rockdb
 * thread link more clients when , main thread would still be triggered because
 * epoll LT-triggering mode. */
void asyncCompleteQueueHanlder(aeEventLoop *el, int eventfd, void *privdata, int mask) {
    uint64_t val;

    UNUSED(el);
    UNUSED(mask);

    int nread = read(eventfd, &val, sizeof(val));
    if (nread == 0) {
        serverLog(LL_WARNING, "[rocks] notify recv fd closed.");
    } else if (nread < 0) {
        serverLog(LL_WARNING, "[rocks] read notify failed: %s",
                strerror(errno));
    }

    asyncCompleteQueueProcess(privdata);
}
int asyncCompleteQueueInit() {
    asyncCompleteQueue *cq = zcalloc(sizeof(asyncCompleteQueue));


    pthread_mutex_init(&cq->lock, NULL);

    cq->complete_queue = listCreate();
    cq->eventfd = eventfd(0, EFD_NONBLOCK);

    if (aeCreateFileEvent(server.el, cq->eventfd,
                AE_READABLE, asyncCompleteQueueHanlder, cq) == AE_ERR) {
        serverLog(LL_WARNING,"Fatal: create notify recv event failed: %s",
                strerror(errno));
        return -1;
    }

    server.storage.swap_CQ = cq;
    return 0;
}
/* --- Async rocks io end --- */