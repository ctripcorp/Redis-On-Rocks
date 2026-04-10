#include "ctrip_storage_thread.h"
#include "ctrip_storage_lock.h"
void swapThreadsGetInflightReqs(size_t swap_threads_inflight_reqs[]) {
    for (int i = EXTRA_SWAP_THREADS_NUM; i < server.storage.swap_total_threads_num; i++) {
        swapThread* thread = server.storage.swap_threads+ i;
        size_t reqs_count;
        atomicGet(thread->inflight_reqs, reqs_count);
        swap_threads_inflight_reqs[i] = reqs_count;
    }
}

void *swapThreadMain (void *arg) {
    char thdname[16];
    swapThread *thread = arg;

    snprintf(thdname, sizeof(thdname), "swap_thd_%d", thread->id);
    redis_set_thread_title(thdname);
#ifndef __APPLE__
    atomicIncr(server.storage.swap_threads_initialized, 1);
#endif
    listIter li;
    listNode *ln;
    list *processing_reqs;
    while (1) {
        pthread_mutex_lock(&thread->lock);
        while (listLength(thread->pending_reqs) == 0) {
            if (thread->stop) {
                pthread_mutex_unlock(&thread->lock);
#ifndef __APPLE__
                atomicDecr(server.storage.swap_threads_initialized, 1);
#endif
                return NULL;
            }
            if (thread->start_idle_time == -1) {
                thread->start_idle_time = ustime();
            }
            pthread_cond_wait(&thread->cond, &thread->lock);
        }
        thread->start_idle_time = -1;
        // During the process of copying a linked list, encountering data corruption could lead to the main thread getting stuck on pthread_mutex_lock, making it impossible to terminate the program normally. In this case, AddressSanitizer (ASan) fails to print the detection results, and no core dump file will be generated.
        processing_reqs = thread->pending_reqs;
        thread->pending_reqs = listCreate();
        pthread_mutex_unlock(&thread->lock);

        listRewind(processing_reqs, &li);
        atomicSetWithSync(thread->is_running_rio, 1);
        while ((ln = listNext(&li))) {
            swapRequestBatch *reqs = listNodeValue(ln);
            size_t reqs_count = reqs->count;
            swapRequestBatchProcess(reqs);
            atomicDecr(thread->inflight_reqs, reqs_count);
        }

        atomicSetWithSync(thread->is_running_rio, 0);
        listRelease(processing_reqs);
    }


}

/**
 * For the thread's initialization and destruction to be the last operations performed, they must ensure that all other threads have completed their execution.
 */
int swapThreadExtendAndInitThread() {
    long long start_time = ustime();
    serverAssert(server.storage.swap_total_threads_num < swapThreadsMaxNum());
    swapThread *thread = server.storage.swap_threads + server.storage.swap_total_threads_num;
    thread->id = server.storage.swap_total_threads_num;
    thread->pending_reqs = listCreate();
    thread->stop = false;
    thread->start_idle_time = -1;
    atomicSetWithSync(thread->is_running_rio, 0);
    pthread_mutex_init(&thread->lock, NULL);
    pthread_cond_init(&thread->cond, NULL);
    if (pthread_create(&thread->thread_id, NULL, swapThreadMain, thread)) {
        serverLog(LL_WARNING, "Fatal: create swap threads failed.");
        return -1;
    }
    server.storage.swap_total_threads_num++;
    serverLog(LL_WARNING, "create thread success use %lld us", ustime() - start_time);
    return C_OK;
}


int swapThreadsAutoScaleUp() {
    if (server.storage.swap_total_threads_num == swapThreadsMaxNum()) return 0;
    serverAssert(server.storage.swap_total_threads_num < swapThreadsMaxNum());
    if (swapThreadExtendAndInitThread() != C_OK) return 0;
    server.storage.swap_thread_auto_scale_up_cooling_down = false;
    #ifndef __APPLE__
        //Delay reset swap threads tid  （in swapThreadCpuUsageUpdate）
        server.storage.swap_cpu_usage->swap_threads_changed = true;
    #endif
    return 1;
}

int swapThreadsAutoScaleUpIfNeeded(size_t swap_threads_inflight_reqs[]) {
    if (!server.storage.swap_thread_auto_scale_up_cooling_down
        || server.storage.swap_total_threads_num == swapThreadsMaxNum()) return 0;
    if (server.storage.swap_total_threads_num >= swapThreadsCoreNum()) {
        bool need_scale_up = true;
        for (int i = EXTRA_SWAP_THREADS_NUM; i < server.storage.swap_total_threads_num; i++) {
            if (swap_threads_inflight_reqs[i] < (size_t)server.storage.swap_threads_auto_scale_up_threshold) {
                need_scale_up = false;
                break;
            }
        }
        if (!need_scale_up) return 0;
    }
    return swapThreadsAutoScaleUp();
}

#define SWAP_REQUEST_MEMORY_OVERHEAD (sizeof(swapRequest)+sizeof(swapCtx)+ \
                                      sizeof(wholeKeySwapData)/*typical*/+ \
                                      sizeof(lock)+sizeof(swapRequestBatch)/SWAP_BATCH_DEFAULT_SIZE)

void swapRequestBatchDispatched(swapRequestBatch *reqs) {
    size_t swap_memory = 0;

    if (server.storage.swap_debug_trace_latency) elapsedStart(&reqs->swap_queue_timer);

    for (size_t i = 0; i < reqs->count; i++) {
        swapRequest *req = reqs->reqs[i];
        if (req->trace) swapTraceDispatch(req->trace);
        req->swap_memory += SWAP_REQUEST_MEMORY_OVERHEAD;
        swap_memory += req->swap_memory;
    }

    atomicIncr(server.storage.swap_inprogress_batch,1);
    atomicIncr(server.storage.swap_inprogress_count,reqs->count);
    atomicIncr(server.storage.swap_inprogress_memory,swap_memory);
}

int swapThreadsSelectThreadIdx(size_t swap_threads_inflight_reqs[]) {
    size_t min_reqs_count = ULONG_MAX;
    size_t min_reqs_index = 0;
    int has_special_handling_thread = server.storage.swap_total_threads_num > swapThreadsCoreNum();
    int loop_end_index =  has_special_handling_thread ? server.storage.swap_total_threads_num - 1 : server.storage.swap_total_threads_num;
    for(int i = EXTRA_SWAP_THREADS_NUM; i < loop_end_index; i++) {
        if (swap_threads_inflight_reqs[i] < min_reqs_count) {
            min_reqs_count = swap_threads_inflight_reqs[i];
            min_reqs_index = i;
        }
    }
    /* The thread with the least tasks among non-special processing threads */
    if (min_reqs_count < (size_t)server.storage.swap_threads_auto_scale_up_threshold || 
        !has_special_handling_thread) {
        return min_reqs_index;
    }
    /* If the number of requests for a special processing thread is less than the current minimum, update the optimal thread index */
    if (swap_threads_inflight_reqs[server.storage.swap_total_threads_num -1] < min_reqs_count) {
        return server.storage.swap_total_threads_num - 1;
    }
    return min_reqs_index;
}


void swapThreadsDispatch(swapRequestBatch *reqs, int idx) {
    if (idx == -1) {
        size_t swap_threads_inflight_reqs[server.storage.swap_total_threads_num];
        swapThreadsGetInflightReqs(swap_threads_inflight_reqs);
        if (swapThreadsAutoScaleUpIfNeeded(swap_threads_inflight_reqs)) {
            idx = server.storage.swap_total_threads_num -1;
        } else {
            idx = swapThreadsSelectThreadIdx(swap_threads_inflight_reqs);
        }
        serverAssert(idx != -1);
    } else {
        serverAssert(idx < server.storage.swap_total_threads_num);
    }
    swapRequestBatchDispatched(reqs);
    swapThread *t = server.storage.swap_threads+idx;
    pthread_mutex_lock(&t->lock);
    listAddNodeTail(t->pending_reqs,reqs);
    atomicIncr(t->inflight_reqs, reqs->count);
    pthread_cond_signal(&t->cond);
    pthread_mutex_unlock(&t->lock);
}