/* Copyright (c) 2021, ctrip.com * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "ctrip_swap.h"



void *swapThreadMain (void *arg) {
    char thdname[16];
    swapThread *thread = arg;

    snprintf(thdname, sizeof(thdname), "swap_thd_%d", thread->id);
    redis_set_thread_title(thdname);
#ifndef __APPLE__
    atomicIncr(server.swap_threads_initialized, 1);
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
                    atomicDecr(server.swap_threads_initialized, 1);
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
    serverAssert(server.swap_total_threads_num < swapThreadsMaxNum());
    swapThread *thread = server.swap_threads + server.swap_total_threads_num;
    thread->id = server.swap_total_threads_num;
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
    server.swap_total_threads_num++;
    serverLog(LL_WARNING, "create thread success use %lld us", ustime() - start_time);
    return C_OK;
}

int swapThreadsInit() {
    int i;
    server.swap_defer_thread_idx = 0;
    server.swap_util_thread_idx = 1; 
    server.swap_threads = zcalloc(sizeof(swapThread)*(swapThreadsMaxNum()));
    for (i = 0; i < swapThreadsCoreNum(); i++) {
        swapThreadExtendAndInitThread();
    }

    return 0;
}

void swapThreadsDeinit() {
    int i, err;
    for (i = 0; i < server.swap_total_threads_num; i++) {
        swapThread *thread = server.swap_threads+i;
        listRelease(thread->pending_reqs);
        if (thread->thread_id == pthread_self()) continue;
        if (thread->thread_id && pthread_cancel(thread->thread_id) == 0) {
            if ((err = pthread_join(thread->thread_id, NULL)) != 0) {
                serverLog(LL_WARNING, "swap thread #%d can't be joined: %s",
                        i, strerror(err));
            } else {
                serverLog(LL_WARNING, "swap thread #%d terminated.", i);
            }
        }
    }
}

static inline int swapThreadsDistNext() {
    static int dist;
    dist++;
    if (dist < 0) dist = 0;
    return dist;
}

void swapThreadDestroy(swapThread* thread) {
    listRelease(thread->pending_reqs);
    thread->pending_reqs = NULL;
    pthread_cond_destroy(&thread->cond);
    pthread_mutex_destroy(&thread->lock);
}

int swapThreadReduceAndCleanupThread() {
    long long start_time = ustime();
    int idx = server.swap_total_threads_num - 1;
    swapThread* thread = server.swap_threads + idx;
    serverAssert(thread->stop == false);
    size_t inflight_reqs;
    atomicGet(thread->inflight_reqs, inflight_reqs);
    serverAssert(inflight_reqs == 0);
    pthread_mutex_lock(&thread->lock);
    serverAssert(listLength(thread->pending_reqs) == 0);
    thread->stop = true;
    pthread_cond_signal(&thread->cond);
    pthread_mutex_unlock(&thread->lock);
    int res = pthread_join(thread->thread_id, NULL);
    serverAssert(res == 0);
    server.swap_total_threads_num--;
    swapThreadDestroy(thread);  
    serverLog(LL_WARNING, "delete thread  use %lld us", ustime() - start_time);                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
    return C_OK;
}

int swapThreadsAutoScaleDown() {
    if (server.swap_total_threads_num <= swapThreadsCoreNum()) return 0;
    if (swapThreadReduceAndCleanupThread() == C_OK) {
        #ifndef __APPLE__
            //Delay reset swap threads tid  （in swapThreadCpuUsageUpdate）
            server.swap_cpu_usage->swap_threads_changed = true;
        #endif  
        return 1;
    }
    return 0;
}

int swapThreadsAutoScaleDownIfNeeded(void) {
    //If capacity expansion occurs during this cycle, no capacity reduction will be performed.
    if (!server.swap_thread_auto_scale_up_cooling_down) return 0; 
    if (server.swap_total_threads_num > swapThreadsCoreNum()) {
        swapThread* thread = server.swap_threads + server.swap_total_threads_num -1;
        long long start_idle_time;
        pthread_mutex_lock(&thread->lock);
        start_idle_time = thread->start_idle_time;
        pthread_mutex_unlock(&thread->lock);
        if (start_idle_time == -1) return 0;
        if (((ustime() - start_idle_time) / 1000000) > server.swap_threads_auto_scale_down_idle_seconds) {
            return swapThreadsAutoScaleDown();
        }
    }
    return 0;
}

void swapThreadsGetInflightReqs(size_t swap_threads_inflight_reqs[]) {
    for (int i = EXTRA_SWAP_THREADS_NUM; i < server.swap_total_threads_num; i++) {
        swapThread* thread = server.swap_threads+ i;
        size_t reqs_count;
        atomicGet(thread->inflight_reqs, reqs_count);
        swap_threads_inflight_reqs[i] = reqs_count;
    }
}
int swapThreadsAutoScaleUp() {
    if (server.swap_total_threads_num == swapThreadsMaxNum()) return 0;
    serverAssert(server.swap_total_threads_num < swapThreadsMaxNum());
    if (swapThreadExtendAndInitThread() != C_OK) return 0;
    server.swap_thread_auto_scale_up_cooling_down = false;
    #ifndef __APPLE__
        //Delay reset swap threads tid  （in swapThreadCpuUsageUpdate）
        server.swap_cpu_usage->swap_threads_changed = true;
    #endif
    return 1;
}
int swapThreadsAutoScaleUpIfNeeded(size_t swap_threads_inflight_reqs[]) {
    if (!server.swap_thread_auto_scale_up_cooling_down
        || server.swap_total_threads_num == swapThreadsMaxNum()) return 0;
    if (server.swap_total_threads_num >= swapThreadsCoreNum()) {
        bool need_scale_up = true;
        for (int i = EXTRA_SWAP_THREADS_NUM; i < server.swap_total_threads_num; i++) {
            if (swap_threads_inflight_reqs[i] < (size_t)server.swap_threads_auto_scale_up_threshold) {
                need_scale_up = false;
                break;
            }
        }
        if (!need_scale_up) return 0;
    }
    return swapThreadsAutoScaleUp();
}



int swapThreadsSelectThreadIdx(size_t swap_threads_inflight_reqs[]) {
    size_t min_reqs_count = ULONG_MAX;
    size_t min_reqs_index = 0;
    int has_special_handling_thread = server.swap_total_threads_num > swapThreadsCoreNum();
    int loop_end_index =  has_special_handling_thread ? server.swap_total_threads_num - 1 : server.swap_total_threads_num;
    for(int i = EXTRA_SWAP_THREADS_NUM; i < loop_end_index; i++) {
        if (swap_threads_inflight_reqs[i] < min_reqs_count) {
            min_reqs_count = swap_threads_inflight_reqs[i];
            min_reqs_index = i;
        }
    }
    /* The thread with the least tasks among non-special processing threads */
    if (min_reqs_count < (size_t)server.swap_threads_auto_scale_up_threshold || 
        !has_special_handling_thread) {
        return min_reqs_index;
    }
    /* If the number of requests for a special processing thread is less than the current minimum, update the optimal thread index */
    if (swap_threads_inflight_reqs[server.swap_total_threads_num -1] < min_reqs_count) {
        return server.swap_total_threads_num - 1;
    }
    return min_reqs_index;
}

void swapThreadsDispatch(swapRequestBatch *reqs, int idx) {
    if (idx == -1) {
        size_t swap_threads_inflight_reqs[server.swap_total_threads_num];
        swapThreadsGetInflightReqs(swap_threads_inflight_reqs);
        if (swapThreadsAutoScaleUpIfNeeded(swap_threads_inflight_reqs)) {
            idx = server.swap_total_threads_num -1;
        } else {
            idx = swapThreadsSelectThreadIdx(swap_threads_inflight_reqs);
        }
        serverAssert(idx != -1);
    } else {
        serverAssert(idx < server.swap_total_threads_num);
    }
    swapRequestBatchDispatched(reqs);
    swapThread *t = server.swap_threads+idx;
    pthread_mutex_lock(&t->lock);
    listAddNodeTail(t->pending_reqs,reqs);
    atomicIncr(t->inflight_reqs, reqs->count);
    pthread_cond_signal(&t->cond);
    pthread_mutex_unlock(&t->lock);
}

int swapThreadsDrained() {
    swapThread *rt;
    int drained = 1, i;
    for (i = 0; i < server.swap_total_threads_num; i++) {
        rt = server.swap_threads+i;

        pthread_mutex_lock(&rt->lock);
        unsigned long count = 0;
        atomicGetWithSync(rt->is_running_rio, count);
        if (listLength(rt->pending_reqs) || count) drained = 0;
        pthread_mutex_unlock(&rt->lock);
    }
    return drained;
}

// utils task
#define ROCKSDB_UTILS_TASK_DONE 0
#define ROCKSDB_UTILS_TASK_DOING 1

rocksdbUtilTaskManager* createRocksdbUtilTaskManager() {
    rocksdbUtilTaskManager * manager = zmalloc(sizeof(rocksdbUtilTaskManager));
    for(int i = 0; i < ROCKSDB_EXCLUSIVE_TASK_COUNT;i++) {
        manager->stats[i].stat = ROCKSDB_UTILS_TASK_DONE;
    }
    return manager;
}
int isUtilTaskExclusive(int type) {
    return type < ROCKSDB_EXCLUSIVE_TASK_COUNT;
}
int isRunningUtilTask(rocksdbUtilTaskManager* manager, int type) {
    serverAssert(type < ROCKSDB_EXCLUSIVE_TASK_COUNT);
    return manager->stats[type].stat == ROCKSDB_UTILS_TASK_DOING;
}

void rocksdbUtilTaskSwapFinished(swapData *data_, void *utilctx_, int errcode) {
    rocksdbUtilTaskCtx *utilctx = utilctx_;
    UNUSED(data_);
    if (utilctx->finish_cb) utilctx->finish_cb(utilctx->result, utilctx->finish_pd, errcode);
    if (isUtilTaskExclusive(utilctx->type))
        server.swap_util_task_manager->stats[utilctx->type].stat = ROCKSDB_UTILS_TASK_DONE;
    zfree(utilctx);
}

int submitUtilTask(int type, void *arg, rocksdbUtilTaskCallback cb, void* pd, sds* error) {
    swapRequest *req = NULL;
    rocksdbUtilTaskCtx *utilctx = NULL;

    if (isUtilTaskExclusive(type)) {
        if (isRunningUtilTask(server.swap_util_task_manager, type)) {
            if(error != NULL) *error = sdsnew("task running");
            return 0;
        }
        server.swap_util_task_manager->stats[type].stat = ROCKSDB_UTILS_TASK_DOING;
    }

    utilctx = zcalloc(sizeof(rocksdbUtilTaskCtx));
    utilctx->type = type;
    utilctx->argument = arg;
    utilctx->result = NULL;
    utilctx->finish_cb = cb;
    utilctx->finish_pd = pd;

    req = swapDataRequestNew(SWAP_UTILS,type,NULL,NULL,NULL,NULL,
            rocksdbUtilTaskSwapFinished,utilctx,NULL);
    submitSwapRequest(SWAP_MODE_ASYNC,req,server.swap_util_thread_idx);
    return 1;
}

sds genSwapThreadInfoString(sds info) {
    size_t thread_depth = 0, async_depth, thread_inflight_reqs = 0, swap_thread_num = (server.swap_total_threads_num - EXTRA_SWAP_THREADS_NUM);

    pthread_mutex_lock(&server.swap_CQ->lock);
    async_depth = listLength(server.swap_CQ->complete_queue);
    pthread_mutex_unlock(&server.swap_CQ->lock);
    for (int i = EXTRA_SWAP_THREADS_NUM; i < server.swap_total_threads_num; i++) {
        swapThread *thread = server.swap_threads+i;
        pthread_mutex_lock(&thread->lock);
        thread_depth += listLength(thread->pending_reqs);
        pthread_mutex_unlock(&thread->lock);
        size_t inflight_reqs;
        atomicGet(thread->inflight_reqs, inflight_reqs);
        thread_inflight_reqs += inflight_reqs;
    }
    thread_depth /= swap_thread_num;
    thread_inflight_reqs /= swap_thread_num;
    info = sdscatprintf(info,
            "swap_thread_num:%lu\r\n"
            "swap_thread_queue_depth:%lu\r\n"
            "swap_async_queue_depth:%lu\r\n"
            "swap_thread_inflight_reqs:%lu\r\n",
            swap_thread_num, thread_depth, async_depth, thread_inflight_reqs);

    return info;
}
