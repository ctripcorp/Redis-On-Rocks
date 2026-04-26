#ifndef __CTRIP_STORAGE_THREAD_H__
#define __CTRIP_STORAGE_THREAD_H__

#include "ctrip_storage_request.h"
#include <stdbool.h>

/* ==================== swapThread（从 types.h 迁移） ==================== */

/* swap IO 线程，每个线程有独立的锁、条件变量和待处理请求队列 */
typedef struct swapThread {
    int id;                                   /* 线程序号 */
    pthread_t thread_id;                      /* 系统线程 ID */
    pthread_mutex_t lock;                     /* 互斥锁 */
    pthread_cond_t cond;                      /* 条件变量 */
    list *pending_reqs;                       /* 待处理请求队列 */
    redisAtomic unsigned long is_running_rio; /* 是否正在执行 RIO */
    redisAtomic size_t inflight_reqs;         /* 进行中的请求数 */
    bool stop;                                /* 停止标志 */
    long long start_idle_time;                /* 空闲开始时间 */
} swapThread;

/* Threads (encode/rio/decode/finish) */
#define SWAP_THREADS_DEFAULT     4
#define SWAP_THREADS_MAX         64
#define EXTRA_SWAP_THREADS_NUM   2
void swapThreadsDispatch(swapRequestBatch *reqs, int idx);


static inline int swapThreadsMaxNum() {
    return EXTRA_SWAP_THREADS_NUM + server.storage.swap_threads_auto_scale_max;
}

static inline int swapThreadsCoreNum() {
    return EXTRA_SWAP_THREADS_NUM + server.storage.swap_threads_auto_scale_min;
}


#ifndef __APPLE__
typedef struct swapThreadCpuUsage{
    /* CPU usage Cacluation */
    double main_thread_cpu_usage;
    double swap_threads_cpu_usage;
    double other_threads_cpu_usage;

    double main_thread_ticks_save;
    double *swap_thread_ticks_save;
    double process_cpu_ticks_save;

    int main_tid[1];
    int *swap_tids;
    bool swap_threads_changed;
    pid_t pid;
    double hertz;
    double uptime_save;
}swapThreadCpuUsage;

void swapThreadCpuUsageUpdate(swapThreadCpuUsage *cpu_usage);
void swapThreadCpuUsageFree(swapThreadCpuUsage *cpu_usage);
struct swapThreadCpuUsage *swapThreadCpuUsageNew(void);
int swapThreadCpuUsageResetTids(swapThreadCpuUsage *cpu_usage);
sds genRedisThreadCpuUsageInfoString(sds info, swapThreadCpuUsage *cpu_usage);
#endif
#endif /* __CTRIP_STORAGE_THREAD_H__ */