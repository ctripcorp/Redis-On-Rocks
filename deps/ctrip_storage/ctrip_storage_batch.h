/*
 * ctrip_storage_batch.h - 批量请求处理结构体（从 request.h 迁移）
 *
 * 包含：swapBatchCtxStat, swapBatchCtx（提交批量上下文）
 *      swapExecBatch（执行批量）及其 inline 辅助函数
 *      SWAP_BATCH_FLUSH_* 宏（批量刷新触发类型）
 */

#ifndef __CTRIP_STORAGE_BATCH_H__
#define __CTRIP_STORAGE_BATCH_H__

#include "ctrip_storage_request.h"


/* ==================== 批量刷新触发类型 ==================== */
#define SWAP_BATCH_FLUSH_FORCE_FLUSH    0
#define SWAP_BATCH_FLUSH_REACH_LIMIT    1
#define SWAP_BATCH_FLUSH_UTILS_TYPE     2
#define SWAP_BATCH_FLUSH_THREAD_SWITCH  3
#define SWAP_BATCH_FLUSH_INTENT_SWITCH  4
#define SWAP_BATCH_FLUSH_BEFORE_SLEEP   5
#define SWAP_BATCH_FLUSH_TYPES          6

/* ==================== 提交批量上下文 ==================== */

/* 提交批量统计信息 */
typedef struct swapBatchCtxStat {
  int stats_metric_idx_request;                         /* 请求指标索引 */
  int stats_metric_idx_batch;                           /* 批量指标索引 */
  long long submit_batch_count;                         /* 提交批量次数 */
  long long submit_request_count;                       /* 提交请求次数 */
  long long submit_batch_flush[SWAP_BATCH_FLUSH_TYPES]; /* 各类型刷新次数 */
} swapBatchCtxStat;

/* 提交批量上下文，管理请求批量提交 */
typedef struct swapBatchCtx {
  swapBatchCtxStat stat;          /* 统计信息 */
  swapRequestBatch *batch;        /* 当前批量 */
  int thread_idx;                 /* 目标线程索引 */
  int cmd_intention;              /* 当前命令意图 */
} swapBatchCtx;
swapBatchCtx *swapBatchCtxNew() ;
/* ==================== 执行批量（IO 线程侧） ==================== */

/* 执行批量，IO 线程按 intention/action 分组执行 */
typedef struct swapExecBatch {
    swapRequest *req_buf[SWAP_BATCH_DEFAULT_SIZE]; /* 内嵌请求缓冲区 */
    swapRequest **reqs;       /* 请求数组（超过缓冲区时动态分配） */
    size_t count;             /* 请求数量 */
    size_t capacity;          /* 容量 */
    int intention;            /* swap 意图 */
    int action;               /* 执行动作 */
    monotime swap_timer;      /* 计时器 */
} swapExecBatch;

static inline int swapExecBatchEmpty(swapExecBatch *exec_batch) {
    return exec_batch->count == 0;
}
static inline void swapExecBatchSetError(swapExecBatch *exec_batch, int errcode) {
    for (size_t i = 0; i < exec_batch->count; i++) {
        swapRequestSetError(exec_batch->reqs[i],errcode);
    }
}
static inline int swapExecBatchGetError(swapExecBatch *exec_batch) {
    int errcode;
    for (size_t i = 0; i < exec_batch->count; i++) {
        if ((errcode = swapRequestGetError(exec_batch->reqs[i])))
            return errcode;
    }
    return 0;
}

#define SWAP_BATCH_STATS_METRIC_SUBMIT_REQUEST 0
#define SWAP_BATCH_STATS_METRIC_SUBMIT_BATCH 1
#define SWAP_BATCH_STATS_METRIC_COUNT 2

#endif /* __CTRIP_STORAGE_BATCH_H__ */
