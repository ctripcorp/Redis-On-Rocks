/*
 * ctrip_storage_metric.h - 统计和监控相关结构体
 *
 * 包含：swapHitStat（命中统计）、swapDebugInfo（调试信息）
 *      以及 metricDebugInfo 接口
 */

#ifndef __CTRIP_STORAGE_METRIC_H__
#define __CTRIP_STORAGE_METRIC_H__
#include "server.h"
#include "ctrip_storage_debug.h"

/* ==================== 统计结构体（从 types.h 迁移） ==================== */

/* swap 命中统计，跟踪各类 swap-in 请求的命中/过滤情况 */
typedef struct swapHitStat {
    redisAtomic long long stat_swapin_attempt_count;                              /* swap-in 尝试总次数 */
    redisAtomic long long stat_swapin_not_found_coldfilter_cuckoofilter_filt_count; /* 被 cuckoo filter 过滤的次数 */
    redisAtomic long long stat_swapin_not_found_coldfilter_absentcache_filt_count;  /* 被 absent cache 过滤的次数 */
    redisAtomic long long stat_swapin_not_found_coldfilter_miss_count;              /* cold filter 未命中次数 */
    redisAtomic long long stat_swapin_no_io_count;                                  /* 无需 IO 的次数 */
    redisAtomic long long stat_swapin_data_not_found_count;                         /* 数据未找到次数 */
    redisAtomic long long stat_absent_subkey_query_count;                           /* absent subkey 查询次数 */
    redisAtomic long long stat_absent_subkey_filt_count;                            /* absent subkey 被过滤次数 */
} swapHitStat;

/* swap 调试信息，用于运行时监控 */
typedef struct swapDebugInfo {
    const char *name;              /* 调试信息名称 */
    redisAtomic size_t count;      /* 计数器 */
    redisAtomic size_t value;      /* 值 */
    int metric_idx_count;          /* 指标索引（计数） */
    int metric_idx_value;          /* 指标索引（值） */
} swapDebugInfo;


void metricDebugInfo(int type, long val);

/* ror stat */
typedef struct swapStat {
    const char *name;
    redisAtomic size_t count;
    redisAtomic size_t batch;
    redisAtomic size_t memory;
    redisAtomic size_t time;
    int stats_metric_idx_count;
    int stats_metric_idx_batch;
    int stats_metric_idx_memory;
    int stats_metric_idx_time;
} swapStat;

typedef struct compactionFilterStat {
    const char *name;
    redisAtomic long long filt_count;
    redisAtomic long long scan_count;
    redisAtomic long long rio_count;
    int stats_metric_idx_filt;
    int stats_metric_idx_scan;
    int stats_metric_idx_rio;
} compactionFilterStat;
typedef struct rorStat {
    struct swapStat *swap_stats; /* array of swap stats (one for each swap type). */
    struct swapStat *rio_stats; /* array of rio stats (one for each rio type). */
    struct compactionFilterStat *compaction_filter_stats; /* array of compaction filter stats (one for each column family). */
} rorStat;



#endif /* __CTRIP_STORAGE_METRIC_H__ */