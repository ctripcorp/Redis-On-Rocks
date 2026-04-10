/*
 * ctrip_storage_types.h - 存储引擎类型定义（无循环依赖）
 *
 * 此头文件定义 server.h 所需的存储引擎相关类型。
 * 不包含 server.h，避免循环依赖。
 * 注意：此文件必须在 dict.h, adlist.h, sds.h, atomicvar.h 之后包含。
 * server.h 已确保正确的包含顺序。
 */

#ifndef __CTRIP_STORAGE_TYPES_H__
#define __CTRIP_STORAGE_TYPES_H__

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <stdbool.h>

/* ==================== 存储引擎基础类型 ==================== */

/* 存储引擎类型枚举 */
typedef enum StorageType {
    STORAGE_TYPE_NONE,      /* Redis 内置存储引擎 */
    STORAGE_TYPE_MEMORY,    /* 内存存储引擎测试框架使用 */
    STORAGE_TYPE_ROCKSDB    /* 用户自定义存储引擎 */
} StorageType;

/* 存储引擎接口 */
typedef struct StorageEngine {
    void* context;  /* 存储引擎上下文，可以包含连接信息等 */
    int (*put)(void* context, struct RIO* rio);
    int (*get)(void* context, struct RIO* rio);
    int (*del)(void* context, struct RIO* rio);
    int (*iterate)(void* context, struct RIO* rio);
    int (*batch_get)(void* context, struct RIOBatch* batch);
    int (*batch_put)(void* context, struct RIOBatch* batch);
    int (*batch_del)(void* context, struct RIOBatch* batch);
    /* utils */
    int (*compact_range)(void* context, void* pd);
    int (*get_stats)(void* context, void* pd);
    int (*flush)(void* context, void* pd);
    int (*create_checkpoint)(void* context, void* pd);
    int (*collect_cf_meta)(void* context, void* pd);
} StorageEngine;


/* ==================== Server 级别存储命名空间 ==================== */

typedef enum {
    SWAP_REWIND_OFF = 0, /* rewind off */
    SWAP_REWIND_WRITE,   /* rewind client with write commands */
    SWAP_REWIND_ALL,     /* rewind client with any commands */
} swap_rewind_type;

/* swapHitStat 和 swapDebugInfo 已迁移到 ctrip_storage_metric.h */
/* StorageServerNamespace 通过 struct 指针引用，无需完整定义 */

/* swap block - swapUnblockCtx 完整定义在 ctrip_storage_client.h */

typedef struct swapBatchLimitsConfig {
    int count;
    unsigned long long mem;
} swapBatchLimitsConfig;

/* asyncCompleteQueue 和 parallelSync 完整定义已迁移到 ctrip_storage_request.h */
/* StorageServerNamespace 通过 struct 指针引用，无需完整定义 */

/* swapThread 完整定义已迁移到 ctrip_storage_thread.h */

struct swapBatchLimitsConfig;
#define SWAP_TYPES_FORWARD 5

/* 存储引擎 Server 命名空间，嵌入 struct server */

typedef struct StorageServerNamespace {
    /* async */
    struct asyncCompleteQueue *swap_CQ; /* 异步完成队列，用于IO线程通知主线程完成异步操作 */
    /* parallel sync */ 
    struct parallelSync *swap_parallel_sync;
    /* maxmemory*/
    unsigned long long maxmemory_scale_from; 
    unsigned long long maxmemory_scaledown_rate; /* Number of bytes actually scale down maxmemory every seconds */ 
    /*ratelimit*/
    int swap_ratelimit_maxmemory_percentage; 
    int swap_ratelimit_maxmemory_pause_growth_rate; 
    long long stat_swap_ratelimit_client_pause_ms; 
    long long stat_swap_ratelimit_client_pause_count; 
    long long stat_swap_ratelimit_rejected_cmd_count; 
    int swap_ratelimit_policy;
    int swap_ratelimit_persist_lag; 
    int swap_ratelimit_persist_pause_growth_rate; 
    struct StorageEngine* engine;
    int64_t swap_txid; /* swap transaction id generator, only used for trace and debug */
    /* swap block*/ 
    struct swapUnblockCtx* swap_dependency_block_ctx;
    /* absent cache */ 
    int swap_absent_cache_enabled; 
    int swap_absent_cache_include_subkey; 
    unsigned long long swap_absent_cache_capacity; 

    
     
    redisAtomic size_t swap_inprogress_batch; /* swap request inprogress batch */ 
    redisAtomic size_t swap_inprogress_count; /* swap request inprogress count */ 
    redisAtomic size_t swap_inprogress_memory;  /* swap consumed memory in bytes */ 
    redisAtomic size_t swap_error_count;  /* swap error count */  
    

    /* request wait */ 
    struct swapLock *swap_lock; 
    /* swap scan session */ 
    struct swapScanSessions *swap_scan_sessions;
    int swap_scan_session_bits; 
    int swap_scan_session_max_idle_seconds; 
    /* swap batch */
    struct swapBatchCtx *swap_batch_ctx;
    struct swapBatchLimitsConfig swap_batch_limits[SWAP_TYPES_FORWARD];
    /* swap persist */ 
    int swap_persist_enabled; 
    struct swapPersistCtx* swap_persist_ctx;
    /* repl swap */ 
    int swap_repl_workers;   /* num of repl worker clients */ 
    list *swap_repl_worker_clients_free; /* free clients for repl(slaveof & peerof) swap. */ 
    list *swap_repl_worker_clients_used; /* used clients for repl swap. */ 
    list *swap_repl_swapping_clients; /* list of repl swapping clients. */ 
    /* rewind */
    swap_rewind_type swap_rewind_type;
    list* swap_torewind_clients;  /* 待 rewind 队列 */
    list* swap_rewinding_clients; /* 正在 rewind 队列 */
    /* swap debug */
    int swap_debug_rio_delay_micro; /* sleep swap_debug_rio_delay microsencods to simulate ssd delay. */  
    int swap_debug_swapout_notify_delay_micro; /* sleep swap_debug_swapout_notify_delay microsencods  
                                        to simulate notify queue blocked after swap out */  
    int swap_debug_before_exec_swap_delay_micro; /* sleep swap_debug_before_exec_swap_delay microsencods before exec swap request */  
    int swap_debug_init_rocksdb_delay_micro; /* sleep swap_debug_init_rocksdb_delay microsencods before init rocksdb */ 
    int swap_debug_rio_error; /* mock rio error */  
    int swap_debug_rio_error_action;  
    int swap_debug_trace_latency; /* 是否开启异步完成队列调试日志 */
    int swap_debug_bgsave_metalen_addition; 
    int swap_debug_compaction_filter_delay_micro; 
    int swap_debug_rdb_key_save_delay_micro;  
    /* version */
    uint64_t swap_key_version;
    /*bitmap*/
    size_t swap_bitmap_subkey_size;
    redisAtomic unsigned long long swap_bitmap_switched_to_string_count; 
    redisAtomic unsigned long long swap_string_switched_to_bitmap_count; 
    /* swap 线程 */
    int swap_defer_thread_idx;
    int swap_util_thread_idx;
    int swap_total_threads_num;
    int swap_thread_auto_scale_up_cooling_down; /* the thread pool can only be expanded once within a period of time */ 
    int swap_threads_auto_scale_max;  /* upper limit of thread pool size  */ 
    int swap_threads_auto_scale_min;      /* lower limit of thread pool size*/ 
    int swap_threads_auto_scale_up_threshold; /* when the number of requests exceeds a certain threshold, a new thread is created */ 
    int swap_threads_auto_scale_down_idle_seconds; 
#ifdef __APPLE__
#else
    /* swap_cpu_usage */ 
    redisAtomic int swap_threads_initialized; 
    struct swapThreadCpuUsage *swap_cpu_usage;
#endif
    struct swapThread *swap_threads;
    /* swap 统计 */
    struct swapHitStat *swap_hit_stats;
    struct swapDebugInfo *swap_debug_info;
    struct rorStat *ror_stats;

    
    /* 配置属性 */
    StorageType type;  /* 存储引擎类型 */
    int swap_dirty_subkeys_enabled; /* 是否开启脏子键功能 */
    int swap_cuckoo_filter_enabled;
    int swap_cuckoo_filter_bit_type;
    unsigned long long swap_cuckoo_filter_estimated_keys;
    /*bitmap*/
    int swap_rdb_bitmap_encode_enabled;
    int swap_bitmap_subkeys_enabled; 
} StorageServerNamespace;



/* 存储引擎 DB 命名空间，嵌入 redisDb */
typedef struct {
    dict *meta;                 /* meta for rocksdb subkeys of big object. */
    dict *dirty_subkeys;        /* dirty subkeys. */
    list *evict_asap;           /* keys to be evicted asap. */
    long long cold_keys;        /* # of cold keys. */
    sds randomkey_nextseek;     /* nextseek for randomkey command */
    struct scanExpire *scan_expire; /* scan expire related */
    struct coldFilter *cold_filter; /* cold keys filter */
} StorageDBNamespace;

/* ==================== Deferred Command 前向声明 ==================== */
/* 完整定义已迁移到 ctrip_storage_client.h */
typedef struct swapMstate swapMstate;
typedef struct deferredCommand deferredCommand;



#endif /* __CTRIP_STORAGE_TYPES_H__ */
