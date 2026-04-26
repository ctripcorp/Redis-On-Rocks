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

/* Forward declarations for structs defined in ctrip_storage_rio.h */
struct RIO;
struct RIOBatch;

/* Forward declarations for structs defined in server.h */
struct redisDb;
struct rio;

/* ==================== 存储引擎基础类型 ==================== */

/* 存储引擎类型枚举 */
typedef enum StorageType {
    STORAGE_TYPE_NONE,      /* Redis 内置存储引擎 */
    STORAGE_TYPE_MEMORY,    /* 内存存储引擎测试框架使用 */
    STORAGE_TYPE_ROCKSDB    /* 用户自定义存储引擎 */
} StorageType;

struct StorageForkCtx;
typedef struct StorageForkCtxType {
  int (*init)(struct StorageForkCtx *sfrctx);
  int (*beforeFork)(struct StorageForkCtx *sfrctx);
  int (*afterForkChild)(struct StorageForkCtx *sfrctx);
  int (*afterForkParent)(struct StorageForkCtx *sfrctx, int childpid);
  int (*deinit)(struct StorageForkCtx *sfrctx);
} StorageForkCtxType;

typedef struct StorageForkCtx {
    StorageForkCtxType* type;
    void* context;
    struct rdbSaveInfo* info;
} StorageForkCtx;

typedef struct  RdbSaveCtx RdbSaveCtx;
typedef struct RdbLoadCtx RdbLoadCtx;
typedef enum SAVE_RESULT_ENUM {
    SAVE_NOP,
    SAVE_SUCC,
    SAVE_FAIL
} SAVE_RESULT_ENUM;
typedef struct RdbSaveCtxType  {
    int (*free_rdb_save_ctx)(RdbSaveCtx* ctx, struct rio* rdb);
    int (*save_db_init)(RdbSaveCtx* ctx, struct rio* rdb,struct redisDb* db);
    int (*save_db_deinit)(RdbSaveCtx* ctx, struct rio* rdb,struct redisDb* db);
    enum SAVE_RESULT_ENUM (*save_hot_key)(RdbSaveCtx* ctx, struct rio* rdb,struct redisDb* db, robj* key, robj* val);
    enum SAVE_RESULT_ENUM (*save_cold_keys)(RdbSaveCtx* ctx, struct rio* rdb, struct redisDb* db);
} RdbSaveCtxType;

typedef struct RdbSaveCtx {
    void* ctx;
    RdbSaveCtxType* type;
} RdbSaveCtx;

/* RDB 加载结果枚举，对称于 SAVE_RESULT_ENUM */
typedef enum RDB_LOAD_RESULT {
    RDB_LOAD_NOP,       /* 存储引擎不处理，走主字典插入 */
    RDB_LOAD_SUCC,      /* 存储引擎已处理，跳过主字典插入 */
    RDB_LOAD_FAIL       /* 存储引擎错误，终止加载 */
} RDB_LOAD_RESULT;

/* RDB 加载虚函数表，对称于 RdbSaveCtxType */
typedef struct RdbLoadCtxType {
    int (*free_rdb_load_ctx)(RdbLoadCtx* ctx);                          /* 释放 load ctx 资源 */
    int (*load_db_init)(RdbLoadCtx* ctx, struct redisDb* db);           /* 切换 DB 时初始化 */
    int (*load_db_deinit)(RdbLoadCtx* ctx, struct redisDb* db);         /* 离开 DB 时清理 */
    RDB_LOAD_RESULT (*load_key_value)(RdbLoadCtx* ctx, struct redisDb* db,
                                       sds key, robj** val, long long expiretime,
                                       long long lfu_freq, long long lru_idle);  /* 加载单个 key-value */
} RdbLoadCtxType;

typedef struct RdbLoadCtx {
    void* ctx;                /* 引擎特定上下文（如 MemoryStorageRdbLoadCtx） */
    RdbLoadCtxType* type;     /* 虚函数表指针 */
} RdbLoadCtx;

/* 存储引擎接口 */
struct StorageEngine;
typedef struct StorageEngineType {
     int (*put)(void* context, struct RIO* rio);
    int (*get)(void* context, struct RIO* rio);
    int (*del)(void* context, struct RIO* rio);
    int (*iterate)(void* context, struct RIO* rio);
    int (*batch_get)(void* context, struct RIOBatch* batch);
    int (*batch_put)(void* context, struct RIOBatch* batch);
    int (*batch_del)(void* context, struct RIOBatch* batch);
    /* utils */
    int (*compact_range)(void* context, void* pd);
    void* (*get_stats)(void* context, void* pd);
    int (*flush)(void* context, void* pd);
    int (*create_checkpoint)(void* context, void* pd);
    int (*collect_cf_meta)(void* context, void* pd);
    /* fork */
    int (*set_forkctx_type)(struct StorageForkCtx* ctx,struct rdbSaveInfo* rsiptr, int mincapa);
    int (*set_rdb_save_ctx_type)(struct RdbSaveCtx* ctx,struct rio* rio, int rdbflags);
    int (*set_rdb_load_ctx_type)(struct RdbLoadCtx* ctx);

} StorageEngineType;
typedef struct StorageEngine {
    void* context;  /* 存储引擎上下文，可以包含连接信息等 */
    StorageEngineType* type;
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

/* 存储引擎 evict 状态 */
typedef struct StorageEvictCtx {
    size_t mem_used;
    size_t mem_tofree;
    long long keys_scanned;
    long long swap_trigged;
    int ended;
} StorageEvictCtx;
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
    /* evict*/
    struct swapEvictionCtx* swap_eviction_ctx;
    struct StorageEvictCtx evict_step_ctx;
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
    /* evict */
    struct client **swap_evict_clients; /* array of evict clients (one for each db). */
    struct client **swap_expire_clients; /* array of expire clients (one for each db), 用于对热key提交异步expire请求 */
    struct client **swap_scan_expire_clients; /* array of expire scan clients (one for each db). */
    struct client **swap_ttl_clients;    /* array of ttl clients (one for each db), 用于对冷key加载meta后决定是否过期 */ 
    /* ttl compact, only compact default CF */ 
    // int swap_ttl_compact_enabled;
    // struct swapTtlCompactCtx *swap_ttl_compact_ctx; 
    int swap_evict_inprogress_growth_rate;
    int swap_evict_inprogress_limit;
    int swap_evict_loop_check_interval; 
    /* swap 线程 */
    int swap_defer_thread_idx;
    int swap_util_thread_idx;
    int swap_total_threads_num;
    int swap_thread_auto_scale_up_cooling_down; /* the thread pool can only be expanded once within a period of time */ 
    int swap_threads_auto_scale_max;  /* upper limit of thread pool size  */ 
    int swap_threads_auto_scale_min;      /* lower limit of thread pool size*/ 
    int swap_threads_auto_scale_up_threshold; /* when the number of requests exceeds a certain threshold, a new thread is created */ 
    int swap_threads_auto_scale_down_idle_seconds; 
    /* replication */
    struct client* swap_draining_master;
    StorageForkCtx* forkctx;
    RdbSaveCtx* rdb_save_ctx;
    RdbLoadCtx* rdb_load_ctx;

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
    /* expire*/
    int swap_slow_expire_effort;
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


void ctripStorageCommand(struct client* c);
void ctripStorageEvictCommand(struct client* c);
void ctripStorageScanExpireCommand(struct client* c);
void ctripStorageExpiredCommand(struct client* c);
void ctripStorageWaitCommand(struct client* c);

/* swap datatype flags*/
#define CMD_SWAP_DATATYPE_KEYSPACE (1ULL<<40)
#define CMD_SWAP_DATATYPE_STRING (1ULL<<41)
#define CMD_SWAP_DATATYPE_HASH (1ULL<<42)
#define CMD_SWAP_DATATYPE_SET (1ULL<<43)
#define CMD_SWAP_DATATYPE_ZSET (1ULL<<44)
#define CMD_SWAP_DATATYPE_LIST (1ULL<<45)
#define CMD_SWAP_DATATYPE_BITMAP (1ULL<<46)


/* replication */
int storageDraningMasterSetDontReconecMasterFlags();
int connectWithMaster();
int storageConnectWithMaster();
int storageForkStart(struct rdbSaveInfo* rsiptr, int mincapa);
int storageForkEnd(struct rdbSaveInfo* rsiptr);
int storageForkRdbBefore(struct rdbSaveInfo *info);
int storageForkRdbAfterParent(struct rdbSaveInfo *info, int childpid);
int storageForkRdbAfterChild(struct rdbSaveInfo *info);
int storageRdbSaveDbBefore(struct rio* rdb, int rdbflags);
int storageRdbSaveDb(struct rio* rdb, int dbid);
int storageRdbSaveDbAfter(struct rio* rdb);
int storageRdbSaveDbInit(struct rio* rdb, struct redisDb* db);
int storageRdbSaveDbDeinit(struct rio* rdb, struct redisDb* db);
size_t storageDbSize(int dbid);
int storageRdbSaveDbColdKeys(struct rio* rdb,struct redisDb* db);
SAVE_RESULT_ENUM storageRdbSaveDbHotKey(struct rio* rdb,struct redisDb* db, robj* key, robj* val);


int swapThreadExtendAndInitThread();
int isClientStopNeeded(struct client *c);
int isStorageSPIEnabled();
void initDeferredCommand(struct client *c);
void resetDeferredCommand(struct client *c);
void initStorageDB(struct redisDb *db);
void StorageEvictSelectedKey(struct redisDb *db, robj *keyobj, long long *key_mem_freed);

int storageRdbLoadBefore(struct rio* rdb, struct redisDb* db);
int storageRdbLoadAuxField(struct rio* rdb, sds auxkey, sds auxval);
int storageRdbLoadError(struct rio* rdb);
int storageRdbLoadKVBegin(struct rio* rdb);
int storageRdbLoadAfter(struct rio* rdb, struct redisDb* db);
int storageRdbLoadNoKV(struct rio* rdb, struct redisDb* db, int type);

/* 存储引擎 RDB load 拦截：引擎直接处理则跳过主字典插入 */
RDB_LOAD_RESULT storageRdbLoadKeyVal(struct rio* rdb, struct redisDb* db,
    sds key, robj** val, long long expiretime, long long lfu_freq, long long lru_idle);
int storageRdbLoadDbSwitch(struct rio* rdb, struct redisDb* db);

int StorageEvictShouldStop();
void StorageEvictCtxEnd();
int StorageEvictCtxStart(size_t mem_used, size_t mem_tofree);

/* expireSlaveKeys 的 swap 模式扩展点（由 expireSlaveKeys 内部调用）：
 * 1. expireSlaveKeysPendingQueue    - Phase1 排空已确认过期冷key队列
 * 2. storageSlaveExpireGetKeyname   - 获取安全 keyname（swap=sdsdup，否则直接引用）
 * 3. storageSlaveExpireCheckColdKey - 冷key检测+提交 ttl_client，返回1表示已处理
 * 4. storageSlaveExpireUpdateBitmap - 更新 slaveKeysWithExpire bitmap */
void expireSlaveKeysPendingQueue(void);
int ctripStorageScanExpireDbCycle(struct redisDb* db, int type, long long timelimit);
void ctripStorageScanExpireCommand(struct client* c);
/* swap模式下active expire：若key有前序请求则跳过，否则提交异步expire请求
 * 输入: db - 数据库, keyobj - 需要过期的key对象（调用者持有引用）
 * 输出: 1=已提交expire请求, 0=跳过（有前序请求在进行中） */
int storageActiveExpireTryExpire(struct redisDb* db,robj* keyobj);
#endif /* __CTRIP_STORAGE_TYPES_H__ */
