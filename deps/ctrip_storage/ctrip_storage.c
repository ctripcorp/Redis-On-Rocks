#include "ctrip_storage.h"
#include "ctrip_storage_rate_limit.h"
#include "buffered_allocator.h"
#include "ctrip_storage_object_meta.h"
#include "server.h"
#include "memory_storage_engine.h"
#include "ctrip_storage_metric.h"
#include "ctrip_storage_filter.h"
#include "ctrip_storage_batch.h"
#include "ctrip_storage_thread.h"
#include "ctrip_storage_persist.h"
#include "ctrip_storage_evict.h"

/* 处理就绪的延迟命令（异步IO完成后回调） */
void processReadyDeferredCommands() {
    asyncCompleteQueueProcess(server.storage.swap_CQ);
}

void storageBeforeSleep() {
    swapBatchCtxFlush(server.storage.swap_batch_ctx, SWAP_BATCH_FLUSH_BEFORE_SLEEP);
    /* 目前仅有 processReadyDeferredCommands 需要在 sleep 前调用，后续如果有其他需要在 sleep 前调用的函数，也可以放在这里统一调用。 */
    processReadyDeferredCommands();
}

void storageSPICronLoop() {
    run_with_period(100) {
        
    }
}

int isStorageSPIEnabled() {
    return server.storage.type != STORAGE_TYPE_NONE;
}

int isClientStopNeeded(client *c) {
    return c->deferred_cmd && ((c->deferred_cmd->flags & CLIENT_DEFERRED_SWAPPING)) 
        || (c->deferred_cmd->flags & CLIENT_DEFERRED_REWINDING);
}


const char *swap_cf_names[CF_COUNT] = {data_cf_name, meta_cf_name, score_cf_name};



static inline const char *rocksActionName(int action) {
  const char *name = "?";
  const char *actions[] = {"NOP", "GET", "PUT", "DEL", "ITERATE"};
  if (action >= 0 && (size_t)action < sizeof(actions)/sizeof(char*))
    name = actions[action];
  return name;
}

static inline const char *swapDebugName(int type) {
    const char *name = "?";
    const char *names[] = {"LOCK_WAIT", "SWAP_QUEUE_WAIT", "NOTIFY_QUEUE_WAIT", "NOTIFT_QUEUE_HANDLES", "NOTIFT_QUEUE_HANDLE_TIME"};
    if (type >= 0 && (size_t)type < sizeof(names)/sizeof(char*))
        name = names[type];
    return name;
}

int swapThreadsInit() {
    int i;
    server.storage.swap_defer_thread_idx = 0;
    server.storage.swap_util_thread_idx = 1; 
    server.storage.swap_threads = zcalloc(sizeof(swapThread)*(swapThreadsMaxNum()));
    for (i = 0; i < swapThreadsCoreNum(); i++) {
        swapThreadExtendAndInitThread();
    }

    return 0;
}


void initStorage() {
    
    

    int i, metric_offset;
    server.storage.ror_stats = zmalloc(sizeof(rorStat));
    server.storage.ror_stats->swap_stats = zmalloc(SWAP_TYPES*sizeof(swapStat));
    for (i = 0; i < SWAP_TYPES; i++) {
        metric_offset = SWAP_SWAP_STATS_METRIC_OFFSET + i*SWAP_STAT_METRIC_SIZE;
        server.storage.ror_stats->swap_stats[i].name = swapIntentionName(i);
        server.storage.ror_stats->swap_stats[i].batch = 0;
        server.storage.ror_stats->swap_stats[i].count = 0;
        server.storage.ror_stats->swap_stats[i].memory = 0;
        server.storage.ror_stats->swap_stats[i].time = 0;
        server.storage.ror_stats->swap_stats[i].stats_metric_idx_batch = metric_offset+SWAP_STAT_METRIC_BATCH;
        server.storage.ror_stats->swap_stats[i].stats_metric_idx_count = metric_offset+SWAP_STAT_METRIC_COUNT;
        server.storage.ror_stats->swap_stats[i].stats_metric_idx_memory = metric_offset+SWAP_STAT_METRIC_MEMORY;
        server.storage.ror_stats->swap_stats[i].stats_metric_idx_time = metric_offset+SWAP_STAT_METRIC_TIME;
    }
    server.storage.ror_stats->rio_stats = zmalloc(ROCKS_TYPES*sizeof(swapStat));
    for (i = 0; i < ROCKS_TYPES; i++) {
        metric_offset = SWAP_RIO_STATS_METRIC_OFFSET + i*SWAP_STAT_METRIC_SIZE;
        server.storage.ror_stats->rio_stats[i].name = rocksActionName(i);
        server.storage.ror_stats->rio_stats[i].batch = 0;
        server.storage.ror_stats->rio_stats[i].count = 0;
        server.storage.ror_stats->rio_stats[i].memory = 0;
        server.storage.ror_stats->rio_stats[i].time = 0;
        server.storage.ror_stats->rio_stats[i].stats_metric_idx_batch = metric_offset+SWAP_STAT_METRIC_BATCH;
        server.storage.ror_stats->rio_stats[i].stats_metric_idx_count = metric_offset+SWAP_STAT_METRIC_COUNT;
        server.storage.ror_stats->rio_stats[i].stats_metric_idx_memory = metric_offset+SWAP_STAT_METRIC_MEMORY;
        server.storage.ror_stats->rio_stats[i].stats_metric_idx_time = metric_offset+SWAP_STAT_METRIC_TIME;
    }
    server.storage.ror_stats->compaction_filter_stats = zmalloc(sizeof(compactionFilterStat) * CF_COUNT);
    for (i = 0; i < CF_COUNT; i++) {
        metric_offset = SWAP_COMPACTION_FILTER_STATS_METRIC_OFFSET + i * COMPACTION_FILTER_METRIC_SIZE;
        server.storage.ror_stats->compaction_filter_stats[i].name = swap_cf_names[i];
        server.storage.ror_stats->compaction_filter_stats[i].filt_count = 0;
        server.storage.ror_stats->compaction_filter_stats[i].scan_count = 0;
        server.storage.ror_stats->compaction_filter_stats[i].rio_count = 0;
        server.storage.ror_stats->compaction_filter_stats[i].stats_metric_idx_filt = metric_offset+COMPACTION_FILTER_METRIC_FILT;
        server.storage.ror_stats->compaction_filter_stats[i].stats_metric_idx_scan = metric_offset+COMPACTION_FILTER_METRIC_SCAN;
        server.storage.ror_stats->compaction_filter_stats[i].stats_metric_idx_rio = metric_offset+COMPACTION_FILTER_METRIC_RIO;
    }
    server.storage.swap_debug_info = zmalloc(SWAP_DEBUG_INFO_TYPE*sizeof(swapDebugInfo));
    for (i = 0; i < SWAP_DEBUG_INFO_TYPE; i++) {
        metric_offset = SWAP_DEBUG_STATS_METRIC_OFFSET + i*SWAP_DEBUG_SIZE;
        server.storage.swap_debug_info[i].name = swapDebugName(i);
        server.storage.swap_debug_info[i].count = 0;
        server.storage.swap_debug_info[i].value = 0;
        server.storage.swap_debug_info[i].metric_idx_count = metric_offset+SWAP_DEBUG_COUNT;
        server.storage.swap_debug_info[i].metric_idx_value = metric_offset+SWAP_DEBUG_VALUE;
    }
    server.storage.swap_hit_stats = zcalloc(sizeof(swapHitStat));
    server.storage.swap_batch_ctx = swapBatchCtxNew();
    server.storage.swap_evict_clients = zmalloc(sizeof(client*)*server.dbnum);
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->cmd = lookupCommandByCString("CTRIP_STORAGE_EVICT");
        c->db = server.db+i;
        c->deferred_cmd->swap_lock_mode = SWAP_LOCK_SHARED;
        server.storage.swap_evict_clients[i] = c;
    }

    server.storage.swap_eviction_ctx = swapEvictionCtxCreate();

    if (server.storage.swap_persist_enabled)
        server.storage.swap_persist_ctx = swapPersistCtxNew();
    else
        server.storage.swap_persist_ctx = NULL;

    // server.storage.swap_load_clients = zmalloc(server.dbnum*sizeof(client*));
    // for (i = 0; i < server.dbnum; i++) {
    //     client *c = createClient(NULL);
    //     c->cmd = lookupCommandByCString("SWAP.LOAD");
    //     c->db = server.db+i;
    //     c->deferred_cmd->swap_lock_mode = SWAP_LOCK_SHARED;
    //     server.storage.swap_load_clients[i] = c;
    // }

    /* 初始化热key expire client 数组（每个db一个），用于对已在内存中的key提交异步expire请求 */
    server.storage.swap_expire_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->cmd = lookupCommandByCString("CTRIP_STORAGE_EXPIRED");
        c->deferred_cmd->swap_lock_mode = SWAP_LOCK_SHARED;
        server.storage.swap_expire_clients[i] = c;
    }

    server.storage.swap_scan_expire_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->cmd = lookupCommandByCString("CTRIP_STORAGE_SCANEXPIRE");
        c->deferred_cmd->swap_lock_mode = SWAP_LOCK_SHARED;
        server.storage.swap_scan_expire_clients[i] = c;
    }

    /* 初始化冷key ttl client 数组（每个db一个），用于加载冷key meta，从而判断是否需要过期 */
    // server.storage.swap_ttl_clients = zmalloc(server.dbnum*sizeof(client*));
    // for (i = 0; i < server.dbnum; i++) {
    //     client *c = createClient(NULL);
    //     c->db = server.db+i;
    //     c->cmd = lookupCommandByCString("ttl");
    //     c->deferred_cmd->swap_lock_mode = SWAP_LOCK_SHARED;
    //     server.storage.swap_ttl_clients[i] = c;
    // }

    swapThreadsInit();

    if (server.storage.type == STORAGE_TYPE_NONE) {
        server.storage.engine = NULL;
        return;
    } else if (server.storage.type == STORAGE_TYPE_MEMORY) {
        // 初始化内存存储引擎
        server.storage.engine = createMemoryStorageEngine();
    } else if (server.storage.type == STORAGE_TYPE_ROCKSDB) {
        // 初始化RocksDB存储引擎
        server.storage.engine = createRocksDBStorageEngine();
    }
    initSwapRequest();
    swapLockCreate();
    //注册ae事件处理异步回调
    asyncCompleteQueueInit();
}


/* 在调用命令之前进行存储引擎相关的处理 */
int serverStorageBeforeProcessCommand(client *c) {
    int keyrequests_submit;
    swapRatelimitCtx _rlctx = {0}, *rlctx = &_rlctx;

    swapRatelimitStart(rlctx,c);

    if (swapRateLimitReject(rlctx,c)) return C_OK;

    if (!(c->flags & CLIENT_MASTER)) {
        keyrequests_submit = submitNormalClientRequests(c);
    } else {
        keyrequests_submit = submitReplClientRequests(c);
    }

    swapRateLimitPause(rlctx,c);

    if (c->deferred_cmd->flags & CLIENT_DEFERRED_REWINDING) {
        /* Rewinding command parsed but not processed, See below */
        return C_ERR;
    } else if (keyrequests_submit > 0) {
        /* Swapping command parsed but not processed, return C_ERR so that:
         * 1. repl stream will not propagate to sub-slaves
         * 2. client will not reset
         * 3. client will break out process loop. */
        if (c->deferred_cmd-> keyrequests_count) c->deferred_cmd->flags |= CLIENT_DEFERRED_SWAPPING;
        return C_ERR;
    } else if (keyrequests_submit < 0) {
        /* Swapping command parsed and dispatched, return C_OK so that:
         * 1. repl client will skip call
         * 2. repl client will reset (cmd moved to worker).
         * 3. repl client will continue parse and dispatch cmd */
        return C_OK; //C_CONTINUE
    } else {
        return STORAGE_ACTION_CONTINUE;
    }
        
}

int serverStorageAfterProcessCommand(client *c) {
    /*THINKING?*/
    // c->woff = server.master_repl_offset;
    return C_OK;
}

void dictObjectMetaFree(struct dict *privdata, void *val) {
    UNUSED(privdata);
    freeObjectMeta(val);
}

dictType objectMetaDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    dictObjectMetaFree,         /* val destructor */
    dictResizeAllowed           /* allow to expand */
};

dictType dbDirtySubkeysDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    dictObjectDestructor,       /* val destructor */
    dictResizeAllowed,          /* allow to expand */
};

void initStorageDB(struct redisDb *db) {
    if (!isStorageSPIEnabled()) return;
    db->storage.meta = dictCreate(&objectMetaDictType);
    db->storage.dirty_subkeys = dictCreate(&dbDirtySubkeysDictType);
    serverLog(LL_NOTICE,"Storage: init db %d meta and dirty subkeys dict", db->id);
    db->storage.evict_asap = listCreate();
    db->storage.cold_keys = 0;
    db->storage.randomkey_nextseek = NULL;
    db->storage.scan_expire = scanExpireCreate();
    db->storage.cold_filter = coldFilterCreate();


}

/* 释放单个 db 的存储资源 */
static void resetStorageDB(struct redisDb *db) {
    if (db->storage.meta) {
        dictRelease(db->storage.meta);
        db->storage.meta = NULL;
    }
    if (db->storage.dirty_subkeys) {
        dictRelease(db->storage.dirty_subkeys);
        db->storage.dirty_subkeys = NULL;
    }
    if (db->storage.evict_asap) {
        listRelease(db->storage.evict_asap);
        db->storage.evict_asap = NULL;
    }
    if (db->storage.scan_expire) {
        /* scanExpireFree 未实现，暂时跳过 */
        db->storage.scan_expire = NULL;
    }
    if (db->storage.cold_filter) {
        /* coldFilterFree 未实现，暂时跳过 */
        db->storage.cold_filter = NULL;
    }
    db->storage.cold_keys = 0;
    db->storage.randomkey_nextseek = NULL;
}

/* 释放所有存储引擎资源 */
void resetStorage() {
    int i;

    serverLog(LL_WARNING, "resetStorage: starting cleanup, type=%d", server.storage.type);

    if (!isStorageSPIEnabled()) return;

    /* 释放每个 db 的存储资源 */
    for (i = 0; i < server.dbnum; i++) {
        resetStorageDB(&server.db[i]);
    }

    /* 释放统计信息 */
    if (server.storage.ror_stats) {
        if (server.storage.ror_stats->swap_stats) {
            zfree(server.storage.ror_stats->swap_stats);
        }
        if (server.storage.ror_stats->rio_stats) {
            zfree(server.storage.ror_stats->rio_stats);
        }
        if (server.storage.ror_stats->compaction_filter_stats) {
            zfree(server.storage.ror_stats->compaction_filter_stats);
        }
        zfree(server.storage.ror_stats);
        server.storage.ror_stats = NULL;
    }

    /* 释放调试信息 */
    if (server.storage.swap_debug_info) {
        zfree(server.storage.swap_debug_info);
        server.storage.swap_debug_info = NULL;
    }

    /* 释放命中统计 */
    if (server.storage.swap_hit_stats) {
        zfree(server.storage.swap_hit_stats);
        server.storage.swap_hit_stats = NULL;
    }

    /* 释放批处理上下文 - swapBatchCtxFree 未实现，暂时跳过 */
    server.storage.swap_batch_ctx = NULL;

    /* 释放 evict clients */
    if (server.storage.swap_evict_clients) {
        for (i = 0; i < server.dbnum; i++) {
            if (server.storage.swap_evict_clients[i]) {
                freeClient(server.storage.swap_evict_clients[i]);
            }
        }
        zfree(server.storage.swap_evict_clients);
        server.storage.swap_evict_clients = NULL;
    }

    /* 释放 eviction context - swapEvictionCtxFree 未实现，暂时跳过 */
    server.storage.swap_eviction_ctx = NULL;

    /* 释放 persist context - swapPersistCtxFree 未实现，暂时跳过 */
    server.storage.swap_persist_ctx = NULL;

    /* 释放 expire clients */
    if (server.storage.swap_expire_clients) {
        for (i = 0; i < server.dbnum; i++) {
            if (server.storage.swap_expire_clients[i]) {
                freeClient(server.storage.swap_expire_clients[i]);
            }
        }
        zfree(server.storage.swap_expire_clients);
        server.storage.swap_expire_clients = NULL;
    }

    /* 释放 scan expire clients */
    if (server.storage.swap_scan_expire_clients) {
        for (i = 0; i < server.dbnum; i++) {
            if (server.storage.swap_scan_expire_clients[i]) {
                freeClient(server.storage.swap_scan_expire_clients[i]);
            }
        }
        zfree(server.storage.swap_scan_expire_clients);
        server.storage.swap_scan_expire_clients = NULL;
    }

    /* 释放线程资源 - swapThreadsFree 未实现，暂时跳过 */
    if (server.storage.swap_threads) {
        zfree(server.storage.swap_threads);
        server.storage.swap_threads = NULL;
    }

    /* 释放存储引擎 */
    if (server.storage.engine) {
        if (server.storage.type == STORAGE_TYPE_MEMORY) {
            freeMemoryStorageEngine(server.storage.engine);
        }
        /* RocksDB 引擎的释放待实现 */
        server.storage.engine = NULL;
    }

    /* 释放请求相关的缓冲分配器 */
    deinitSwapRequest();
}


size_t storageDbSize(int dbid) {
    redisDb* db = server.db + dbid;
    return db->storage.cold_keys;
}

#ifdef REDIS_TEST

int ctripStorageTest(int argc, char **argv, int accurate) {
    int result = 0;
    /* swapDataTest 依赖 initTestRedisServer（需完整 server 初始化），暂不运行 */
    result += memoryStorageEngineTest(argc, argv, accurate);
    return result;
}
#endif