#include "ctrip_storage.h"
#include "ctrip_storage_rate_limit.h"
#include "buffered_allocator.h"
#include "ctrip_storage_object_meta.h"

/* 处理就绪的延迟命令（异步IO完成后回调） */
void processReadyDeferredCommands() {
    asyncCompleteQueueProcess(server.storage.swap_CQ);
}

int isStorageSPIEnabled() {
    return server.storage.engine != NULL;
}



void initStorage() {
    if (server.storage.type == STORAGE_TYPE_NONE) {
        server.storage.engine = NULL;
        return;
    } else if (server.storage.type == STORAGE_TYPE_MEMORY) {
        // 初始化内存存储引擎
        server.storage.engine = initMemoryStorageEngine();
    } else if (server.storage.type == STORAGE_TYPE_ROCKSDB) {
        // 初始化RocksDB存储引擎
        server.storage.engine = initRocksDBStorageEngine();
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

void dictObjectMetaFree(void *privdata, void *val) {
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
void initStorageDB(redisDb  *db) {
    if (!isStorageSPIEnabled()) return;
    db->storage.meta = dictCreate(&objectMetaDictType);
    db->storage.dirty_subkeys = dictCreate(&dbDirtySubkeysDictType);
    db->storage.evict_asap = listCreate();
    db->storage.cold_keys = 0;
    db->storage.randomkey_nextseek = NULL;
    db->storage.scan_expire = scanExpireCreate();
    db->storage.cold_filter = NULL;
}

#ifdef REDIS_TEST

int ctripStorageTest(int argc, char **argv, int accurate) {
    int result = 0;
    result += swapDataTest(argc, argv, accurate);
    return result;
}
#endif