#include "ctrip_storage.h"
#include "ctrip_storage_evict.h"
#include "ctrip_storage_lock.h"
#include "ctrip_storage_request.h"
#include "object.h"
/* ----------------------------- evict asap ------------------------------ */
#define EVICT_ASAP_KEYS_LIMIT 256


int tryEvictKey(redisDb *db, robj *key, int *evict_result) {
    int dirty, old_keyrequests_count;
    robj *o;
    serverLog(LL_WARNING, "tryEvictKey step1 %d", db->id);
    client *evict_client = server.storage.swap_evict_clients[db->id];
    if (lockWouldBlock(server.storage.swap_txid++, db, key)) {
        if (evict_result) *evict_result = EVICT_FAIL_SWAPPING;
        return 0;
    }
    serverLog(LL_WARNING, "tryEvictKey step2");
    if ((o = lookupKeyReadWithFlags(db, key, LOOKUP_NOTOUCH)) == NULL) {
        if (evict_result) *evict_result = EVICT_FAIL_ABSENT;
        return 0;
    }
    serverLog(LL_WARNING, "tryEvictKey step3 %p", evict_client);
    dirty = objectIsDirty(o);
    old_keyrequests_count = evict_client->deferred_cmd->keyrequests_count;
    serverLog(LL_WARNING, "tryEvictKey step4");
    submitEvictClientRequest(evict_client,key,0,SWAP_PERSIST_VERSION_NO);
    
    /* Evit request finished right away, no swap triggered. */
    if (evict_client->deferred_cmd->keyrequests_count == old_keyrequests_count) {
        if (dirty) {
            if (evict_result) *evict_result = EVICT_FAIL_UNSUPPORTED;
        } else {
            if (evict_result) *evict_result = EVICT_SUCC_FREED;
        }
        return 0;
    } else {
        if (evict_result) *evict_result = EVICT_SUCC_SWAPPED;
        return 1;
    }
    serverLog(LL_WARNING, "tryEvictKey step5");
}

inline int swapEvictionReachedInprogressLimit() {
    return server.storage.swap_eviction_ctx->inprogress_count + server.storage.swap_eviction_ctx->freed_inrow >=
        server.storage.swap_eviction_ctx->inprogress_limit;
}

int swapEvictAsap() {
    static mstime_t stat_mstime;
    static long stat_evict, stat_scan, stat_loop;

    int evicted = 0, scanned = 0, result = EVICT_ASAP_OK;

    for (int i = 0; i < server.dbnum; i++) {
        listIter li;
        listNode *ln;
        redisDb *db = server.db+i;

        if (listLength(db->storage.evict_asap) == 0) continue;

        listRewind(db->storage.evict_asap, &li);
        while ((ln = listNext(&li))) {
            int evict_result;
            robj *key = listNodeValue(ln);

            if (swapEvictionReachedInprogressLimit() ||
                    scanned >= EVICT_ASAP_KEYS_LIMIT) {
                result = EVICT_ASAP_AGAIN;
                goto end;
            }

            tryEvictKey(db, key, &evict_result);

            scanned++;
            if (evict_result == EVICT_FAIL_SWAPPING) {
                /* Try evict again if key is holded or swapping */
                listAddNodeHead(db->storage.evict_asap, key);
            } else {
                decrRefCount(key);
                evicted++;
            }
            listDelNode(db->storage.evict_asap, ln);
        }
    }

end:
    stat_loop++;
    stat_evict += evicted;
    stat_scan += scanned;

    if (server.mstime - stat_mstime > 1000) {
        if (stat_scan > 0) {
            serverLog(LL_VERBOSE, "SwapEvictAsap loop=%ld,scaned=%ld,swapped=%ld",
                    stat_loop, stat_scan, stat_evict);
        }
        stat_mstime = server.mstime;
        stat_loop = 0, stat_evict = 0, stat_scan = 0;
    }

    return result;
}

inline int swapEvictGetInprogressLimit(size_t mem_tofree) {
    /* Base inprogress limit is threads num(deffer thread), increase one every n MB */
    int inprogress_limit = 1 + mem_tofree/(server.storage.swap_evict_inprogress_growth_rate);

    if (inprogress_limit > server.storage.swap_evict_inprogress_limit)
        inprogress_limit = server.storage.swap_evict_inprogress_limit;
    return inprogress_limit;
}
int StorageEvictCtxStart(size_t mem_used, size_t mem_tofree) {
    /* Evict keys registered to be evicted ASAP even if not over maxmemory,
     * because evict asap could reduce cow. */
    server.storage.evict_step_ctx.mem_used = mem_used;
    server.storage.evict_step_ctx.mem_tofree = mem_tofree;
    server.storage.evict_step_ctx.keys_scanned = 0;
    server.storage.evict_step_ctx.swap_trigged = 0;
    server.storage.evict_step_ctx.ended = 0;
    server.storage.swap_eviction_ctx->inprogress_limit = swapEvictGetInprogressLimit(mem_tofree);
    if (swapEvictAsap() == EVICT_ASAP_AGAIN) {
        startEvictionTimeProc();
    }
    return 1;
}
int StorageEvictShouldStop() {
    if (swapEvictionReachedInprogressLimit()) { // 队列积压
        startEvictionTimeProc();
        return 1;
    } else {
        if (server.storage.swap_eviction_ctx->failed_inrow > 16) return 1; // 连续失败

        return 0;
    }
}
#define OBJECT_ESTIMATE_SIZE_SAMPLE 5
#define DEFAULT_STRING_SIZE 512
#define DEFAULT_HASH_FIELD_COUNT 8
#define DEFAULT_HASH_FIELD_SIZE 256
#define DEFAULT_SET_MEMBER_COUNT 8
#define DEFAULT_SET_MEMBER_SIZE 128
#define DEFAULT_LIST_ELE_COUNT 32
#define DEFAULT_LIST_ELE_SIZE 128
#define DEFAULT_ZSET_MEMBER_COUNT 16
#define DEFAULT_ZSET_MEMBER_SIZE 128
#define DEFAULT_KEY_SIZE 48
size_t objectEstimateSize(robj *o, int dbid) {
    size_t asize = 0;

    switch (o->type) {
    case OBJ_STRING:
        asize = kvobjAllocSize(o);
        break;
    case OBJ_HASH:
        /* Hash may convert encoding in swap thread, so we can't safely
         * estimate hash size by encoding. */
        asize = DEFAULT_HASH_FIELD_COUNT*DEFAULT_HASH_FIELD_SIZE;
        break;
    case OBJ_SET:
        /* similar to Hash */
        asize = DEFAULT_SET_MEMBER_COUNT*DEFAULT_SET_MEMBER_SIZE;
        break;
    case OBJ_LIST:
        serverAssert(o->encoding == OBJ_ENCODING_QUICKLIST || o->encoding == OBJ_ENCODING_LISTPACK);
        asize = listTypeLength(o)*DEFAULT_LIST_ELE_SIZE;
        break;
    case OBJ_ZSET:
        asize = DEFAULT_ZSET_MEMBER_COUNT*DEFAULT_ZSET_MEMBER_SIZE;
        break;
    case OBJ_STREAM:
        asize = kvobjAllocSize(o);
        break;
    case OBJ_MODULE:
        /*TODO support module*/
        asize = kvobjComputeSize(NULL, o,OBJECT_ESTIMATE_SIZE_SAMPLE, dbid);
        break;
    }

    return asize;
}


size_t keyEstimateSize(redisDb *db, robj *key) {
    robj *val = lookupKeyReadWithFlags(db, key, LOOKUP_NOTOUCH);
    return val ? objectEstimateSize(val, db->id): 0;
}
#define swapEvictionFreedInrowIncr(ctx) do { ctx->freed_inrow++; } while(0)
#define swapEvictionFreedInrowReset(ctx) do { ctx->freed_inrow=0; } while(0)


static inline void swapEvictionCtxUpdateStat(swapEvictionCtx *ctx, int evict_result) {
    ctx->stat.evict_result[evict_result]++;
}
void StorageEvictSelectedKey(redisDb *db, robj *keyobj, long long *key_mem_freed) {
    int evict_result;
    size_t mem_freed;
    mstime_t eviction_latency;
    swapEvictionCtx *ctx = server.storage.swap_eviction_ctx;

    latencyStartMonitor(eviction_latency);

    /* Key might be directly freed if not dirty, so we need to compute key
     * size beforehand. */
    mem_freed = keyEstimateSize(db, keyobj);
    server.storage.evict_step_ctx.swap_trigged += tryEvictKey(db, keyobj, &evict_result);

    if (evictResultIsFreed(evict_result))
        swapEvictionFreedInrowIncr(ctx);

    if (evictResultIsSucc(evict_result)) {
        ctx->failed_inrow = 0;
        notifyKeyspaceEvent(NOTIFY_EVICTED, "swap-evicted", keyobj, db->id);
    } else {
        ctx->failed_inrow++;
    }
    swapEvictionCtxUpdateStat(ctx,evict_result);

    latencyEndMonitor(eviction_latency);
    latencyAddSampleIfNeeded("swap-eviction",eviction_latency);

    server.storage.evict_step_ctx.keys_scanned++;
    *key_mem_freed += mem_freed;
}

void StorageEvictCtxEnd() {
    static long long nscaned, nloop, nswap;
    static mstime_t prev_logtime;

    if (server.storage.evict_step_ctx.ended) return;

    nloop++;
    nscaned += server.storage.evict_step_ctx.keys_scanned;
    nswap += server.storage.evict_step_ctx.swap_trigged;

    if (server.mstime - prev_logtime > 1000) {
        serverLog(LL_VERBOSE,
                "Eviction loop=%lld,scaned=%lld,swapped=%lld,mem_used=%ld,mem_inprogress=%ld",
                nloop, nscaned, nswap, server.storage.evict_step_ctx.mem_used, server.storage.swap_inprogress_memory);
        prev_logtime = server.mstime;
        nscaned = 0, nloop = 0, nswap = 0;
    }

    server.storage.evict_step_ctx.ended = 1;
}


void ctripStorageEvictCommand(client* c) {
    if (!isStorageSPIEnabled()) {
        addReplyError(c, "Storage SPI is not enabled");
        return;
    }
    /*THINKING?*/
     int i, nevict = 0;

    for (i = 2; i < c->argc; i++) {
        nevict += tryEvictKey(c->db,c->argv[i],NULL);
    }

    addReplyLongLong(c, nevict);
}

swapEvictionCtx *swapEvictionCtxCreate() {
    swapEvictionCtx *ctx = zcalloc(sizeof(swapEvictionCtx));
    ctx->inprogress_count = 0;
    ctx->inprogress_limit = 0;
    ctx->failed_inrow = 0;
    ctx->freed_inrow = 0;
    return ctx;
}

void ctripStorageScanExpireCommand(client *c) {
    addReply(c, shared.ok);
}

void ctripStorageExpiredCommand(struct client* c) {
    addReply(c, shared.ok);
}

/* CTRIP_STORAGE WAIT - Wait for all async swap operations to complete */
void ctripStorageWaitCommand(client *c) {
    if (!isStorageSPIEnabled()) {
        addReplyError(c, "Storage SPI is not enabled");
        return;
    }

    /* Wait for pending async operations with a timeout */
    mstime_t timeout = 5000; /* 5 seconds timeout */
    mstime_t start = mstime();

    while (mstime() - start < timeout) {
        /* Check if there are pending async operations */
        size_t inprogress_batch, inprogress_count;
        atomicGet(server.storage.swap_inprogress_batch, inprogress_batch);
        atomicGet(server.storage.swap_inprogress_count, inprogress_count);

        if (inprogress_batch == 0 && inprogress_count == 0 &&
            server.storage.swap_eviction_ctx->inprogress_count == 0) {
            break;
        }

        /* Process async completions via beforeSleep */
        processReadyDeferredCommands();

        /* Small delay to avoid busy loop */
        usleep(1000);
    }

    addReply(c, shared.ok);
}