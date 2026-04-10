#include "ctrip_storage_request.h"
#include "ctrip_storage_batch.h"
#include "server.h"
#include "ctrip_storage_debug.h"
#include "ctrip_storage.h"
#include "buffered_allocator.h"
#include "versions/functions.h"
#include "ctrip_storage_commands.h"
#include "ctrip_storage_filter.h"
#include "ctrip_storage_metric.h"

/* --- cmd intention flags --- */


#define NOSWAP_REASON_KEYNOTEXISTS 1
#define NOSWAP_REASON_NOTKEYLEVEL 2
#define NOSWAP_REASON_KEYNOTSUPPORT 3
#define NOSWAP_REASON_SWAPANADECIDED 4
#define NOSWAP_REASON_FILT_BY_CUCKOOFILTER 5
#define NOSWAP_REASON_FILT_BY_ABSENTCACHE 6
#define NOSWAP_REASON_ALREAY_SWAPPED_OUT 7
#define NOSWAP_REASON_UNEXPECTED 100



/* cold keys filter */




/* 前向声明 */
void swapCtxFree(swapCtx *ctx);
void clientReleaseRequestIO(client *c, swapCtx *ctx) {
    UNUSED(c);
    lockProceeded(ctx->swap_lock);
}


int dbDeleteMeta(redisDb *db, robj *key) {
    if (dictSize(db->storage.meta) == 0) return 0;
    return dictDelete(db->storage.meta,key->ptr) == DICT_OK ? 1 : 0;
}
int swapDataKeyRequestFinished(swapData *data) {
    if (data->propagate_expire) {
        deleteExpiredKeyAndPropagate(data->db,data->key);
        postExecutionUnitOperations();
    }

    if (data->set_dirty) {
        dbSetDirty(data->db,data->key);
    }

    if (data->set_dirty_meta) {
        dbSetMetaDirty(data->db,data->key);
    }

    if (data->persistence_deleted) {
        if (data->swap_type != SWAP_TYPE_BITMAP) {
            dbDeleteMeta(data->db, data->key);
        } else {
            /* bitmap need to keep marker. */
            objectMeta *bitmap_meta = lookupMeta(data->db,data->key);
            serverAssert(bitmap_meta != NULL);
            bitmapMetaTransToMarkerIfNeeded(bitmap_meta);
        }
    }

    if (data->set_persist_keep && !getObjectPersistKeep(data->value)) {
        setObjectPersistKeep(data->value);
    }
    return 0;
}
void keyRequestSwapFinished(swapData *data, void *pd, int errcode) {
    UNUSED(data);
    swapCtx *ctx = pd;
	if (errcode) ctx->errcode = errcode;

    if (data) {
        swapDataKeyRequestFinished(data);
        DEBUG_MSGS_APPEND(&ctx->msgs,"swap-finished",
                "key=%s,propagate_expire=%d,set_dirty=%d",
                (sds)data->key->ptr,data->propagate_expire,data->set_dirty);
    }

    /* release io will trigger either another swap within the same tx or
     * command call, but never both. so swap and main thread will not
     * touch the same key in parallel. */
    clientReleaseRequestIO(ctx->c,ctx);

    ctx->finished(ctx->c,ctx);
}

swapRequest *swapRequestNew(keyRequest *key_request, int intention,
        uint32_t intention_flags, swapCtx *ctx, swapData *data,
        void *datactx,swapTrace *trace,
        swapRequestFinishedCallback cb, void *pd, void *msgs) {
    swapRequest *req = zcalloc(sizeof(swapRequest));
    UNUSED(msgs);
    req->key_request = key_request;
    req->intention = intention;
    req->intention_flags = intention_flags;
    req->swap_ctx = ctx;
    req->data = data;
    req->datactx = datactx;
    req->result = NULL;
    req->finish_cb = cb;
    req->finish_pd = pd;
    req->swap_memory = 0;
#ifdef SWAP_DEBUG
    req->msgs = msgs;
#endif
    req->errcode = 0;
    req->trace = trace;
    return req;
}


void swapRequestFree(swapRequest *req) {
    zfree(req);
}
static inline swapRequest *swapDataRequestNew(
    int intention, uint32_t intention_flags, swapCtx *ctx, swapData *data,
    void *datactx, swapTrace *trace, swapRequestFinishedCallback cb, void *pd,
    void *msgs) {
  return swapRequestNew(NULL,intention,intention_flags,ctx,data,datactx,trace,cb,pd,msgs);
}

int swapDataMarkedPropagateExpire(swapData *data) {
    return data->propagate_expire;
}


int getObjectPersistent(robj *o) {
    if (o == NULL) return 0;
    if (o->type == OBJ_STRING) {
        return 1; /* string type is always persistent, even when value is NULL. */
    } else {
        return o->storage.persistent;
    }
}

static bool isCmdMatchedDataType(swapData *d, struct keyRequest *key_request) {
    if (key_request->cmd_flags & CMD_SWAP_DATATYPE_KEYSPACE) {
        return true;
    }

    int cmd_string_compatible =
        (key_request->cmd_flags & CMD_SWAP_DATATYPE_STRING) ||
        (key_request->cmd_flags & CMD_SWAP_DATATYPE_BITMAP);
    int data_string_compatible =
        (d->type->cmd_swap_flags & CMD_SWAP_DATATYPE_STRING) ||
        (d->type->cmd_swap_flags & CMD_SWAP_DATATYPE_BITMAP);

    if (key_request->cmd_flags & d->type->cmd_swap_flags)  {
        /* cmd type equals swap type: proceed */
    } else if (cmd_string_compatible && data_string_compatible) {
        /* cmd type and swap type both string compatible, 
         * data type will be changed, so we must delete it in rocksdb. */
        key_request->cmd_intention_flags = SWAP_IN_DEL;
    } else {
        return false;
    }
    return true;
}

/* Main/swap-thread: analyze data and command intention & request to decide
 * final swap intention. e.g. command might want SWAP_IN but data not
 * evicted, then intention is decided as NOP. */
int swapDataAna(swapData *d, int thd, struct keyRequest *key_request,
        int *intention, uint32_t *intention_flags, void *datactx) {
    int retval = 0;

    serverAssert(swapDataAlreadySetup(d));

    if (swapDataMarkedPropagateExpire(d)) {
        key_request->cmd_intention = SWAP_DEL;
        key_request->cmd_intention_flags = 0;
    }

    if (key_request->cmd_intention == SWAP_DEL && d->value && !getObjectPersistent(d->value)) {
        // no persistent data, skip del
        key_request->cmd_intention = SWAP_NOP;
        key_request->cmd_intention_flags = 0;
    }

    if (d->type->swapAna) {
        /* cmd of SWAP_IN is strongly related to data type, 
         * and the original data type may be changed after current operation,
         * so we need to check if data type is matched with cmd.
         * SWAP_OUT, SWAP_DEL will never change original data type. */
        if (key_request->cmd_intention == SWAP_IN && (!isCmdMatchedDataType(d, key_request))) {
            return SWAP_ERR_DATA_WRONG_TYPE_ERROR;
        }

        retval = d->type->swapAna(d,thd,key_request,intention,
                intention_flags,datactx);

        if ((*intention_flags & SWAP_EXEC_IN_DEL) &&
                key_request->type == KEYREQUEST_TYPE_SUBKEY &&
                key_request->b.num_subkeys > 0 &&
                server.storage.swap_dirty_subkeys_enabled) {
            /* commands with EXEC_IN_DEL flags for subkeys:
             *  - HDEL/SREM: meta will be set dirty when swap finished and
             *    persisted later, but subkeys in rocksdb and memory will
             *    not diverge.
             *  - ZADD/ZINCRBY/GEOADD...: meta will be set dirty when swap
             *    finished, subkeys will be flag dirty when call() and then
             *    persisted later.
             * set meta dirty is sufficient for both kind.
             */
            d->set_dirty_meta = 1;
        } else if ((*intention_flags & SWAP_FIN_DEL_SKIP) ||
                (*intention_flags & SWAP_EXEC_IN_DEL)) {
            /* rocksdb and memory will diverge when swap finish. */
            d->set_dirty = 1;
        }

        if (key_request->cmd_intention == SWAP_IN && !swapDataIsCold(d))
            d->set_persist_keep = 1;
    }

    return retval;
}

static inline void swapDataSetObjectMeta(swapData *d, objectMeta *object_meta) {
    d->object_meta = object_meta;
}

// int isImportingExpireDisabled() {
//     return (((server.storage.importing_end_time > server.mstime) ||
//             (listLength(server.storage.importing_evict_queue) != 0)) &&
//             (server.storage.importing_expire_enabled == 0));
// }
/* See keyIsExpired for more details */
int timestampIsExpired(mstime_t when) {
    mstime_t now;

    if (when < 0) return 0;
    if (server.loading) return 0;
    now = commandTimeSnapshot();
    return now > when;
}

static int swapDataExpiredAndShouldDelete(swapData *data) {
    //TODO importing
    // if (isImportingExpireDisabled()) return 0;

    if (!timestampIsExpired(data->expire)) return 0;
    if (server.masterhost != NULL) return 0;
    if (!!(isPausedActionsWithUpdate(PAUSE_ACTION_REPLICA))) return 0;
    return 1;
}

void swapDataMarkPropagateExpire(swapData *data) {
    data->propagate_expire = 1;
}

int swapDataSetupMeta(swapData *d, int swap_type, long long expire,
        void **datactx) {
    int retval;
    serverAssert(d->type == NULL);

    d->expire = expire;
    d->swap_type = swap_type;

    if (!swapDataMarkedPropagateExpire(d) &&
            swapDataExpiredAndShouldDelete(d)) {
        swapDataMarkPropagateExpire(d);
    }

    if (datactx) *datactx = NULL;

    switch (d->swap_type) {
    case SWAP_TYPE_STRING:
        retval = swapDataSetupWholeKey(d,datactx);
        break;
    case SWAP_TYPE_HASH:
        retval = swapDataSetupHash(d,datactx);
        break;
    case SWAP_TYPE_SET:
        retval = swapDataSetupSet(d,datactx);
        break;
    case SWAP_TYPE_ZSET:
        retval = swapDataSetupZSet(d, datactx);
        break;
    case SWAP_TYPE_LIST:
        retval = swapDataSetupList(d, datactx);
        break;
    case SWAP_TYPE_STREAM:
        retval = SWAP_ERR_SETUP_UNSUPPORTED;
        break;
    case SWAP_TYPE_BITMAP:
        retval = swapDataSetupBitmap(d, datactx);
        break;
    default:
        retval = SWAP_ERR_SETUP_FAIL;
        break;
    }
    return retval;
}



static inline int swapDataAnaSwapType(robj *value, objectMeta *object_meta) {
    if (object_meta) return object_meta->swap_type;
    if (value) return value->type;
    return -1;
}





static
inline int swapBatchCtxExceedsLimit(swapBatchCtx *batch_ctx) {
    int exceeded = 0;
    swapBatchLimitsConfig *limit;

    serverAssert(swapIntentionInOutDel(batch_ctx->cmd_intention));

    limit = server.storage.swap_batch_limits+batch_ctx->cmd_intention;
    if (limit->count > 0 && batch_ctx->batch->count >= (size_t)limit->count) {
        exceeded = 1;
    }

    /* TODO account for mem as well. */
    return exceeded;
}
/* swapBatchCtxFlush*/
static inline int swapRequestBatchEmpty(swapRequestBatch *reqs) {
    return reqs->count == 0;
}
/* swapRequestBatch: batch of requests that submitted together, those requests
 * does not depend on each other to proceed or unlock.
 * Although these requests have same cmd intentions, their swap intention
 * might differ because swapAna might result in different intention. */
swapRequestBatch *swapRequestBatchNew() {
    swapRequestBatch *reqs = zmalloc(sizeof(swapRequestBatch));
    reqs->reqs = reqs->req_buf;
    reqs->capacity = SWAP_BATCH_DEFAULT_SIZE;
    reqs->count = 0;
    reqs->swap_queue_timer = 0;
    reqs->notify_queue_timer = 0;
    return reqs;
}
static inline swapRequestBatch *swapBatchCtxShift(swapBatchCtx *batch_ctx) {
    serverAssert(batch_ctx->batch != NULL);
    swapRequestBatch *reqs = batch_ctx->batch;
    batch_ctx->batch = swapRequestBatchNew();
    return reqs;
}

void swapRequestBatchFree(swapRequestBatch *reqs) {
    if (reqs == NULL) return;
    for (size_t i = 0; i < reqs->count; i++) {
        swapRequestFree(reqs->reqs[i]);
        reqs->reqs[i] = NULL;
    }
    if (reqs->reqs != reqs->req_buf) {
        zfree(reqs->reqs);
        reqs->reqs = NULL;
    }
    zfree(reqs);
}


static inline void submitSwapRequestBatch(int mode,swapRequestBatch *reqs, int idx) {
    if (mode == SWAP_MODE_ASYNC) {
        asyncSwapRequestBatchSubmit(reqs,idx);
    } else {
        parallelSyncSwapRequestBatchSubmit(reqs,idx);
    }
}
size_t swapBatchCtxFlush(swapBatchCtx *batch_ctx, int reason) {
    if (swapRequestBatchEmpty(batch_ctx->batch)) return 0;
    int thread_idx = batch_ctx->thread_idx;
    swapRequestBatch *reqs = swapBatchCtxShift(batch_ctx);
    size_t reqs_count = reqs->count;
    batch_ctx->stat.submit_batch_count++;
    batch_ctx->stat.submit_request_count+=reqs_count;
    batch_ctx->stat.submit_batch_flush[reason]++;
    submitSwapRequestBatch(SWAP_MODE_ASYNC,reqs,thread_idx);
    return reqs_count;
}

void swapRequestBatchAppend(swapRequestBatch *reqs, swapRequest *req) {
    if (reqs->count == reqs->capacity) {
        reqs->capacity = reqs->capacity < SWAP_BATCH_LINEAR_SIZE ? reqs->capacity*2 : reqs->capacity + SWAP_BATCH_LINEAR_SIZE;
        serverAssert(reqs->capacity > reqs->count);
        if (reqs->reqs == reqs->req_buf) {
            reqs->reqs = zmalloc(sizeof(swapRequest*)*reqs->capacity);
            memcpy(reqs->reqs,reqs->req_buf,reqs->count*sizeof(swapRequest*));
        } else {
            reqs->reqs = zrealloc(reqs->reqs,sizeof(swapRequest*)*reqs->capacity);
        }
    }
    reqs->reqs[reqs->count++] = req;
}
void swapBatchCtxFeed(swapBatchCtx *batch_ctx, int flush,
        swapRequest *req, int thread_idx) {
    int cmd_intention;

    if (req->intention == SWAP_UNSET) {
        cmd_intention = req->key_request->cmd_intention;
    } else {
        cmd_intention = req->intention;
    }

    /* flush before handling req if req is dispatched to another thread. */
    if (batch_ctx->thread_idx != thread_idx) {
        swapBatchCtxFlush(batch_ctx,SWAP_BATCH_FLUSH_THREAD_SWITCH);
    } else if (batch_ctx->cmd_intention != cmd_intention) {
        swapBatchCtxFlush(batch_ctx,SWAP_BATCH_FLUSH_INTENT_SWITCH);
    } else {
        /* no need to flush beforehand */
    }

    batch_ctx->thread_idx = thread_idx;
    batch_ctx->cmd_intention = cmd_intention;

    swapRequestBatchAppend(batch_ctx->batch,req);

    /* flush after handling req if flush hint set. */
    /* execute after append req if exceeded swap-batch-limit */
    if (flush) {
        swapBatchCtxFlush(batch_ctx,SWAP_BATCH_FLUSH_FORCE_FLUSH);
    } else if (!swapIntentionInOutDel(batch_ctx->cmd_intention)) {
        swapBatchCtxFlush(batch_ctx,SWAP_BATCH_FLUSH_UTILS_TYPE);
    } else if (swapBatchCtxExceedsLimit(batch_ctx)) {
        swapBatchCtxFlush(batch_ctx,SWAP_BATCH_FLUSH_REACH_LIMIT);
    } else {
        /* no need to flush afterwards */
    }
}



static inline swapRequest *swapMetaRequestNew(
    keyRequest *key_request, swapCtx *ctx, swapData *data,
    void *datactx, swapTrace *trace, swapRequestFinishedCallback cb, void *pd,
    void *msgs) {
  return swapRequestNew(key_request,SWAP_UNSET,-1,ctx,data,datactx,trace,cb,pd,msgs);
}


robj *lookupDirtySubkeys(redisDb *db, robj* key) {
    return dictFetchValue(db->storage.dirty_subkeys,key->ptr);
}

void swapCtxSetSwapData(swapCtx *ctx, MOVE swapData *data, MOVE void *datactx) {
    ctx->data = data;
    ctx->datactx = datactx;
}


#define BUFFERED_ALLOCATOR_CAPACITY_SWAPDATA 4096
#define BUFFERED_ALLOCATOR_CAPACITY_SWAPCTX 4096
bufferedAllocator *buffered_allocator_swapctx;
bufferedAllocator *buffered_allocator_swapdata;
void initSwapRequest() {
    buffered_allocator_swapdata = bufferedAllocatorCreate(
            BUFFERED_ALLOCATOR_CAPACITY_SWAPDATA,sizeof(struct swapData),NULL,NULL);
    buffered_allocator_swapctx = bufferedAllocatorCreate(
            BUFFERED_ALLOCATOR_CAPACITY_SWAPCTX,sizeof(struct swapCtx),NULL,NULL);
}
swapData *createSwapData(redisDb *db, robj *key, robj *value, robj* dirty_subkeys) {
    swapData *data = bufferedAllocatorAlloc(buffered_allocator_swapdata);
    memset(data,0,sizeof(struct swapData));

    data->db = db;
    if (key) incrRefCount(key);
    data->key = key;
    if (value) {
        incrRefCount(value);
    }
    data->value = value;
    if (dirty_subkeys) incrRefCount(dirty_subkeys);
    data->dirty_subkeys = dirty_subkeys;
    return data;
}

void clientGotLock(client *c, swapCtx *ctx, void *lock) {
    serverAssert(ctx->swap_lock == NULL);
    ctx->swap_lock = lock;
    switch (c->deferred_cmd->swap_lock_mode) {
    case SWAP_LOCK_UNIQUE:
        serverAssert(c->deferred_cmd->swap_locks != NULL);
        listAddNodeTail(c->deferred_cmd->swap_locks,lock);
        break;
    case SWAP_LOCK_SHARED:
    default:
        break;
    }
}
list *clientRenewLocks(client *c) {
    list *old = c->deferred_cmd->swap_locks;
    c->deferred_cmd->swap_locks = listCreate();
    return old;
}
void clientReleaseLocks(client *c, swapCtx *ctx) {
    list *locks;
    listNode *ln;
    listIter li;
    if (ctx && ctx->data && ctx->data->value != NULL) {
        decrRefCount(ctx->data->value);
        ctx->data->value = NULL;
    }
    switch (c->deferred_cmd->swap_lock_mode) {
    case SWAP_LOCK_UNIQUE:
        locks = clientRenewLocks(c);
        listRewind(locks,&li);
        while ((ln = listNext(&li))) {
            lockUnlock(listNodeValue(ln));
        }
        listRelease(locks);
        break;
    case SWAP_LOCK_SHARED:
        if (ctx->swap_lock) {
            lockUnlock(ctx->swap_lock);
        }
        break;
    default:
        break;
    }
}

void keyRequestProceed(void *lock, int flush, redisDb *db, robj *key,
        client *c, void *pd) {
    int reason_num = 0, retval = 0, swap_intention, errcode, swap_type;
    void *datactx = NULL;
    swapData *data = NULL;
    swapCtx *ctx = pd;
    robj *value, *dirty_subkeys;
    objectMeta *object_meta;
    char *reason;
    void *msgs = NULL;
    uint32_t swap_intention_flags;
    long long expire;
    int cmd_intention = ctx->key_request->cmd_intention;
    uint32_t cmd_intention_flags = ctx->key_request->cmd_intention_flags;
    int thread_idx = ctx->key_request->deferred ? server.storage.swap_defer_thread_idx : -1;
    UNUSED(reason);
    swapRequest *req = NULL;

#ifdef SWAP_DEBUG
    msgs = &ctx->msgs;
#endif

    serverAssert(c == ctx->c);
    clientGotLock(c,ctx,lock);

    if (db == NULL || key == NULL) {
        reason = "noswap needed for db/svr level request";
        reason_num = NOSWAP_REASON_NOTKEYLEVEL;
        goto noswap;
    }

	/* handle metascan request. */
    if (isMetaScanRequest(cmd_intention_flags)) {
        data = createSwapData(db,NULL,NULL,NULL);
        retval = swapDataSetupMetaScan(data,cmd_intention_flags,c,&datactx);
        swapCtxSetSwapData(ctx,data,datactx);
        if (retval) {
            ctx->errcode = retval;
            reason = "setup metascan failed";
            reason_num = NOSWAP_REASON_UNEXPECTED;
            goto noswap;
        } else {
            goto allset;
        }
    }

    value = dbFindByLink(db, key->ptr, NULL);
    dirty_subkeys = lookupDirtySubkeys(db,key);

    data = createSwapData(db,key,value,dirty_subkeys);
    swapCtxSetSwapData(ctx,data,datactx);

    if (isSwapHitStatKeyRequest(ctx->key_request)) {
        atomicIncr(server.storage.swap_hit_stats->stat_swapin_attempt_count,1);
    }

    /* slave expire decided before swap */
    if (cmd_intention_flags & SWAP_EXPIRE_FORCE) {
        swapDataMarkPropagateExpire(data);
    }

    if (value == NULL) {
        if (cmd_intention == SWAP_OUT) {
            /* nothing to persist or evict. */
            reason = "key already swapped out";
            reason_num = NOSWAP_REASON_ALREAY_SWAPPED_OUT;
            goto noswap;
        }

        int filt_by;
        if (!coldFilterMayContainKey(db->storage.cold_filter,key->ptr,&filt_by)) {
            reason = "key is absent";
            if (filt_by == COLDFILTER_FILT_BY_CUCKOO_FILTER)
                reason_num = NOSWAP_REASON_FILT_BY_CUCKOOFILTER;
            else
                reason_num = NOSWAP_REASON_FILT_BY_ABSENTCACHE;
            goto noswap;
        } else {
            req = swapMetaRequestNew(ctx->key_request,
                    ctx,data,datactx,ctx->key_request->trace,
                    keyRequestSwapFinished,ctx,msgs);
            swapBatchCtxFeed(server.storage.swap_batch_ctx,flush,req,thread_idx);
            return;
        }
    }

    expire = getExpire(db,key->ptr, NULL);

    object_meta = lookupMeta(db,key);
    swap_type = swapDataAnaSwapType(value,object_meta);
    if (swap_type < 0) {
        reason = "data not support swap";
        reason_num = NOSWAP_REASON_KEYNOTSUPPORT;
        ctx->errcode = SWAP_ERR_SETUP_UNEXPECTED_SWAP_TYPE;
        goto noswap;
    }

    retval = swapDataSetupMeta(data,swap_type,expire,&datactx);
    swapCtxSetSwapData(ctx,data,datactx);
    if (retval) {
        if (retval == SWAP_ERR_SETUP_UNSUPPORTED) {
            reason = "data not support swap";
            reason_num = NOSWAP_REASON_KEYNOTSUPPORT;
        } else {
            ctx->errcode = retval;
            reason = "setup meta failed";
            reason_num = NOSWAP_REASON_UNEXPECTED;
        }
        goto noswap;
    }

    swapDataSetObjectMeta(data,object_meta);

allset:

    if ((errcode = swapDataAna(data,SWAP_ANA_THD_MAIN,ctx->key_request,&swap_intention,
                &swap_intention_flags,datactx))) {
        if (errcode == SWAP_ERR_DATA_WRONG_TYPE_ERROR) {
            ctx->errcode = errcode;
            reason = "swap data type error";
            reason_num = NOSWAP_REASON_UNEXPECTED;
        } else {
            ctx->errcode = SWAP_ERR_DATA_ANA_FAIL;
            reason = "swap ana failed";
            reason_num = NOSWAP_REASON_UNEXPECTED;
        }

        goto noswap;
    }

    if (swap_intention == SWAP_NOP) {
        reason = "swapana decided no swap";
        reason_num = NOSWAP_REASON_SWAPANADECIDED;
        goto noswap;
    }

    DEBUG_MSGS_APPEND(&ctx->msgs,"request-proceed","start swap=%s",
            swapIntentionName(swap_intention));

    req = swapDataRequestNew(swap_intention,swap_intention_flags,ctx,data,
            datactx,ctx->key_request->trace,keyRequestSwapFinished,ctx,msgs);

    swapBatchCtxFeed(server.storage.swap_batch_ctx,flush,req,thread_idx);

    return;

noswap:
    DEBUG_MSGS_APPEND(&ctx->msgs,"request-proceed",
            "no swap needed: %s", reason);
    if (isSwapHitStatKeyRequest(ctx->key_request)) {
        if (reason_num == NOSWAP_REASON_SWAPANADECIDED)
            atomicIncr(server.storage.swap_hit_stats->stat_swapin_no_io_count,1);
        if (reason_num == NOSWAP_REASON_FILT_BY_CUCKOOFILTER)
            atomicIncr(server.storage.swap_hit_stats->stat_swapin_not_found_coldfilter_cuckoofilter_filt_count,1);
        if (reason_num == NOSWAP_REASON_FILT_BY_ABSENTCACHE)
            atomicIncr(server.storage.swap_hit_stats->stat_swapin_not_found_coldfilter_absentcache_filt_count,1);
    }

    /* noswap is kinda swapfinished. */
    if (ctx->key_request->trace) ctx->key_request->trace->swap_dispatch_time = getMonotonicUs();
    keyRequestSwapFinished(data,ctx,ctx->errcode);

    return;
}
void swapCmdSwapFinished(swapCmdTrace *swap_cmd) {
    swap_cmd->finished_swap_cnt++;
    if (swap_cmd->finished_swap_cnt == swap_cmd->swap_cnt) {
        swap_cmd->swap_finished_time = getMonotonicUs();
    }
}

void replySwapFailed(client *c) {
    serverAssert(c->deferred_cmd->swap_errcode);
    switch (c->deferred_cmd->swap_errcode) {
    case SWAP_ERR_METASCAN_UNSUPPORTED_IN_MULTI:
        rejectCommandFormat(c,
                "Swap failed: scan not supported in multi.");
        break;
    case SWAP_ERR_METASCAN_SESSION_UNASSIGNED:
        rejectCommandFormat(c,
                "Swap failed: scan session unassigned");
        break;
    case SWAP_ERR_METASCAN_SESSION_INPROGRESS:
        rejectCommandFormat(c,
                "Swap failed: scan in progress.");
        break;
    case SWAP_ERR_METASCAN_SESSION_SEQUNMATCH:
        rejectCommandFormat(c,
                "Swap failed: cursor not match (restart scan with cursor 0 when failed)");
        break;
    case SWAP_ERR_DATA_WRONG_TYPE_ERROR:
        addReplyErrorObject(c,shared.wrongtypeerr);
        break;
    default:
        rejectCommandFormat(c,"Swap failed (code=%d)",c->deferred_cmd->swap_errcode);
        break;
    }
}



void continueProcessCommand(client *c) {
	c->deferred_cmd->flags &= ~CLIENT_DEFERRED_SWAPPING;
    client *old_client = server.current_client;
    server.current_client = c;

	if (c->deferred_cmd->swap_errcode) {
        replySwapFailed(c);
        c->lastcmd->calls++;
        c->lastcmd->failed_calls++;
        c->deferred_cmd->swap_errcode = 0;
        if (c->flags & CLIENT_REEXECUTING_COMMAND) {
            server.storage.swap_dependency_block_ctx->swap_err_count++;
        }
    } else {
		call(c,CMD_CALL_FULL);
		/* post call */
		c->woff = server.master_repl_offset;
		if (listLength(server.ready_keys) && !isInsideYieldingLongCommand())
			handleClientsBlockedOnKeys();
	}

    /* post command */
    commandProcessed(c);
    if (c->flags & CLIENT_REEXECUTING_COMMAND) {
        server.storage.swap_dependency_block_ctx->swapping_count--;
        c->flags &= ~CLIENT_REEXECUTING_COMMAND;
    }
    server.current_client = old_client;
    c->deferred_cmd->flags |= CLIENT_DEFERRED_UNLOCKING;
    clientReleaseLocks(c,NULL/*ctx unused*/);
    c->deferred_cmd->flags &= ~CLIENT_DEFERRED_UNLOCKING;

    /* pipelined command might already read into querybuf, if process not
     * restarted, pending commands would not be processed again. */
    //LATTE_TO_DO
    if (!c->deferred_cmd->CLIENT_DEFERED_CLOSING && c->querybuf != NULL) processInputBuffer(c);
}


inline int swapDataBeforeCall(swapData *d, keyRequest *key_request,
        client *c, void *datactx) {
    if (d->type->beforeCall)
        return d->type->beforeCall(d,key_request,c,datactx);
    else
        return 0;
}

void keyRequestBeforeCall(client *c, swapCtx *ctx) {
    swapData *data = ctx->data;
    void *datactx = ctx->datactx;
    if (data == NULL) return;
    if (!swapDataAlreadySetup(data)) return;
    swapDataBeforeCall(data,ctx->key_request,c,datactx);
}

void normalClientKeyRequestFinished(client *c, swapCtx *ctx) {
    robj *key = ctx->key_request->key;
    UNUSED(key);
    DEBUG_MSGS_APPEND(&ctx->msgs,"request-finished",
            "key=%s, keyrequests_count=%d, errcode=%d",
            key?(sds)key->ptr:"<nil>", c->deferred_cmd->keyrequests_count, ctx->errcode);
    c->deferred_cmd->keyrequests_count--;
    swapCmdSwapFinished(ctx->key_request->swap_cmd);
    if (ctx->errcode) clientSwapError(c,ctx->errcode);
    keyRequestBeforeCall(c,ctx);
    if (ctx->data && ctx->data->value != NULL) {
        decrRefCount(ctx->data->value);
        ctx->data->value = NULL;
    }
    if (c->deferred_cmd->keyrequests_count == 0) {
        continueProcessCommand(c);
    }
}

void getKeyRequestsPrepareResult(getKeyRequestsResult *result, int num) {
	/* GETKEYS_RESULT_INIT initializes keys to NULL, point it to the
     * pre-allocated stack buffer here. */
	if (!result->key_requests) {
		serverAssert(!result->num);
		result->key_requests = result->buffer;
	}

	/* Resize if necessary */
	if (num > result->size) {
		if (result->key_requests != result->buffer) {
			/* We're not using a static buffer, just (re)alloc */
			result->key_requests = zrealloc(result->key_requests,
                    num*sizeof(keyRequest));
		} else {
			/* We are using a static buffer, copy its contents */
			result->key_requests = zmalloc(num*sizeof(keyRequest));
			if (result->num) {
				memcpy(result->key_requests,result->buffer,
                        result->num*sizeof(keyRequest));
            }
		}
		result->size = num;
	}
}

static inline int clientSwitchDb(client *c, int argidx) {
    long long dbid;
    if (getLongLongFromObject(c->argv[argidx],&dbid)) return C_ERR;
    if (dbid < 0 || dbid > server.dbnum)  return C_ERR;
    selectDb(c,dbid);
    return C_OK;
}

swapCmdTrace *createSwapCmdTrace() {
    swapCmdTrace *cmd = zcalloc(sizeof(swapCmdTrace));
    return cmd;
}


inline void getKeyRequestsAttachSwapTrace(getKeyRequestsResult * result, swapCmdTrace *swap_cmd,
                                   int from, int count) {
    if (server.storage.swap_debug_trace_latency) {
        initSwapTraces(swap_cmd, count);
        for (int i = 0; i < count; i++) {
            result->key_requests[from + i].swap_cmd = swap_cmd;
            result->key_requests[from + i].trace = swap_cmd->swap_traces + i;
        }
    } else {
        swap_cmd->swap_cnt = count;
        for (int i = 0; i < count; i++) result->key_requests[from + i].swap_cmd = swap_cmd;
    }
}


keyRequest *getKeyRequestsAppendCommonResult(getKeyRequestsResult *result,
        int level, robj *key, int cmd_intention, int cmd_intention_flags, uint64_t cmd_flags,
        int dbid) {
    if (result->num == result->size) {
        int newsize = result->size +
            (result->size > 8192 ? 8192 : result->size);
        getKeyRequestsPrepareResult(result, newsize);
    }

    keyRequest *key_request = &result->key_requests[result->num++];
    key_request->level = level;
    key_request->key = key;
    key_request->cmd_intention = cmd_intention;
    key_request->cmd_intention_flags = cmd_intention_flags;
    key_request->cmd_flags = cmd_flags;
    key_request->dbid = dbid;
    key_request->trace = NULL;
    key_request->deferred = 0;
    argRewriteRequestInit(key_request->arg_rewrite + 0);
    argRewriteRequestInit(key_request->arg_rewrite + 1);
    return key_request;
}

/* Note that key&subkeys ownership moved */
void getKeyRequestsAppendSubkeyResult(getKeyRequestsResult *result, int level,
        robj *key, int num_subkeys, robj **subkeys, int cmd_intention,
        int cmd_intention_flags, uint64_t cmd_flags, int dbid) {
    keyRequest *key_request = getKeyRequestsAppendCommonResult(result,level,
            key,cmd_intention,cmd_intention_flags,cmd_flags,dbid);

    key_request->type = KEYREQUEST_TYPE_SUBKEY;
    key_request->b.num_subkeys = num_subkeys;
    key_request->b.subkeys = subkeys;
    key_request->swap_cmd = NULL;
    key_request->trace = NULL;
    key_request->deferred = 0;
}

/* NOTE that result.{key,subkeys} are ONLY REFS to client argv (since client
 * outlives getKeysResult if no swap action happend. key, subkey will be
  * copied (using incrRefCount) when async swap acutally proceed. */
static int _getSingleCmdKeyRequests(int dbid, struct redisCommand* cmd,
        robj** argv, int argc, getKeyRequestsResult *result) {
    swapCommand *swap_cmd = NULL;
    if (swap_cmd->getkeyrequests_proc == NULL) {
        int i, numkeys;
        getKeysResult keys = GETKEYS_RESULT_INIT;
        /* whole key swaping, swaps defined by command arity. */
        numkeys = getKeysFromCommand(cmd,argv,argc,&keys);
        getKeyRequestsPrepareResult(result,result->num+numkeys);
        for (i = 0; i < numkeys; i++) {
            robj *key = argv[keys.keys[i].pos];

            incrRefCount(key);
            getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,key,0,NULL,
                    swap_cmd->intention,swap_cmd->intention_flags,cmd->flags, dbid);
        }
        getKeysFreeResult(&keys);
        return 0;
    } else if (cmd->flags & CMD_MODULE) {
        /* TODO support module */
    } else {
        return swap_cmd->getkeyrequests_proc(dbid,cmd,argv,argc,result);
    }
    return 0;
}

static void getSingleCmdKeyRequests(client *c, getKeyRequestsResult *result) {
    _getSingleCmdKeyRequests(c->db->id,c->cmd,c->argv,c->argc,result);
}

void getKeyRequests(client *c, getKeyRequestsResult *result) {
    getKeyRequestsPrepareResult(result, MAX_KEYREQUESTS_BUFFER);
    swapCmdTrace *swap_cmd;

    if ((c->flags & CLIENT_MULTI) &&
            !(c->flags & (CLIENT_DIRTY_CAS | CLIENT_DIRTY_EXEC)) &&
            (c->cmd->proc == execCommand || isGtidExecCommand(c))) {
        /* if current is EXEC, we get swaps for all queue commands. */
        robj **orig_argv;
        int i, orig_argc;
        redisDb *orig_db;
        struct redisCommand *orig_cmd;
        int need_swap = 0;

        orig_argv = c->argv;
        orig_argc = c->argc;
        orig_cmd = c->cmd;
        orig_db = c->db;

        if (isGtidExecCommand(c)) {
            if (clientSwitchDb(c,2) != C_OK)
                return;
        }
        //
        c->deferred_cmd->mstates = zmalloc(sizeof(swapMstate)*c->mstate.count);
        // maste.count 暂时没变化的话不改, TODO clientMstateCount(c)
        for (i = 0; i < c->mstate.count; i++) {
            int prev_keyrequest_num = result->num;
            // 需要支持多redis版本取值  clientMstateGetArgc, clientMstateGetArgv, clientMstateGetCmd
            c->argc = clientMstateGetArgc(c, i);
            c->argv = clientMstateGetArgv(c, i);
            c->cmd = clientMstateGetCmd(c, i);

            getSingleCmdKeyRequests(c, result);

            int requests_delta = result->num - prev_keyrequest_num;
            if (requests_delta) {
                need_swap = 1;
                swap_cmd = createSwapCmdTrace();
                getKeyRequestsAttachSwapTrace(result,swap_cmd,prev_keyrequest_num,requests_delta);
                c->deferred_cmd->mstates[i].cmd = c->cmd;
                c->deferred_cmd->mstates[i].swap_cmd = swap_cmd;
            }
            for (int j = prev_keyrequest_num; j < result->num; j++) {
                result->key_requests[j].arg_rewrite[0].mstate_idx = i;
                result->key_requests[j].arg_rewrite[1].mstate_idx = i;
            }

            if (c->cmd->proc == selectCommand) {
                if (clientSwitchDb(c,1) != C_OK)
                    return;
            }
        }

        c->argv = orig_argv;
        c->argc = orig_argc;
        c->cmd = orig_cmd;
        c->db = orig_db;
        if (need_swap) c->deferred_cmd->swap_cmd = createSwapCmdTrace();
    } else {
        int prev_keyrequest_num = result->num;
        getSingleCmdKeyRequests(c, result);
        int requests_delta = result->num - prev_keyrequest_num;
        if (requests_delta) {
            swap_cmd = createSwapCmdTrace();
            getKeyRequestsAttachSwapTrace(result,swap_cmd,prev_keyrequest_num,requests_delta);
            c->deferred_cmd->swap_cmd = swap_cmd;
        }
    }
    result->swap_cmd = c->deferred_cmd->swap_cmd;
}

static void registerSwapToRewindClient(client *c) {
    serverAssert(c->cmd);
    c->deferred_cmd->flags |= CLIENT_DEFERRED_REWINDING;

    listAddNodeTail(server.storage.swap_torewind_clients,c);
}

static int registerSwapToRewindClientIfNeeded(client *c) {
    int is_may_replicate_command = (c->cmd->flags & (CMD_WRITE | CMD_MAY_REPLICATE)) ||
                                   (c->cmd->proc == execCommand && (c->mstate.cmd_flags & (CMD_WRITE | CMD_MAY_REPLICATE)));
    if (!(c->flags & CLIENT_SLAVE) &&
        ((server.storage.swap_rewind_type == SWAP_REWIND_ALL) ||
        (server.storage.swap_rewind_type == SWAP_REWIND_WRITE && is_may_replicate_command))) {
        registerSwapToRewindClient(c);
        return 1;
    } else {
        return 0;
    }
}

void freeClientSwapCmdTrace(client *c) {
    c->deferred_cmd->mstates[c->mstate.executing_cmd].swap_cmd = NULL;
    for (int i = 0; i < c->mstate.count; i++) {
        if (c->deferred_cmd->mstates[i].swap_cmd) {
            swapCmdTraceFree(c->deferred_cmd->mstates[i].swap_cmd);
            c->deferred_cmd->mstates[i].swap_cmd = NULL;
        }
    }
    zfree(c->deferred_cmd->mstates);

    if (c->deferred_cmd->swap_cmd) {
        swapCmdTraceFree(c->deferred_cmd->swap_cmd);
        c->deferred_cmd->swap_cmd = NULL;
    }
}

void startSwapRewind(swap_rewind_type rewind_type) {
    server.storage.swap_rewind_type = rewind_type;
    serverAssert(rewind_type != SWAP_REWIND_OFF);
    serverLog(LL_WARNING,"Start swap rewind(%d), current master_repl_offset:%lld", rewind_type, server.master_repl_offset);
}

static void startSwapRewindIfNeeded(client *c) {
    if (c->cmd && (c->cmd->proc == failoverCommand ||
                c->cmd->proc == replicaofCommand)) {
        startSwapRewind(SWAP_REWIND_WRITE);
    }
}




/*
 * Record key sequence for FIFO eviction
 */
static void recordKeySeqIfNeeded(getKeyRequestsResult *result) {
    // TODO importing
    // if (!isImportingFifoEnabled()) {
    //     return;
    // }

    for (int i = 0; i < result->num; i++) {
        keyRequest *key_request = &result->key_requests[i];
        robj *key = key_request->key;
        int dbid = key_request->dbid;

        if (key_request->level != REQUEST_LEVEL_KEY ||
            !(key_request->cmd_flags & CMD_WRITE) || 
            key == NULL) {
                continue;
        }
        // TODO importing  evict 队列去重，目前是直接添加到队列，后续可以优化成先检查待添加的key是否和队列最后一个key相同，如果相同则不添加
        // if (listLength(server.importing_evict_queue) != 0) {
        //     importingEvictKeyInfo *last_key_info = listNodeValue(listLast(server.importing_evict_queue));
        //     sds lastkey = last_key_info->key;

        //     if (sdscmp(key->ptr, lastkey) != 0) {
        //         addKeyInfoToEvictionQueue(key, dbid);
        //     }
        // } else {
        //      addKeyInfoToEvictionQueue(key, dbid);
        // }
    }
}




void moveKeyRequest(keyRequest *dst, keyRequest *src) {
    dst->key = src->key;
    src->key = NULL;
    dst->level = src->level;
    dst->cmd_intention = src->cmd_intention;
    dst->cmd_intention_flags = src->cmd_intention_flags;
    dst->dbid = src->dbid;
    dst->type = src->type;
    dst->swap_cmd = src->swap_cmd;
    dst->trace = src->trace;
    dst->deferred = src->deferred;
    dst->cmd_flags = src->cmd_flags;

    switch (src->type) {
    case KEYREQUEST_TYPE_KEY:
        break;
    case KEYREQUEST_TYPE_SUBKEY:
        dst->b.subkeys = src->b.subkeys;
        src->b.subkeys = NULL;
        dst->b.num_subkeys = src->b.num_subkeys;
        src->b.num_subkeys = 0;
        break;
    case KEYREQUEST_TYPE_RANGE:
        dst->l.num_ranges = src->l.num_ranges;
        dst->l.ranges = src->l.ranges;
        src->l.ranges = NULL;
        break;
    case KEYREQUEST_TYPE_SCORE:
        dst->zs.rangespec = src->zs.rangespec;
        src->zs.rangespec = NULL;
        dst->zs.reverse = src->zs.reverse;
        dst->zs.limit = src->zs.limit;
        break;
    case KEYREQUEST_TYPE_SAMPLE:
        dst->sp.count = src->sp.count;
        break;
    case KEYREQUEST_TYPE_BTIMAP_OFFSET:
        dst->bo.offset = src->bo.offset;
        break;
    case KEYREQUEST_TYPE_BTIMAP_RANGE:
        dst->br.start = src->br.start;
        dst->br.end = src->br.end;
        break;
    default:
        break;
    }

    dst->arg_rewrite[0] = src->arg_rewrite[0];
    dst->arg_rewrite[1] = src->arg_rewrite[1];
}
/* SwapCtx manages context and data for swapping specific key. Note that:
 * - key_request copy to swapCtx.key_request
 * - swapdata moved to swapCtx,
 * - swapRequest managed by async/sync complete queue (not by swapCtx).
 * swapCtx released when keyRequest finishes. */
swapCtx *swapCtxCreate(client *c, keyRequest *key_request,
        clientKeyRequestFinished finished, void* pd) {
    swapCtx *ctx = bufferedAllocatorAlloc(buffered_allocator_swapctx);
    memset(ctx,0,sizeof(swapCtx));

    ctx->c = c;
    moveKeyRequest(ctx->key_request,key_request);
    ctx->finished = finished;
    ctx->errcode = 0;
    ctx->swap_lock = NULL;
#ifdef SWAP_DEBUG
    char *key = key_request->key ? key_request->key->ptr : "(nil)";
    char identity[MAX_MSG];
    snprintf(identity,MAX_MSG,"[%s(%u):%s:%.*s]",
            swapIntentionName(key_request->cmd_intention),
            key_request->cmd_intention_flags,
            c->cmd->name,MAX_MSG/2,key);
    swapDebugMsgsInit(&ctx->msgs, identity);
#endif
    ctx->pd = pd;
    return ctx;
}

void keyRequestDeinit(keyRequest *key_request) {
    if (key_request == NULL) return;
    if (key_request->key) decrRefCount(key_request->key);
    key_request->key = NULL;

    switch (key_request->type) {
    case KEYREQUEST_TYPE_KEY:
        break;
    case KEYREQUEST_TYPE_SUBKEY:
        for (int i = 0; i < key_request->b.num_subkeys; i++) {
            if (key_request->b.subkeys[i])
                decrRefCount(key_request->b.subkeys[i]);
            key_request->b.subkeys[i] = NULL;
        }
        zfree(key_request->b.subkeys);
        key_request->b.subkeys = NULL;
        key_request->b.num_subkeys = 0;
        break;
    case KEYREQUEST_TYPE_RANGE:
        zfree(key_request->l.ranges);
        key_request->l.ranges = NULL;
        key_request->l.num_ranges = 0;
        break;
    case KEYREQUEST_TYPE_SCORE:
        if (key_request->zs.rangespec != NULL) {
            zfree(key_request->zs.rangespec);
            key_request->zs.rangespec = NULL;
        }
        break;
    case KEYREQUEST_TYPE_SAMPLE:
        key_request->sp.count = 0;
        break;
    case KEYREQUEST_TYPE_BTIMAP_OFFSET:
        key_request->bo.offset = 0;
        break;
    case KEYREQUEST_TYPE_BTIMAP_RANGE:
        key_request->br.start = 0;
        key_request->br.end = 0;
        break;
    default:
        break;
    }
}


void swapDataAbsentSubkeyFree(swapDataAbsentSubkey *absent) {
    if (absent == NULL) return;
    if (absent->subkeys) {
        for (size_t i = 0; i < absent->count; i++) {
            sdsfree(absent->subkeys[i]);
        }
        zfree(absent->subkeys);
        absent->subkeys = NULL;
    }
    zfree(absent);
}

inline void swapDataFree(swapData *d, void *datactx) {
    /* free extend */
    if (d->type && d->type->free) d->type->free(d,datactx);
    /* free base */
    if (d->cold_meta) freeObjectMeta(d->cold_meta);
    if (d->new_meta) freeObjectMeta(d->new_meta);
    if (d->value) freeObjAsync(d->key, d->value, d->db->id);
    if (d->key) decrRefCount(d->key);
    if (d->dirty_subkeys) decrRefCount(d->dirty_subkeys);
    if (d->absent) swapDataAbsentSubkeyFree(d->absent);
    bufferedAllocatorFree(buffered_allocator_swapdata,d);
}
void swapCtxFree(swapCtx *ctx) {
    if (!ctx) return;
#ifdef SWAP_DEBUG
    swapDebugMsgsDump(&ctx->msgs);
#endif
    keyRequestDeinit(ctx->key_request);
    if (ctx->data) {
        swapDataFree(ctx->data,ctx->datactx);
        ctx->data = NULL;
    }
    bufferedAllocatorFree(buffered_allocator_swapctx,ctx);
}

inline void swapCmdSwapSubmitted(swapCmdTrace *swap_cmd) {
    swap_cmd->swap_submitted_time = getMonotonicUs();
}


void _submitClientKeyRequests(client *c, getKeyRequestsResult *result,
        clientKeyRequestFinished cb, void* ctx_pd, int deferred) {
    int64_t txid = server.storage.swap_txid++;

    if (result->swap_cmd) swapCmdSwapSubmitted(result->swap_cmd);
    for (int i = 0; i < result->num; i++) {
        void *msgs = NULL;
        keyRequest *key_request = result->key_requests + i;
        key_request->deferred = deferred;
        redisDb *db = key_request->level == REQUEST_LEVEL_SVR ?
            NULL : server.db + key_request->dbid;
        robj *key = key_request->key;

        swapCtx *ctx = swapCtxCreate(c,key_request,cb, ctx_pd);
#ifdef SWAP_DEBUG
        msgs = &ctx->msgs;
#endif
        DEBUG_MSGS_APPEND(&ctx->msgs,"request-wait", "key=%s",
                key ? (sds)key->ptr : "<nil>");

        if (key_request->trace) swapTraceLock(key_request->trace);
        lockLock(txid,db,key,keyRequestProceed,c,ctx,
                (freefunc)swapCtxFree,msgs);
    }
}

void submitDeferredClientKeyRequests(client *c, getKeyRequestsResult *result,
                                     clientKeyRequestFinished cb, void* ctx_pd) {
    _submitClientKeyRequests(c, result, cb, ctx_pd, 1);
}
void submitClientKeyRequests(client *c, getKeyRequestsResult *result,
                             clientKeyRequestFinished cb, void* ctx_pd) {
    _submitClientKeyRequests(c, result, cb, ctx_pd, 0);
}

void releaseKeyRequests(getKeyRequestsResult *result) {
    for (int i = 0; i < result->num; i++) {
        keyRequest *key_request = result->key_requests + i;
        keyRequestDeinit(key_request);
    }
}

void getKeyRequestsFreeResult(getKeyRequestsResult *result) {
    if (result && result->key_requests != result->buffer) {
        zfree(result->key_requests);
    }
}
/* Returns submited keyrequest count, if any keyrequest submitted, command
 * gets called in contiunueProcessCommand instead of normal call(). */
int submitNormalClientRequests(client *c) {
    serverAssert(c->deferred_cmd->swap_cmd == NULL);
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    getKeyRequests(c,&result);
    c->deferred_cmd->keyrequests_count = result.num;
    if (registerSwapToRewindClientIfNeeded(c)) {
        freeClientSwapCmdTrace(c);
    } else {
        startSwapRewindIfNeeded(c);
        recordKeySeqIfNeeded(&result);
        submitClientKeyRequests(c,&result,normalClientKeyRequestFinished,NULL);
    }
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
    return result.num;
}





#ifdef REDIS_TEST
int swapDataTest(int argc, char *argv[], int accurate) {
    int error = 0, intention;
    uint32_t intention_flags;
    swapData *data;
    void *datactx;
    redisDb *db;
    robj *key1, *val1;

    UNUSED(argc), UNUSED(argv), UNUSED(accurate);

    TEST("swapdata - init") {
        initTestRedisServer();
        db = server.db + 0;
        key1 = createRawStringObject("key1",4);
        val1 = createRawStringObject("val1",4);
    }

    TEST("swapdata - propagate_expire") {
        keyRequest key_request_, *key_request = &key_request_;
        key_request->level = REQUEST_LEVEL_KEY;
        key_request->key = key1;
        key_request->b.subkeys = NULL;
        key_request->cmd_intention = SWAP_IN;
        key_request->cmd_intention_flags = 0;
        key_request->dbid = 0;

        data = createSwapData(db,key1,NULL,NULL);
        test_assert(!swapDataAlreadySetup(data));
        swapDataSetupMeta(data,OBJ_STRING,0/*expired*/,&datactx);
        test_assert(swapDataAlreadySetup(data));
        swapDataAna(data,0,key_request,&intention,&intention_flags,datactx);
        test_assert(intention == SWAP_DEL);
        test_assert(intention_flags == 0);
        test_assert(data->propagate_expire == 1);

        swapDataFree(data,datactx);
    }

    TEST("swapdata - string cmd operate expired bitmap") {
        keyRequest key_request_, *key_request = &key_request_;
        key_request->level = REQUEST_LEVEL_KEY;
        key_request->key = key1;
        key_request->b.subkeys = NULL;
        key_request->cmd_intention = SWAP_IN;
        key_request->cmd_intention_flags = 0;
        key_request->dbid = 0;
        key_request->cmd_flags |= CMD_SWAP_DATATYPE_STRING;

        data = createSwapData(db,key1,NULL,NULL);
        test_assert(!swapDataAlreadySetup(data));
        swapDataSetupMeta(data,SWAP_TYPE_BITMAP,0/*expired*/,&datactx);
        test_assert(swapDataAlreadySetup(data));
        swapDataAna(data,0,key_request,&intention,&intention_flags,datactx);
        test_assert(intention == SWAP_DEL);
        test_assert(intention_flags == 0);
        test_assert(data->propagate_expire == 1);

        swapDataFree(data,datactx);
    }

    TEST("swapdata - set_dirty") {
        keyRequest key_request_, *key_request = &key_request_;
        key_request->level = REQUEST_LEVEL_KEY;
        key_request->key = key1;
        key_request->b.subkeys = NULL;
        key_request->cmd_intention = SWAP_IN;
        key_request->cmd_intention_flags = SWAP_IN_DEL;
        key_request->dbid = 0;

        data = createSwapData(db,key1,val1,NULL);
        swapDataSetupMeta(data,OBJ_STRING,-1,&datactx);
        swapDataAna(data,0,key_request,&intention,&intention_flags,datactx);
        test_assert(intention == SWAP_DEL);
        test_assert(intention_flags == SWAP_FIN_DEL_SKIP);
        test_assert(data->set_dirty == 1);

        swapDataFree(data,datactx);
    }

    TEST("swapdata - deinit") {
        decrRefCount(key1), decrRefCount(val1);
    }

    return error;
}

#endif