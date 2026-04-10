#include "ctrip_storage_request.h"
#include "ctrip_storage_batch.h"
#include "ctrip_storage_metric.h"
#include "ctrip_storage_request_utils.h"
#include "ctrip_storage_rio.h"
void swapRequestBatchProcessStart(swapRequestBatch *reqs) {
    if (reqs->swap_queue_timer) {
        metricDebugInfo(SWAP_DEBUG_SWAP_QUEUE_WAIT,elapsedUs(reqs->swap_queue_timer));
    }
    for (size_t i = 0; i < reqs->count; i++) {
        swapRequest *req = reqs->reqs[i];
        if (req->trace) swapTraceProcess(req->trace);
    }
}

void swapRequestBatchProcessEnd(swapRequestBatch *reqs) {
    if (server.storage.swap_debug_trace_latency) elapsedStart(&reqs->notify_queue_timer);
    for (size_t i = 0; i < reqs->count; i++) {
        swapRequest *req = reqs->reqs[i];
        if (req->trace) swapTraceNotify(req->trace,req->intention);
    }
    reqs->notify_cb(reqs, reqs->notify_pd);
}

/* swapExecBatch: batch of requests with the same SWAP intention and action. */
void swapExecBatchInit(swapExecBatch *exec_batch) {
    exec_batch->reqs = exec_batch->req_buf;
    exec_batch->capacity = SWAP_BATCH_DEFAULT_SIZE;
    exec_batch->count = 0;
    exec_batch->intention = SWAP_UNSET;
    exec_batch->action = ROCKS_UNSET;
}

void swapExecBatchAppend(swapExecBatch *exec_batch, swapRequest *req) {
    if (exec_batch->count == exec_batch->capacity) {
        exec_batch->capacity = exec_batch->capacity < SWAP_BATCH_LINEAR_SIZE ? exec_batch->capacity*2 : exec_batch->capacity + SWAP_BATCH_LINEAR_SIZE;
        serverAssert(exec_batch->capacity > exec_batch->count);
        if (exec_batch->reqs == exec_batch->req_buf) {
            exec_batch->reqs = zmalloc(sizeof(swapRequest*)*exec_batch->capacity);
            memcpy(exec_batch->reqs,exec_batch->req_buf,sizeof(swapRequest*)*exec_batch->count);
        } else {
            exec_batch->reqs = zrealloc(exec_batch->reqs,sizeof(swapRequest*)*exec_batch->capacity);
        }
    }
    exec_batch->reqs[exec_batch->count++] = req;
}

sds swapDataEncodeMetaKey(swapData *d) {
    return rocksEncodeMetaKey(d->db,(sds)d->key->ptr);
}
static inline char objectType2Abbrev(int object_type) {
    char abbrevs[] = {'K','L','S','Z','H','M','X','B'};
    if (object_type >= 0 && object_type < (int)sizeof(abbrevs)) {
        return abbrevs[object_type];
    } else {
        return '?';
    }
}
sds rocksEncodeMetaVal(int swap_type, long long expire, uint64_t version,
        sds extend) {
    uint64_t encoded_version = rocksEncodeVersion(version);
    size_t len = 1 + sizeof(expire) + sizeof(encoded_version) +
        (extend ? sdslen(extend) : 0);
    sds raw = sdsnewlen(SDS_NOINIT,len), ptr = raw;
    ptr[0] = objectType2Abbrev(swap_type), ptr++;
    memcpy(ptr,&expire,sizeof(expire)), ptr+=sizeof(expire);
    memcpy(ptr,&encoded_version,sizeof(encoded_version));
    ptr += sizeof(encoded_version);
    if (extend) memcpy(ptr,extend,sdslen(extend));
    return raw;
}
inline void *swapDataGetObjectMetaAux(swapData *data, void *datactx) {
    if (data->type->getObjectMetaAux)
        return data->type->getObjectMetaAux(data,datactx);
    else
        return NULL;
}

sds swapDataEncodeMetaVal(swapData *d, void *datactx) {
    sds extend = NULL, encoded;
    objectMeta *object_meta = swapDataObjectMeta(d);
    uint64_t version = object_meta ? object_meta->version : SWAP_VERSION_ZERO;
    if (d->omtype->encodeObjectMeta) {
        void *omaux = swapDataGetObjectMetaAux(d,datactx);
        extend = d->omtype->encodeObjectMeta(object_meta,omaux, NORMAL_MODE);
    }
    encoded = rocksEncodeMetaVal(d->swap_type,d->expire,version,extend);
    sdsfree(extend);
    return encoded;
}

void swapRequestUpdateStatsSwapInNotFound(swapRequest *req) {
    /* key confirmed not exists, no need to execute swap request. */
    serverAssert(!swapDataAlreadySetup(req->data));
    if (isSwapHitStatKeyRequest(req->key_request)) {
        atomicIncr(server.storage.swap_hit_stats->stat_swapin_not_found_coldfilter_miss_count,1);
    }
}

void swapRequestSetIntention(swapRequest *req, int intention,
        uint32_t intention_flags) {
    req->intention = intention;
    req->intention_flags = intention_flags;
}

static inline void swapExecBatchExecuteStart(swapExecBatch *exec_batch) {
    elapsedStart(&exec_batch->swap_timer);
}

int swapDataDecodeAndSetupMeta(swapData *d, sds rawval, void **datactx) {
    const char *extend;
    size_t extend_len;
    int retval = 0, swap_type;
    long long expire;
    uint64_t version;
    objectMeta *object_meta = NULL;

    retval = rocksDecodeMetaVal(rawval,sdslen(rawval),&swap_type,&expire,
            &version,&extend,&extend_len);
    if (retval) return retval;

    retval = swapDataSetupMeta(d,swap_type,expire,datactx);
    if (retval) return retval;

    retval = buildObjectMeta(swap_type,version,extend,extend_len,&object_meta);
    if (retval) return SWAP_ERR_DATA_DECODE_META_FAILED;

    swapDataSetColdObjectMeta(d,object_meta/*moved*/);
    return retval;
}


void swapExecBatchPreprocess(swapExecBatch *meta_batch) {
    swapRequest *req;
    int errcode, intention;
    uint32_t intention_flags;
    RIO _rio = {0}, *rio = &_rio;
    int *cfs = zmalloc(meta_batch->count*sizeof(int));
    sds *rawkeys = zmalloc(meta_batch->count*sizeof(sds));

    for (size_t i = 0; i < meta_batch->count; i++) {
        req = meta_batch->reqs[i];
        cfs[i] = META_CF;
        rawkeys[i] = swapDataEncodeMetaKey(req->data);
    }

    RIOInitGet(rio,meta_batch->count,cfs,rawkeys);
    RIODo(rio);
    if ((errcode = RIOGetError(rio))) {
        swapExecBatchSetError(meta_batch,errcode);
        goto end;
    }

    for (size_t i = 0; i < meta_batch->count; i++) {
        req = meta_batch->reqs[i];
        sds rawval = rio->get.rawvals[i];
        if (rawval == NULL) {
            /* No swap needed if meta not found. */
            swapRequestSetIntention(req,SWAP_NOP,0);
            swapRequestUpdateStatsSwapInNotFound(req);
            continue;
        }

        if ((errcode = swapDataDecodeAndSetupMeta(req->data,rawval,
                        &req->datactx))) {
            swapRequestSetError(req,errcode);
            continue;
        }

        swapCtxSetSwapData(req->swap_ctx,req->data,req->datactx);

        if ((errcode = swapDataAna(req->data,SWAP_ANA_THD_SWAP,req->key_request,
                        &intention,&intention_flags,req->datactx))) {
            swapRequestSetError(req, errcode);
            continue;
        }

        swapRequestSetIntention(req,intention,intention_flags);
    }

end:
    /* cfs & rawkeys moved to rio */
    RIODeinit(rio);
}

void swapExecBatchDeinit(swapExecBatch *exec_batch) {
    if (exec_batch == NULL) return;
    if (exec_batch->reqs != exec_batch->req_buf) {
        zfree(exec_batch->reqs);
        exec_batch->reqs = NULL;
    }
}
void swapRequestBatchPreprocess(swapRequestBatch *reqs) {
    swapExecBatch meta_batch;

    swapExecBatchInit(&meta_batch);

    for (size_t i = 0; i < reqs->count; i++) {
        swapRequest *req = reqs->reqs[i];
        if (swapRequestIsMetaType(req)) {
            swapExecBatchAppend(&meta_batch,req);
        }
    }

    if (!swapExecBatchEmpty(&meta_batch)) {
        swapExecBatchPreprocess(&meta_batch);
    }

    swapExecBatchDeinit(&meta_batch);
}
#define swapExecBatchCtxInit swapExecBatchInit
#define swapExecBatchCtxDeinit swapExecBatchDeinit
#define swapExecBatchCtxEmpty swapExecBatchEmpty

void swapExecBatchCtxStart(swapExecBatch *exec_ctx) {
    UNUSED(exec_ctx);
}

static void swapRequestInIntentionDelEncodeKeys(swapRequest *req, RIO *rio,
        int merged_is_hot, int *pnumkeys, int **pcfs, sds **prawkeys) {
    int data_numkeys = 0, numkeys = 0;
    sds meta_rawkey = NULL, *data_rawkeys = NULL, *rawkeys = NULL;
    int *data_cfs = NULL, *tmpcfs = NULL, *cfs = NULL;

    serverAssert(rio->action == ROCKS_GET || rio->action == ROCKS_ITERATE);

    /* There is no need to delete subkey if meta gets deleted,
     * subkeys will be deleted by compaction filter (except for
     * string type, which is not deleted by compaction filter). */

    if (merged_is_hot) {
        meta_rawkey = swapDataEncodeMetaKey(req->data);
    }

    if (!merged_is_hot || req->data->swap_type == SWAP_TYPE_STRING) {
        int *rio_cfs = NULL, rio_numkeys = 0;
        sds *rio_rawkeys = NULL, *rio_rawvals = NULL;

        if (rio->action == ROCKS_GET) {
            rio_numkeys = rio->get.numkeys;
            rio_cfs = rio->get.cfs;
            rio_rawkeys = rio->get.rawkeys;
            rio_rawvals = rio->get.rawvals;
        } else { /* ROCKS_ITERATE */
            tmpcfs = zmalloc(sizeof(int)*rio->iterate.numkeys);
            for (int i = 0; i < rio->iterate.numkeys; i++)
                tmpcfs[i] = rio->iterate.cf;
            rio_numkeys = rio->iterate.numkeys;
            rio_cfs = tmpcfs;
            rio_rawkeys = rio->iterate.rawkeys;
            rio_rawvals = rio->iterate.rawvals;
        }

        if (req->data->type->rocksDel) {
            int data_action;
            req->data->type->rocksDel(req->data,req->datactx,
                    rio->action,rio_numkeys,rio_cfs,rio_rawkeys,rio_rawvals,
                    &data_action,&data_numkeys,&data_cfs,&data_rawkeys);
        } else {
            data_numkeys = rio_numkeys;
            data_cfs = zmalloc((rio_numkeys)*sizeof(int));
            data_rawkeys = zmalloc((rio_numkeys)*sizeof(sds));
            for (int i = 0; i < rio_numkeys; i++) {
                data_cfs[i] = rio_cfs[i];
                data_rawkeys[i] = sdsdup(rio_rawkeys[i]);
            }
        }
    }

    if (meta_rawkey && data_rawkeys) {
        /* string */
        numkeys = data_numkeys+1;

        cfs = zmalloc(numkeys*sizeof(int));
        memcpy(cfs,data_cfs,data_numkeys*sizeof(int));
        cfs[data_numkeys] = META_CF;
        zfree(data_cfs);
        data_cfs = NULL;

        rawkeys = zmalloc(numkeys*sizeof(sds));
        memcpy(rawkeys,data_rawkeys,data_numkeys*sizeof(sds));
        rawkeys[data_numkeys] = meta_rawkey;
        zfree(data_rawkeys);
        data_rawkeys = NULL;
        meta_rawkey = NULL;
    } else if (data_rawkeys) {
        /* hash/set/zset/list merged not hot. */
        numkeys = data_numkeys;
        cfs = data_cfs;
        rawkeys = data_rawkeys;
    } else if (meta_rawkey) {
        /* hash/set/zset/list merged is hot.*/
        numkeys = 1;
        cfs = zmalloc(sizeof(int));
        cfs[0] = META_CF;
        rawkeys = zmalloc(sizeof(sds));
        rawkeys[0] = meta_rawkey;
        meta_rawkey = NULL;
    } else {
        numkeys = 0;
        cfs = NULL;
        rawkeys = NULL;
    }

    *pnumkeys = numkeys;
    *pcfs = cfs;
    *prawkeys = rawkeys;

    if (tmpcfs) {
        zfree(tmpcfs);
        tmpcfs = NULL;
    }
}
void RIOBatchInit(RIOBatch *rios, int action) {
    rios->rios = rios->rio_buf;
    rios->capacity = SWAP_BATCH_DEFAULT_SIZE;
    rios->count = 0;
    rios->action = action;
}

inline int swapDataMergedIsHot(swapData *d, void *result, void *datactx) {
    return d->type->mergedIsHot(d,result,datactx);
}

/* Note that Alloc may invalidate previous RIO pointer. */
RIO *RIOBatchAlloc(RIOBatch *rios) {
    if (rios->count == rios->capacity) {
        rios->capacity = rios->capacity < SWAP_BATCH_LINEAR_SIZE ? rios->capacity*2 : rios->capacity + SWAP_BATCH_LINEAR_SIZE;
        serverAssert(rios->capacity > rios->count);
        if (rios->rios == rios->rio_buf) {
            rios->rios = zmalloc(sizeof(RIO)*rios->capacity);
            memcpy(rios->rios,rios->rio_buf,sizeof(RIO)*rios->count);
        } else {
            rios->rios = zrealloc(rios->rios,sizeof(RIO)*rios->capacity);
        }
    }
    return rios->rios + rios->count++;
}
static void swapExecBatchExecuteIntentionDel(swapExecBatch *exec_batch,
        RIOBatch *rios) {
    int errcode, *merged_is_hots = NULL;
    RIOBatch _aux_rios = {0}, *aux_rios = &_aux_rios;
    RIO *aux_rio;

    merged_is_hots = zcalloc(sizeof(int)*exec_batch->count);
    RIOBatchInit(aux_rios,ROCKS_DEL);

    for (size_t i = 0; i < exec_batch->count; i++) {
        int is_hot, aux_numkeys, *aux_cfs;
        sds *aux_rawkeys;
        RIO *rio = rios->rios+i;
        swapRequest *req = exec_batch->reqs[i];
        if (!(req->intention_flags & SWAP_EXEC_IN_DEL)) continue;
        if (swapRequestGetError(req)) continue;

        is_hot = swapDataMergedIsHot(req->data,req->result,req->datactx);
        if (!is_hot && (req->intention_flags & SWAP_EXEC_FORCE_HOT)) {
            serverLog(LL_WARNING, "[rocks] force del meta, key:%s", (char*)req->data->key->ptr);
            is_hot = 1;
        }
        merged_is_hots[i] = is_hot;

        aux_rio = RIOBatchAlloc(aux_rios);
        swapRequestInIntentionDelEncodeKeys(req,rio,is_hot,
                &aux_numkeys,&aux_cfs,&aux_rawkeys);
        RIOInitDel(aux_rio,aux_numkeys,aux_cfs,aux_rawkeys);
    }

    if (aux_rios->count > 0) RIOBatchDo(aux_rios);

    size_t aux_idx = 0;
    for (size_t i = 0; i < exec_batch->count; i++) {
        swapRequest *req = exec_batch->reqs[i];
        if (!(req->intention_flags & SWAP_EXEC_IN_DEL)) continue;
        if (swapRequestGetError(req)) continue;

        aux_rio = aux_rios->rios + aux_idx++;
        if ((errcode = RIOGetError(aux_rio))) {
            swapRequestSetError(req, errcode);
            continue;
        }

        if (merged_is_hots[i]) {
            /* shared subkey(subkey that exists both in rocksdb and redis)
             * gets deleted, because we dont known which are shared, so
             * we have to set whole key dirty */
            req->data->set_dirty = 1;
            req->data->persistence_deleted = 1;
        }
    }

    RIOBatchDeinit(aux_rios);
    if (merged_is_hots) {
        zfree(merged_is_hots);
        merged_is_hots = NULL;
    }
}

static
void swapExecBatchUpdateStatsRIOBatch(swapExecBatch *exec_batch,
        RIOBatch *rios) {
    size_t payload_size = 0;
    serverAssert(exec_batch->count == rios->count);
    for (size_t i = 0; i < rios->count; i++) {
        RIO *rio = rios->rios+i;
        swapRequest *req = exec_batch->reqs[i];
        size_t rio_memory = RIOEstimatePayloadSize(rio);
        payload_size += rio_memory;
        req->swap_memory += rio_memory;
    }
    atomicIncr(server.storage.swap_inprogress_memory,payload_size);
}

inline int swapDataEncodeRange(struct swapData *d, int intention, void *datactx,
        int *limit, uint32_t *flags, int *pcf, sds *start, sds *end) {
    if (d->type->encodeRange)
        return d->type->encodeRange(d,intention,datactx,limit,flags,pcf,start,end);
    else
        return 0;
}


static
void swapExecBatchPrepareRIOBatch(swapExecBatch *exec_batch, RIOBatch *rios) {
    for (size_t i = 0; i < exec_batch->count; i++) {
        int errcode, numkeys, *cfs = NULL;
        sds *rawkeys = NULL, *rawvals = NULL;
        swapRequest *req = NULL;
        RIO *rio = NULL;
        int cf, limit;
        uint32_t flags = 0;
        sds start = NULL, end = NULL;

        req = exec_batch->reqs[i];
        rio = RIOBatchAlloc(rios);

        switch (exec_batch->action) {
        case ROCKS_GET:
            if ((errcode = swapDataEncodeKeys(req->data,req->intention,
                            req->datactx,&numkeys,&cfs,&rawkeys))) {
                swapRequestSetError(req,errcode);
                serverAssert(numkeys == 0);
            }
            RIOInitGet(rio,numkeys,cfs,rawkeys);
            break;
        case ROCKS_ITERATE:
            if ((errcode = swapDataEncodeRange(req->data,req->intention,
                            req->datactx,&limit,&flags,&cf,&start,&end))) {
                swapRequestSetError(req,errcode);
                serverAssert(start == NULL);
            }
            RIOInitIterate(rio,cf,flags,start,end,limit);
            break;
        case ROCKS_PUT:
            if ((errcode = swapDataEncodeData(req->data,req->intention,
                            req->datactx,&numkeys,&cfs,&rawkeys,&rawvals))) {
                swapRequestSetError(req,errcode);
            }
            RIOInitPut(rio,numkeys,cfs,rawkeys,rawvals);
            break;
        case ROCKS_DEL:
            if ((errcode = swapDataEncodeKeys(req->data,req->intention,
                            req->datactx,&numkeys,&cfs,&rawkeys))) {
                swapRequestSetError(req,errcode);
            }
            RIOInitDel(rio,numkeys,cfs,rawkeys);
            break;
        default:
            serverPanic("exec: unexepcted action when prepare");
            swapRequestSetError(req,SWAP_ERR_EXEC_UNEXPECTED_ACTION);
            break;
        }

        if (req->errcode) continue;
        if (req->intention_flags & SWAP_EXEC_OOM_CHECK)
            rio->oom_check = 1;
    }
}

static
void swapExecBatchDoRIOBatch(swapExecBatch *exec_batch, RIOBatch *rios) {
    if (rios->count == 0) return;
    RIOBatchDo(rios);
    for (size_t i = 0; i < exec_batch->count; i++) {
        int errcode = 0;
        swapRequest *req = exec_batch->reqs[i];
        RIO *rio = rios->rios+i;
        if (!swapRequestGetError(req) && ((errcode = RIOGetError(rio)))) {
            swapRequestSetError(req,errcode);
        }
    }
}

/* Swap-thread: prepare robj to be merged.
 * - create new object: return newly created object.
 * - merge fields into robj: subvals merged into db.value, returns NULL */
inline void *swapDataCreateOrMergeObject(swapData *d, void *decoded,
        void *datactx) {
    if (d->type->createOrMergeObject)
        return d->type->createOrMergeObject(d,decoded,datactx);
    else
        return NULL;
}

/* Swap-thread: decode val/subval from rawvalss returned by rocksdb. */
inline int swapDataDecodeData(swapData *d, int num, int *cfs, sds *rawkeys,
        sds *rawvals, void **decoded) {
    if (d->type->decodeData)
        return d->type->decodeData(d,num,cfs,rawkeys,rawvals,decoded);
    else
        return 0;
}

/* Data */
#define SWAP_DATA_ABSENT_SUBKEYS_INIT 4
#define SWAP_DATA_ABSENT_SUBKEYS_LINEAR 1024

void swapDataAbsentSubkeyPush(swapDataAbsentSubkey *absent, sds subkey) {
    if (absent->count == absent->capacity) {
        if (absent->capacity >= SWAP_DATA_ABSENT_SUBKEYS_LINEAR)
            absent->capacity += SWAP_DATA_ABSENT_SUBKEYS_LINEAR;
        else
            absent->capacity *= 2;

        absent->subkeys = zrealloc(absent->subkeys,absent->capacity*sizeof(sds));
    }

    absent->subkeys[absent->count++] = subkey;
}

swapDataAbsentSubkey *swapDataAbsentSubkeyNew() {
    swapDataAbsentSubkey *absent = zmalloc(sizeof(swapDataAbsentSubkey));
    absent->subkeys = zmalloc(sizeof(sds)*SWAP_DATA_ABSENT_SUBKEYS_INIT);
    absent->count = 0;
    absent->capacity = SWAP_DATA_ABSENT_SUBKEYS_INIT;
    return absent;
}



/* Save absent subkeys when in swap thread, which will be merged into cold
 * filter when callback in main thread. */
void swapDataRetainAbsentSubkeys(swapData *data, int num, int *cfs,
        sds *rawkeys, sds *rawvals) {
    uint64_t version = swapDataObjectVersion(data);

    /* string dont have subkey */
    if (version <= 0) return;

    /* bitmap subkey is exclued out of this operation. */
    if (data->swap_type == SWAP_TYPE_STRING) return;

    for (int i = 0; i < num; i++) {
        int dbid;
        const char *keystr, *subkeystr;
        size_t klen, slen;
        uint64_t subkey_version;
        sds absent_subkey;

        if (cfs[i] != DATA_CF) continue;
        if (rawvals[i] != NULL) continue;
        if (rocksDecodeDataKey(rawkeys[i],sdslen(rawkeys[i]),
                    &dbid,&keystr,&klen,&subkey_version,&subkeystr,&slen) < 0) {
            continue;
        }
        if (subkey_version != version) continue;

        absent_subkey = sdsnewlen(subkeystr,slen);

        if (data->absent == NULL)
            data->absent = swapDataAbsentSubkeyNew();

        swapDataAbsentSubkeyPush(data->absent,absent_subkey);
    }
}


void swapExecBatchExecuteIn(swapExecBatch *exec_batch) {
    RIOBatch _rios = {0}, *rios = &_rios;
    int errcode, action = exec_batch->action;
    void *decoded;

    serverAssert(action == ROCKS_GET || action == ROCKS_ITERATE);

    RIOBatchInit(rios,action);
    swapExecBatchPrepareRIOBatch(exec_batch,rios);
    swapExecBatchDoRIOBatch(exec_batch,rios);

    for (size_t i = 0; i < exec_batch->count; i++) {
        swapRequest *req = exec_batch->reqs[i];
        RIO *rio = rios->rios+i;
        if (swapRequestGetError(req)) continue;
        if (action == ROCKS_GET) {
            if ((errcode = swapDataDecodeData(req->data,rio->get.numkeys,
                            rio->get.cfs,rio->get.rawkeys,rio->get.rawvals,
                            &decoded))) {
                swapRequestSetError(req,errcode);
                continue;
            }
            swapDataRetainAbsentSubkeys(req->data,rio->get.numkeys,
                    rio->get.cfs,rio->get.rawkeys,rio->get.rawvals);
        } else { /* ROCKS_ITERATE */
            int *tmpcfs = zmalloc(sizeof(int)*rio->iterate.numkeys);

            for (int i = 0; i < rio->iterate.numkeys; i++)
                tmpcfs[i] = rio->iterate.cf;

            if (rio->iterate.nextseek) {
                req->data->nextseek = rio->iterate.nextseek;
                rio->iterate.nextseek = NULL;
            }

            if ((errcode = swapDataDecodeData(req->data,rio->iterate.numkeys,
                            tmpcfs,rio->iterate.rawkeys,rio->iterate.rawvals,
                            &decoded))) {
                swapRequestSetError(req,errcode);
                zfree(tmpcfs);
                continue;
            }
            zfree(tmpcfs);
        }

        req->result = swapDataCreateOrMergeObject(req->data,decoded,req->datactx);
    }

    swapExecBatchExecuteIntentionDel(exec_batch,rios);
    swapExecBatchUpdateStatsRIOBatch(exec_batch,rios);
    RIOBatchDeinit(rios);
}

static void swapExecBatchExecuteDoOutMeta(swapExecBatch *exec_batch) {
    int errcode, *meta_cfs = NULL, num_metas = 0;
    RIO _meta_rio = {0}, *meta_rio = &_meta_rio;
    sds *meta_rawkeys = NULL, *meta_rawvals = NULL;
    size_t count = exec_batch->count;

    meta_cfs = zmalloc(sizeof(int)*count);
    meta_rawkeys = zmalloc(sizeof(sds)*count);
    meta_rawvals = zmalloc(sizeof(sds)*count);
    for (size_t i = 0; i < count; i++) {
        swapRequest *req = exec_batch->reqs[i];
        /* rdb out do not meta already encoded, can't put. */
        if (req->data->db == NULL || req->data->key == NULL) continue;
        meta_cfs[num_metas] = META_CF;
        meta_rawkeys[num_metas] = swapDataEncodeMetaKey(req->data);
        meta_rawvals[num_metas] = swapDataEncodeMetaVal(req->data,req->datactx);
        num_metas++;
    }
    RIOInitPut(meta_rio,num_metas,meta_cfs,meta_rawkeys,meta_rawvals);
    RIODo(meta_rio);
    if ((errcode = RIOGetError(meta_rio))) {
        swapExecBatchSetError(exec_batch,errcode);
    }
    RIODeinit(meta_rio);
}
/* Swap-thread: clean data.value. */
inline int swapDataCleanObject(swapData *d, void *datactx, int keep_data) {
    if (d->type->cleanObject)
        return d->type->cleanObject(d,datactx,keep_data);
    else
        return 0;
}

void swapExecBatchExecuteOut(swapExecBatch *exec_batch) {
    RIOBatch _rios = {0}, *rios = &_rios;
    serverAssert(exec_batch->action == ROCKS_PUT);
    RIOBatchInit(rios,ROCKS_PUT);
    swapExecBatchPrepareRIOBatch(exec_batch,rios);
    swapExecBatchDoRIOBatch(exec_batch,rios);
    for (size_t i = 0; i < exec_batch->count; i++) {
        int errcode = 0;
        swapRequest *req = exec_batch->reqs[i];
        if (swapRequestGetError(req)) continue;
        int keep_data = req->intention_flags & SWAP_EXEC_OUT_KEEP_DATA;
        if ((errcode = swapDataCleanObject(req->data,req->datactx,keep_data))) {
            swapRequestSetError(req,errcode);
            continue;
        }
    }
    swapExecBatchExecuteDoOutMeta(exec_batch);
    swapExecBatchUpdateStatsRIOBatch(exec_batch,rios);
    RIOBatchDeinit(rios);
}


static void swapExecBatchExecuteDoDelMeta(swapExecBatch *exec_batch) {
    int errcode, *meta_cfs, num_metas = 0;
    sds *meta_rawkeys = NULL;
    RIO _meta_rio = {0}, *meta_rio = &_meta_rio;
    size_t count = exec_batch->count;

    meta_cfs = zmalloc(sizeof(int)*count);
    meta_rawkeys = zmalloc(sizeof(sds)*count);
    for (size_t i = 0; i < count; i++) {
        swapRequest *req = exec_batch->reqs[i];
        meta_cfs[num_metas] = META_CF;
        meta_rawkeys[num_metas] = swapDataEncodeMetaKey(req->data);
        num_metas++;
    }

    RIOInitDel(meta_rio,num_metas,meta_cfs,meta_rawkeys);
    RIODo(meta_rio);
    if ((errcode = RIOGetError(meta_rio))) {
        swapExecBatchSetError(exec_batch,errcode);
    }
    RIODeinit(meta_rio);
}


void swapExecBatchExecuteDel(swapExecBatch *exec_batch) {
    RIOBatch _rios, *rios = &_rios;
    int action = exec_batch->action;
    serverAssert(action == ROCKS_DEL || action == ROCKS_NOP);
    if (action != ROCKS_NOP) {
        RIOBatchInit(rios,ROCKS_DEL);
        swapExecBatchPrepareRIOBatch(exec_batch,rios);
        swapExecBatchDoRIOBatch(exec_batch, rios);
        swapExecBatchUpdateStatsRIOBatch(exec_batch,rios);
        RIOBatchDeinit(rios);
    }
    swapExecBatchExecuteDoDelMeta(exec_batch);
}


void swapRequestExecuteUtil_CompactRange(swapRequest *req) {
    server.storage.engine->compact_range(server.storage.engine->context, req->finish_pd);
}

void swapRequestExecuteUtil_GetRocksdbStats(swapRequest* req) {
    req->result = server.storage.engine->get_stats(server.storage.engine->context, req->finish_pd);
}

void swapRequestExecuteUtil_RocksdbFlush(swapRequest* req) {
    server.storage.engine->flush(server.storage.engine->context, req->finish_pd);
}

void swapRequestExecuteUtil_CreateCheckpoint(swapRequest* req) {
    server.storage.engine->create_checkpoint(server.storage.engine->context, req->finish_pd);
}

void swapRequestExecuteUtil_CollectCfMeta(swapRequest* req) {
    server.storage.engine->collect_cf_meta(server.storage.engine->context, req->finish_pd);
}


void swapRequestExecuteUtil(swapRequest *req) {
    switch(req->intention_flags) {
    case ROCKSDB_COMPACT_RANGE_TASK:
        swapRequestExecuteUtil_CompactRange(req);
        break;
    case ROCKSDB_GET_STATS_TASK:
        swapRequestExecuteUtil_GetRocksdbStats(req);
        break;
    case ROCKSDB_FLUSH_TASK:
        swapRequestExecuteUtil_RocksdbFlush(req);
        break;
    case ROCKSDB_COLLECT_CF_META_TASK:
        swapRequestExecuteUtil_CollectCfMeta(req);
        break;
    case ROCKSDB_CREATE_CHECKPOINT:
        swapRequestExecuteUtil_CreateCheckpoint(req);
        break;
    default:
        swapRequestSetError(req,SWAP_ERR_EXEC_UNEXPECTED_UTIL);
        break;
    }
}

void swapExecBatchExecuteUtils(swapExecBatch *exec_batch) {
    for (size_t i = 0; i < exec_batch->count; i++) {
        swapRequest *req = exec_batch->reqs[i];
        serverAssert(req->intention == SWAP_UTILS);
        if (!swapRequestGetError(req)) {
            swapRequestExecuteUtil(req);
        }
    }
}

static inline void swapExecBatchExecuteEnd(swapExecBatch *exec_batch) {
    size_t swap_memory = 0;
    int intention = exec_batch->intention;
    const long duration = elapsedUs(exec_batch->swap_timer);

    for (size_t i = 0; i < exec_batch->count; i++) {
        swapRequest *req = exec_batch->reqs[i];
        swap_memory += req->swap_memory;
    }

    atomicIncr(server.storage.ror_stats->swap_stats[intention].batch,1);
    atomicIncr(server.storage.ror_stats->swap_stats[intention].count,exec_batch->count);
    atomicIncr(server.storage.ror_stats->swap_stats[intention].time,duration);
    atomicIncr(server.storage.ror_stats->swap_stats[intention].memory,swap_memory);
}


void swapExecBatchExecute(swapExecBatch *exec_batch) {
    serverAssert(exec_batch->intention != SWAP_NOP);
    serverAssert(!swapExecBatchGetError(exec_batch));

    swapExecBatchExecuteStart(exec_batch);

    switch (exec_batch->intention) {
    case SWAP_IN:
        swapExecBatchExecuteIn(exec_batch);
        break;
    case SWAP_OUT:
        swapExecBatchExecuteOut(exec_batch);
        if (server.storage.swap_debug_swapout_notify_delay_micro)
            usleep(server.storage.swap_debug_swapout_notify_delay_micro);
        break;
    case SWAP_DEL:
        swapExecBatchExecuteDel(exec_batch);
        break;
    case SWAP_UTILS:
        swapExecBatchExecuteUtils(exec_batch);
        break;
    default:
        swapExecBatchSetError(exec_batch,SWAP_ERR_EXEC_FAIL);
        serverLog(LL_WARNING,
                "unexpected execute batch intention(%d) action(%d)\n",
                exec_batch->intention, exec_batch->action);
        break;
    }

    swapExecBatchExecuteEnd(exec_batch);
}


static inline void swapExecBatchCtxExecuteIfNeeded(swapExecBatch *exec_ctx) {
    if (!swapExecBatchCtxEmpty(exec_ctx)) {
        /* exec batch and ctx are identical */
        swapExecBatch *exec_batch = exec_ctx;
        swapExecBatchExecute(exec_batch);
    }
}

static inline void swapExecBatchCtxReset(swapExecBatch *exec_batch, int intention, int action) {
    exec_batch->count = 0;
    exec_batch->intention = intention;
    exec_batch->action = action;
}
inline int swapDataSwapAnaAction(swapData *d, int intention, void *datactx, int *action) {
    if (d->type->swapAnaAction)
        return d->type->swapAnaAction(d, intention, datactx, action);
    else
        return 0;
}
void swapExecBatchCtxFeed(swapExecBatch *exec_ctx, swapRequest *req) {
    int req_action;
    serverAssert(req->intention != SWAP_UNSET);

    if (swapIntentionInOutDel(req->intention)) {
        swapDataSwapAnaAction(req->data,req->intention,req->datactx,
                &req_action);
    } else {
        req_action = ROCKS_NOP;
    }

    /* execute before append req if intention or action switched */
    if ((req->intention != exec_ctx->intention ||
            req_action != exec_ctx->action)) {
        swapExecBatchCtxExecuteIfNeeded(exec_ctx);
        swapExecBatchCtxReset(exec_ctx,req->intention,req_action);
    }

    swapExecBatchAppend(exec_ctx,req);

    if (!swapIntentionInOutDel(req->intention)) {
        swapExecBatchCtxExecuteIfNeeded(exec_ctx);
        swapExecBatchCtxReset(exec_ctx,SWAP_UNSET,ROCKS_UNSET);
    }
}

void swapExecBatchCtxEnd(swapExecBatch *exec_batch) {
    swapExecBatchCtxExecuteIfNeeded(exec_batch);
    swapExecBatchCtxReset(exec_batch,SWAP_UNSET,ROCKS_UNSET);
}
void swapRequestBatchExecute(swapRequestBatch *reqs) {
    swapExecBatch exec_ctx;

    swapExecBatchCtxInit(&exec_ctx);
    swapExecBatchCtxStart(&exec_ctx);
    for (size_t i = 0; i < reqs->count; i++) {
        swapRequest *req = reqs->reqs[i];
        if (!swapRequestGetError(req) && req->intention != SWAP_NOP) {
            swapExecBatchCtxFeed(&exec_ctx,req);
        }
    }
    swapExecBatchCtxEnd(&exec_ctx);
    swapExecBatchCtxDeinit(&exec_ctx);
}
void swapRequestBatchProcess(swapRequestBatch *reqs) {
    swapRequestBatchProcessStart(reqs);
    swapRequestBatchPreprocess(reqs);
    swapRequestBatchExecute(reqs);
    swapRequestBatchProcessEnd(reqs);
}

