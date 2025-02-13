/* Copyright (c) 2021, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "ctrip_swap.h"
#include "ctrip_swap_rordb.h"

void decodedResultInit(decodedResult *decoded) {
    memset(decoded,0,sizeof(decodedResult));
    decoded->cf = -1;
}

void decodedResultDeinit(decodedResult *decoded) {
    if (decoded->key) {
        sdsfree(decoded->key);
        decoded->key = NULL;
    }
    if (decoded->cf == META_CF) {
        decodedMeta *dm = (decodedMeta*)decoded;
        if (dm->extend) {
            sdsfree(dm->extend);
            dm->extend = NULL;
        }
    }
    if (decoded->cf == DATA_CF) {
        decodedData *dd = (decodedData*)decoded;
        if (dd->subkey) {
            sdsfree(dd->subkey);
            dd->subkey = NULL;
        }
        if (dd->rdbraw) {
            sdsfree(dd->rdbraw);
            dd->rdbraw = NULL;
        }
    }
    decodedResultInit(decoded);
}

/* ------------------------------ rdb save -------------------------------- */
/* Whole key encoding in rocksdb is the same as in rdb, so we skip encoding
 * and decoding to reduce cpu usage. */
int rdbSaveKeyHeader(rio *rdb, robj *key, robj *x, unsigned char rdbtype,
        long long expiretime) {
    int savelru = server.maxmemory_policy & MAXMEMORY_FLAG_LRU;
    int savelfu = server.maxmemory_policy & MAXMEMORY_FLAG_LFU;

    /* save expire/type/key */
    if (expiretime != -1) {
        if (rdbSaveType(rdb,RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
        if (rdbSaveMillisecondTime(rdb,expiretime) == -1) return -1;
    }

    /* Save the LRU info. */
    if (savelru) {
        uint64_t idletime = estimateObjectIdleTime(x);
        idletime /= 1000; /* Using seconds is enough and requires less space.*/
        if (rdbSaveType(rdb,RDB_OPCODE_IDLE) == -1) return -1;
        if (rdbSaveLen(rdb,idletime) == -1) return -1;
    }

    /* Save the LFU info. */
    if (savelfu) {
        uint8_t buf[1];
        buf[0] = LFUDecrAndReturn(x);
        /* We can encode this in exactly two bytes: the opcode and an 8
         * bit counter, since the frequency is logarithmic with a 0-255 range.
         * Note that we do not store the halving time because to reset it
         * a single time when loading does not affect the frequency much. */
        if (rdbSaveType(rdb,RDB_OPCODE_FREQ) == -1) return -1;
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
    }

    /* Save type, key, value */
    if (rdbSaveType(rdb,rdbtype) == -1) return -1;
    if (rdbSaveStringObject(rdb,key) == -1) return -1;
    return 1;
}

/* return -1 if save hot extension failed. */
int rdbKeySaveHotExtension(struct rdbKeySaveData *save, rio *rdb) {
    if (save->type->save_hot_ext)
        return save->type->save_hot_ext(save,rdb);
    else
        return 0;
}

/* return -1 if save start failed. */
int rdbKeySaveStart(struct rdbKeySaveData *save, rio *rdb) {
    if (save->type->save_start)
        return save->type->save_start(save,rdb);
    else
        return 0;
}

/* return -1 if save failed. */
int rdbKeySave(struct rdbKeySaveData *save, rio *rdb, decodedData *d) {
    uint64_t version;

    if (save->object_meta == NULL) {
        /* string version is always ZERO */
        version = SWAP_VERSION_ZERO;
    } else {
        version = save->object_meta->version;
    }
    /* skip obselete data key */
    if (version != d->version) return 0;

    if (save->type->save) {
        int ret = save->type->save(save,rdb,d);
        /* Delay return if required (for testing) */
        if (server.rdb_key_save_delay)
            debugDelay(server.rdb_key_save_delay);
        return ret;
    } else {
        return 0;
    }
}

/* return -1 if save_result is -1 or save end failed. */
int rdbKeySaveEnd(struct rdbKeySaveData *save, rio *rdb, int save_result) {
    if (save->type->save_end)
        return save->type->save_end(save,rdb,save_result);
    else
        return C_OK;
}

void rdbKeySaveDataDeinit(rdbKeySaveData *save) {
    if (save->key) {
        decrRefCount(save->key);
        save->key = NULL;
    }

    save->value = NULL;

    if (save->object_meta) {
        freeObjectMeta(save->object_meta);
        save->object_meta = NULL;
    }

    if (save->type->save_deinit)
        save->type->save_deinit(save);
}

sds rdbSaveRocksStatsDump(rdbSaveRocksStats *stats) {
    return sdscatprintf(sdsempty(),
            "init.ok=%lld,"
            "init.skip=%lld,"
            "init.err=%lld,"
            "save.ok=%lld",
            stats->init_save_ok,
            stats->init_save_skip,
            stats->init_save_err,
            stats->save_ok);
}

#define INIT_SAVE_OK 0
#define INIT_SAVE_ERR -1
#define INIT_SAVE_SKIP -2

static void rdbKeySaveDataInitCommon(rdbKeySaveData *save,
        MOVE robj *key, robj *value, long long expire, objectMeta *om) {
    save->key = key;
    save->value = value;
    save->expire = expire;
    save->object_meta = dupObjectMeta(om);
    save->saved = 0;
    save->iter = NULL;
}

static int rdbKeySaveDataInitHot(rdbKeySaveData *save, redisDb *db, robj *key, robj *value) {
    
    objectMeta *object_meta = lookupMeta(db,key);
    serverAssert(object_meta->swap_type == SWAP_TYPE_BITMAP);

    long long expire = getExpire(db,key);

    rdbKeySaveDataInitCommon(save,key,value,expire,object_meta);

    serverAssert(bitmapSaveInit(save,SWAP_VERSION_ZERO,NULL,0) == 0);
    return INIT_SAVE_OK;
}

static int rdbKeySaveDataInitWarm(rdbKeySaveData *save, redisDb *db,
        MOVE robj *key, robj *value) {
    int retval = INIT_SAVE_OK;
    objectMeta *object_meta = lookupMeta(db,key);
    long long expire = getExpire(db,key);

    serverAssert(value && !keyIsHot(object_meta,value));

    rdbKeySaveDataInitCommon(save,key,value,expire,object_meta);

    switch (object_meta->swap_type) {
    case SWAP_TYPE_STRING:
        wholeKeySaveInit(save);
        break;
    case SWAP_TYPE_HASH:
        hashSaveInit(save,SWAP_VERSION_ZERO,NULL,0);
        break;
    case SWAP_TYPE_SET:
        setSaveInit(save,SWAP_VERSION_ZERO,NULL,0);
        break;
    case SWAP_TYPE_LIST:
        listSaveInit(save,SWAP_VERSION_ZERO,NULL,0);
        break;
    case SWAP_TYPE_ZSET:
        zsetSaveInit(save,SWAP_VERSION_ZERO,NULL,0);
        break;
    case SWAP_TYPE_BITMAP:
        bitmapSaveInit(save,SWAP_VERSION_ZERO,NULL,0);
        break;
    default:
        retval = INIT_SAVE_ERR;
        break;
    }

    return retval;
}

static int rdbKeySaveDataInitCold(rdbKeySaveData *save, redisDb *db,
        MOVE robj *key, decodedMeta *dm) {
    int retval = INIT_SAVE_OK;
    UNUSED(db);

    rdbKeySaveDataInitCommon(save,key,NULL,dm->expire,NULL);

    switch (dm->swap_type) {
    case SWAP_TYPE_STRING:
        serverAssert(dm->extend == NULL);
        wholeKeySaveInit(save);
        break;
    case SWAP_TYPE_HASH:
        serverAssert(dm->extend != NULL);
        retval = hashSaveInit(save,dm->version,dm->extend,sdslen(dm->extend));
        break;
    case SWAP_TYPE_SET:
        serverAssert(dm->extend != NULL);
        retval = setSaveInit(save,dm->version,dm->extend,sdslen(dm->extend));
        break;
    case SWAP_TYPE_LIST:
        serverAssert(dm->extend != NULL);
        retval = listSaveInit(save,dm->version,dm->extend,sdslen(dm->extend));
        break;
    case SWAP_TYPE_ZSET:
        serverAssert(dm->extend != NULL);
        retval = zsetSaveInit(save,dm->version,dm->extend,sdslen(dm->extend));
        break;
    case SWAP_TYPE_BITMAP:
        serverAssert(dm->extend != NULL);
        retval = bitmapSaveInit(save,dm->version,dm->extend,sdslen(dm->extend));
        break;
    default:
        retval = INIT_SAVE_ERR;
        break;
    }

    return retval;
}

int rdbKeySaveHotExtensionInit(rdbKeySaveData *save, redisDb *db, sds keystr) {

    robj *value, *key;
    key = createStringObject(keystr, sdslen(keystr));
    value = lookupKey(db,key,LOOKUP_NOTOUCH);

    objectMeta *object_meta = lookupMeta(db,key);

    serverAssert(keyIsHot(object_meta,value));

    return rdbKeySaveDataInitHot(save,db,key,value);
}

int rdbKeySaveWarmColdInit(rdbKeySaveData *save, redisDb *db, decodedResult *dr) {
    robj *value, *key;
    objectMeta *object_meta;
    serverAssert(db->id == dr->dbid);

    if (dr->cf != META_CF) {
        /* skip orphan (sub)data keys: note that meta key is prefix of data
         * subkey, so rocksIter always start init with meta key, except for
         * orphan (sub)data key. */
#ifdef ROCKS_DEBUG
        serverLog(LL_WARNING,"rdbKeySaveWarmColdInit: key(%s) not meta, skipped",dr->key);
#endif
        return INIT_SAVE_SKIP;
    }

    key = createStringObject(dr->key, sdslen(dr->key));
    value = lookupKey(db,key,LOOKUP_NOTOUCH);
    object_meta = lookupMeta(db,key);

    if (keyIsHot(object_meta,value)) { /* hot */
#ifdef ROCKS_DEBUG
        serverLog(LL_WARNING,"rdbKeySaveWarmColdInit: key(%s) is hot, skipped",dr->key);
#endif
        decrRefCount(key);
        return INIT_SAVE_SKIP;
    } else if (value) { /* warm */
        return rdbKeySaveDataInitWarm(save,db,key,value);
    } else  { /* cold */
        serverAssert(dr->cf == META_CF);
        return rdbKeySaveDataInitCold(save,db,key,(decodedMeta*)dr);
    }
}

int swapRdbSaveHotExtension(rio *rdb, int *error, redisDb *db, swapRdbSaveCtx *ctx)
{
    long long save_ok = 0;
    sds errstr = NULL;

    listIter li;
    listNode *ln;
    listRewind(ctx->hot_keys_extension, &li);
    while ((ln = listNext(&li))) {
        rdbKeySaveData _save, *save = &_save;

        sds keystr = listNodeValue(ln);

        int init_result = rdbKeySaveHotExtensionInit(save,db,keystr);
        serverAssert(init_result == INIT_SAVE_OK);

        int save_res = rdbKeySaveHotExtension(save,rdb);
        if (save_res == -1) {
            errstr = sdscatfmt(sdsempty(),"Save key(%S) failed: %s",
                    keystr, strerror(errno));
            rdbKeySaveDataDeinit(save);
            goto err; /* IO error. */
        }

        if (server.swap_debug_rdb_key_save_delay_micro)
            debugDelay(server.swap_debug_rdb_key_save_delay_micro);

        save_ok++;

        rdbKeySaveDataDeinit(save);
        swapRdbSaveProgress(rdb,ctx);
    }

    serverLog(LL_NOTICE,
            "Save hot key extension to rdb finished: save.ok=%lld",save_ok);
    return C_OK;

err:
    if (error && *error == 0) *error = errno;
    serverLog(LL_WARNING, "Save hot key extension to rdb failed: %s", errstr);

    if (errstr) sdsfree(errstr);
    return C_ERR;
}

/* warm or cold key, which are saved here.
 * Bighash/set/zset... fields are located adjacent, and will be iterated
 * next to each.
 * Note that only IO error aborts rdbSaveRocks, keys with decode/init_save
 * errors are skipped. */
int swapRdbSaveRocks(rio *rdb, int *error, redisDb *db, swapRdbSaveCtx *ctx) {
    rocksIter *it = NULL;
    sds errstr = NULL;
    int recoverable_err = 0;
    rocksIterDecodeStats _iter_stats = {0}, *iter_stats = &_iter_stats;
    rdbSaveRocksStats _stats = {0}, *stats = &_stats;
    decodedResult  _cur, *cur = &_cur, _next, *next = &_next;
    decodedResultInit(cur);
    decodedResultInit(next);
    int iter_valid; /* true if current iter value is valid. */

    rocks *rocks = serverRocksGetReadLock();
    if (!(it = rocksCreateIter(rocks,db))) {
        serverLog(LL_WARNING, "Create rocks iterator failed.");
        serverRocksUnlock(rocks);
        return C_ERR;
    }

    iter_valid = rocksIterSeekToFirst(it);

    while (1) {
        int init_result, decode_result, save_result = 0;
        rdbKeySaveData _save, *save = &_save;
        serverAssert(next->key == NULL);

        if (cur->key == NULL) {
            if (!iter_valid) break;

            decode_result = rocksIterDecode(it,cur,iter_stats);
            iter_valid = rocksIterNext(it);

            if (decode_result) continue;

            serverAssert(cur->key != NULL);
        }

        init_result = rdbKeySaveWarmColdInit(save,db,cur);
        if (init_result == INIT_SAVE_SKIP) {
            stats->init_save_skip++;
            decodedResultDeinit(cur);
            continue;
        } else if (init_result == INIT_SAVE_ERR) {
            if (stats->init_save_err++ < 10) {
                sds repr = sdscatrepr(sdsempty(),cur->key,sdslen(cur->key));
                serverLog(LL_WARNING, "Init rdb save key failed: %s", repr);
                sdsfree(repr);
            }
            decodedResultDeinit(cur);
            continue;
        } else {
            stats->init_save_ok++;
        }

        if (rdbKeySaveStart(save,rdb) == -1) {
            errstr = sdscatfmt(sdsempty(),"Save key(%S) start failed: %s",
                    cur->key, strerror(errno));
            rdbKeySaveDataDeinit(save);
            decodedResultDeinit(cur);
            goto err; /* IO error, can't recover. */
        }

        /* There may be no rocks-meta for warm/hot hash(set/zset...), in
         * which case cur is decodedData. note that rdbKeySaveWarmColdInit only
         * consumes decodedMeta. */
        if (cur->cf == DATA_CF) {
            if ((save_result = rdbKeySave(save,rdb,(decodedData*)cur)) == -1) {
                sds repr = sdscatrepr(sdsempty(),cur->key,sdslen(cur->key));
                errstr = sdscatfmt("Save key (%S) failed: %s", repr,
                        strerror(errno));
                sdsfree(repr);
                decodedResultDeinit(cur);
                goto saveend;
            }
        }

        while (1) {
            int key_switch;

            /* Iterate untill next valid rawkey found or eof. */
            while (1) {
                if (!iter_valid) break; /* eof */

                decode_result = rocksIterDecode(it,next,iter_stats);
                iter_valid = rocksIterNext(it);

                if (decode_result) {
                    continue;
                } else { /* next found */
                    serverAssert(next->key != NULL);
                    break;
                }
            }

            /* Can't find next valid rawkey, break to finish saving cur key.*/
            if (next->key == NULL) {
                decodedResultDeinit(cur);
                break;
            }

            serverAssert(cur->key && next->key);
            key_switch = sdslen(cur->key) != sdslen(next->key) ||
                    sdscmp(cur->key,next->key);

            decodedResultDeinit(cur);
            _cur = _next;
            decodedResultInit(next);

            /* key switched, finish current & start another. */
            if (key_switch) break;

            /* key not switched, continue rdbSave. */
            if ((save_result = rdbKeySave(save,rdb,(decodedData*)cur)) == -1) {
                sds repr = sdscatrepr(sdsempty(),cur->key,sdslen(cur->key));
                errstr = sdscatfmt("Save key (%S) failed: %s", repr,
                        strerror(errno));
                sdsfree(repr);
                decodedResultDeinit(cur);
                break;
            }
        }

saveend:
        /* call save_end if save_start called, no matter error or not. */
        save_result = rdbKeySaveEnd(save,rdb,save_result);
        if (server.swap_debug_rdb_key_save_delay_micro)
            debugDelay(server.swap_debug_rdb_key_save_delay_micro);
        if (save_result == SAVE_ERR_NONE) {
            stats->save_ok++;
        } else if (server.swap_bgsave_fix_metalen_mismatch && save_result > SAVE_ERR_NONE && save_result < SAVE_ERR_UNRECOVERABLE) {
            /* try to fix err while swap_bgsave_robust set */
            sendSwapChildErr(save_result, db->id, save->key->ptr);
            recoverable_err++;
        } else {
            if (errstr == NULL) {
                errstr = sdscatfmt(sdsempty(),"Save key end failed: %s",
                        strerror(errno));
            }
            rdbKeySaveDataDeinit(save);
            goto err;
        }

        rdbKeySaveDataDeinit(save);
        swapRdbSaveProgress(rdb,ctx);
    };

    if (recoverable_err > 0) {
        if (errstr == NULL) {
            errstr = sdscatfmt(sdsempty(),"recoverable err: %i, try later",recoverable_err);
        }
        goto err;
    }

    sds iter_stats_dump = rocksIterDecodeStatsDump(iter_stats);
    sds stats_dump = rdbSaveRocksStatsDump(stats);
    serverLog(LL_NOTICE,
            "Rdb save keys from rocksdb finished: iter=(%s), save=(%s)",
            iter_stats_dump,stats_dump);
    sdsfree(iter_stats_dump);
    sdsfree(stats_dump);

    if (it) rocksReleaseIter(it);
    serverRocksUnlock(rocks);

    return C_OK;

err:
    if (error && *error == 0) *error = errno;
    serverLog(LL_WARNING, "Save rocks data to rdb failed: %s", errstr);
    if (it) rocksReleaseIter(it);
    if (errstr) sdsfree(errstr);
    serverRocksUnlock(rocks);
    return C_ERR;
}

/* ------------------------------ rdb load -------------------------------- */
typedef struct rdbLoadSwapData {
    swapData d;
    redisDb *db;
    size_t idx;
    int num;
    int *cfs;
    sds *rawkeys;
    sds *rawvals;
#ifdef SWAP_DEBUG
  struct swapDebugMsgs msgs;
#endif
} rdbLoadSwapData;

void rdbLoadSwapDataFree(swapData *data_, void *datactx) {
    rdbLoadSwapData *data = (rdbLoadSwapData*)data_;
    UNUSED(datactx);
    for (int i = 0; i < data->num; i++) {
        sdsfree(data->rawkeys[i]);
        sdsfree(data->rawvals[i]);
    }
    zfree(data->cfs);
    zfree(data->rawkeys);
    zfree(data->rawvals);
    zfree(data);
}

int rdbLoadSwapAna(swapData *data, int thd, struct keyRequest *req,
        int *intention, uint32_t *intention_flags, void *datactx) {
    UNUSED(data), UNUSED(thd), UNUSED(req), UNUSED(intention_flags), UNUSED(datactx);
    *intention = SWAP_OUT;
    *intention_flags = 0;
    return 0;
}

int rdbLoadSwapAnaAction(swapData *data, int intention, void *datactx, int *action) {
    UNUSED(data), UNUSED(intention), UNUSED(datactx);
    *action = ROCKS_PUT;
    return 0;
}

int rdbLoadEncodeData(swapData *data_, int intention, void *datactx,
        int *numkeys, int **pcfs, sds **prawkeys, sds **prawvals) {
    rdbLoadSwapData *data = (rdbLoadSwapData*)data_;
    UNUSED(intention), UNUSED(datactx);
    *numkeys = data->num;
    *pcfs = data->cfs;
    *prawkeys = data->rawkeys;
    *prawvals = data->rawvals;
    data->num = 0;
    data->cfs = NULL;
    data->rawkeys = NULL;
    data->rawvals = NULL;
    return 0;
}

swapDataType rdbLoadSwapDataType = {
    .name = "rdbload",
    .swapAna = rdbLoadSwapAna,
    .swapAnaAction = rdbLoadSwapAnaAction,
    .encodeKeys = NULL,
    .encodeData = rdbLoadEncodeData,
    .encodeRange = NULL,
    .decodeData = NULL,
    .swapIn =  NULL,
    .swapOut = NULL,
    .swapDel = NULL,
    .createOrMergeObject = NULL,
    .cleanObject = NULL,
    .beforeCall = NULL,
    .free = rdbLoadSwapDataFree,
};

/* rawkeys/rawvals moved, ptr array copied. */
swapData *createRdbLoadSwapData(size_t idx, int num, int *cfs, sds *rawkeys, sds *rawvals) {
    rdbLoadSwapData *data = zcalloc(sizeof(rdbLoadSwapData));
    data->d.type = &rdbLoadSwapDataType;
    data->idx = idx;
    data->num = num;
    data->cfs = zmalloc(sizeof(int)*data->num);
    memcpy(data->cfs,cfs,sizeof(int)*data->num);
    data->rawkeys = zmalloc(sizeof(sds)*data->num);
    memcpy(data->rawkeys,rawkeys,sizeof(sds)*data->num);
    data->rawvals = zmalloc(sizeof(sds)*data->num);
    memcpy(data->rawvals,rawvals,sizeof(sds)*data->num);
#ifdef SWAP_DEBUG
    char identity[MAX_MSG];
    snprintf(identity,MAX_MSG,"[rdbload:%ld:%d]",idx,num);
    swapDebugMsgsInit(&data->msgs,identity);
#endif
    return (swapData*)data;
}

swapRdbLoadObjectCtx *swapRdbLoadObjectCtxNew() {
    swapRdbLoadObjectCtx *ctx = zmalloc(sizeof(swapRdbLoadObjectCtx));
    ctx->errors = 0;
    ctx->idx = 0;
    ctx->batch.count = RDB_LOAD_BATCH_COUNT;
    ctx->batch.index = 0;
    ctx->batch.capacity = RDB_LOAD_BATCH_CAPACITY;
    ctx->batch.memory = 0;
    ctx->batch.cfs = zmalloc(sizeof(int)*ctx->batch.count);
    ctx->batch.rawkeys = zmalloc(sizeof(sds)*ctx->batch.count);
    ctx->batch.rawvals = zmalloc(sizeof(sds)*ctx->batch.count);
    return ctx;
}

void swapRdbLoadObjectWriteFinished(swapData *data, void *pd, int errcode) {
    UNUSED(pd), UNUSED(errcode);
#ifdef SWAP_DEBUG
    void *msgs = &((rdbLoadSwapData*)data)->msgs;
    DEBUG_MSGS_APPEND(msgs,"request-finish","ok");
#endif
    rdbLoadSwapDataFree(data,NULL);
}

void swapRdbLoadObjectSendBatch(swapRdbLoadObjectCtx *ctx) {
    swapData *data;
    void *msgs = NULL;

    if (ctx->batch.index == 0)
        return;

    data = createRdbLoadSwapData(ctx->idx,ctx->batch.index,
            ctx->batch.cfs,ctx->batch.rawkeys,ctx->batch.rawvals);

#ifdef SWAP_DEBUG
    msgs = &((rdbLoadSwapData*)data)->msgs;
#endif

    DEBUG_MSGS_APPEND(msgs,"request-start","idx=%ld,num=%ld",ctx->idx,
            ctx->batch.index);

    /* Submit to rio thread. */
    swapRequest *req = swapDataRequestNew(SWAP_OUT,0,NULL,data,NULL,NULL,
            swapRdbLoadObjectWriteFinished,NULL,msgs);
    submitSwapRequest(SWAP_MODE_PARALLEL_SYNC,req,-1);
}

void swapRdbLoadObjectCtxFeed(swapRdbLoadObjectCtx *ctx, int cf, MOVE sds rawkey, MOVE sds rawval) {
    ctx->batch.cfs[ctx->batch.index] = cf;
    ctx->batch.rawkeys[ctx->batch.index] = rawkey;
    ctx->batch.rawvals[ctx->batch.index] = rawval;
    ctx->batch.index++;
    ctx->batch.memory = ctx->batch.memory + sdslen(rawkey) + sdslen(rawval);

    if (ctx->batch.index >= ctx->batch.count ||
            ctx->batch.memory >= ctx->batch.capacity) {
        swapRdbLoadObjectSendBatch(ctx);
        /* Reset batch state */
        ctx->batch.index = 0;
        ctx->batch.memory = 0;
    }
}

void swapRdbLoadObjectCtxFree(swapRdbLoadObjectCtx *ctx) {
    zfree(ctx->batch.cfs);
    zfree(ctx->batch.rawkeys);
    zfree(ctx->batch.rawvals);
    zfree(ctx);
}

void swapStartLoading() {
    server.swap_rdb_load_object_ctx = swapRdbLoadObjectCtxNew();
}

void swapStopLoading(int success) {
    UNUSED(success);
    /* send last buffered batch. */
    swapRdbLoadObjectSendBatch(server.swap_rdb_load_object_ctx);
    asyncCompleteQueueDrain(-1); /* CONFIRM */
    parallelSyncDrain();
    swapRdbLoadObjectCtxFree(server.swap_rdb_load_object_ctx);
    server.swap_rdb_load_object_ctx = NULL;
}

/* ------------------------------ rdb load start -------------------------------- */
void rdbLoadStartLenMeta(struct rdbKeyLoadData *load, rio *rdb, int *cf,
                    sds *rawkey, sds *rawval, int *error) {
    int isencode;
    unsigned long long len;
    sds header, extend = NULL;

    header = rdbVerbatimNew((unsigned char)load->rdbtype);

    /* nfield */
    if (rdbLoadLenVerbatim(rdb,&header,&isencode,&len)) {
        sdsfree(header);
        *error = RDB_LOAD_ERR_OTHER;
        return;
    }

    if (len == 0) {
        sdsfree(header);
        *error = RDB_LOAD_ERR_EMPTY_KEY;
        return;
    }

    load->total_fields = len;
    extend = rocksEncodeObjectMetaLen(len);

    *cf = META_CF;
    *rawkey = rocksEncodeMetaKey(load->db,load->key);
    *rawval = rocksEncodeMetaVal(load->swap_type,load->expire,load->version,extend);
    *error = 0;

    sdsfree(extend);
    sdsfree(header);
}

/* ------------------------------ rdb verbatim -------------------------------- */
int rdbLoadIntegerVerbatim(rio *rdb, sds *verbatim, int enctype, long long *val) {
    unsigned char enc[4];

    if (enctype == RDB_ENC_INT8) {
        if (rioRead(rdb,enc,1) == 0) return -1;
        *val = (signed char)enc[0];
        *verbatim = sdscatlen(*verbatim,enc,1);
    } else if (enctype == RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(rdb,enc,2) == 0) return -1;
        v = enc[0]|(enc[1]<<8);
        *val = (int16_t)v;
        *verbatim = sdscatlen(*verbatim,enc,2);
    } else if (enctype == RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(rdb,enc,4) == 0) return -1;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        *val = (int32_t)v;
        *verbatim = sdscatlen(*verbatim,enc,4);
    } else {
        return -1; /* Never reached. */
    }
    return 0;
}

int rdbLoadLenVerbatim(rio *rdb, sds *verbatim, int *isencoded, unsigned long long *lenptr) {
    unsigned char buf[2];
    int type;

    if (isencoded) *isencoded = 0;
    if (rioRead(rdb,buf,1) == 0) return -1;
    type = (buf[0]&0xC0)>>6;
    if (type == RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        if (isencoded) *isencoded = 1;
        *lenptr = buf[0]&0x3F;
        *verbatim = sdscatlen(*verbatim,buf,1);
    } else if (type == RDB_6BITLEN) {
        /* Read a 6 bit len. */
        *lenptr = buf[0]&0x3F;
        *verbatim = sdscatlen(*verbatim,buf,1);
    } else if (type == RDB_14BITLEN) {
        /* Read a 14 bit len. */
        if (rioRead(rdb,buf+1,1) == 0) return -1;
        *lenptr = ((buf[0]&0x3F)<<8)|buf[1];
        *verbatim = sdscatlen(*verbatim,buf,2);
    } else if (buf[0] == RDB_32BITLEN) {
        /* Read a 32 bit len. */
        uint32_t len;
        if (rioRead(rdb,&len,4) == 0) return -1;
        *lenptr = ntohl(len);
        *verbatim = sdscatlen(*verbatim,buf,1);
        *verbatim = sdscatlen(*verbatim,&len,sizeof(len));
    } else if (buf[0] == RDB_64BITLEN) {
        /* Read a 64 bit len. */
        uint64_t len;
        if (rioRead(rdb,&len,8) == 0) return -1;
        *lenptr = ntohu64(len);
        *verbatim = sdscatlen(*verbatim,buf,1);
        *verbatim = sdscatlen(*verbatim,&len,sizeof(len));
    } else {
        return -1; /* Never reached. */
    }

    return 0;
}

int rdbLoadRawVerbatim(rio *rdb, sds *verbatim, unsigned long long len) {
    size_t oldlen = sdslen(*verbatim);
    *verbatim = sdsMakeRoomForExact(*verbatim, len);
    rioRead(rdb, *verbatim+oldlen, len);
    sdsIncrLen(*verbatim, len);
    return 0;
}

int rdbLoadLzfStringVerbatim(rio *rdb, sds *verbatim) {
    unsigned long long len, clen;
    int isencode;
    if ((rdbLoadLenVerbatim(rdb,verbatim,&isencode,&clen))) return -1;
    if ((rdbLoadLenVerbatim(rdb,verbatim,&isencode,&len))) return -1;
    if ((rdbLoadRawVerbatim(rdb,verbatim,clen))) return -1;
    return 0;
}

int rdbLoadStringVerbatim(rio *rdb, sds *verbatim) {
    int isencoded, retval;
    unsigned long long len;
    long long val;

     if ((retval = rdbLoadLenVerbatim(rdb,verbatim,&isencoded,&len)))
         return retval;

     if (isencoded) {
        switch(len) {
        case RDB_ENC_INT8:
        case RDB_ENC_INT16:
        case RDB_ENC_INT32:
            return rdbLoadIntegerVerbatim(rdb,verbatim,len,&val);
        case RDB_ENC_LZF:
            return rdbLoadLzfStringVerbatim(rdb,verbatim);
        default:
            return -1;
        }
     } else {
         return rdbLoadRawVerbatim(rdb,verbatim,len);
     }

     return 0;
}

int rdbLoadHashFieldsVerbatim(rio *rdb, unsigned long long len, sds *verbatim) {
    while (len--) {
        if (rdbLoadStringVerbatim(rdb,verbatim)) return -1; /* field */
        if (rdbLoadStringVerbatim(rdb,verbatim)) return -1; /* value */
    }
    return 0;
}

/* return 1 if load not load finished (needs to continue load). */
int rdbKeyLoad(struct rdbKeyLoadData *load, rio *rdb, int *cf, sds *rawkey,
        sds *rawval, int *error) {
    if (load->type->load)
        return load->type->load(load,rdb,cf,rawkey,rawval,error);
    else
        return 0;
}

void rdbKeyLoadStart(struct rdbKeyLoadData *load, rio *rdb, int *cf,
        sds *rawkey, sds *rawval, int *error) {
    if (load->type->load_start)
        load->type->load_start(load,rdb,cf,rawkey,rawval,error);
}

int rdbKeyLoadEnd(struct rdbKeyLoadData *load, rio *rdb) {
    if (load->type->load_end)
        return load->type->load_end(load,rdb);
    else
        return C_OK;
}

void rdbKeyLoadDataDeinit(struct rdbKeyLoadData *load) {
    if (load->type && load->type->load_deinit)
        load->type->load_deinit(load);
}

int rdbKeyLoadDataInit(rdbKeyLoadData *load, int rdbtype,
        redisDb *db, sds key, long long expire, long long now) {
    int retval = 0;
    if (!rdbIsObjectType(rdbtype)) return RDB_LOAD_ERR_OTHER;
    memset(load,0,sizeof(rdbKeyLoadData));

    load->rdbtype = rdbtype;
    load->db = db;
    load->key = key;
    load->expire = expire;
    load->now = now;
    load->value = NULL;
    load->iter = NULL;
    load->version = swapGetAndIncrVersion();
    load->load_info = NULL;

    switch(rdbtype) {
    case RDB_TYPE_STRING:
        wholeKeyLoadInit(load);
        break;
    case RDB_TYPE_HASH:
    case RDB_TYPE_HASH_ZIPMAP:
    case RDB_TYPE_HASH_ZIPLIST:
        hashLoadInit(load);
        break;
    case RDB_TYPE_SET:
    case RDB_TYPE_SET_INTSET:
        setLoadInit(load);
        break;
    case RDB_TYPE_LIST:
    case RDB_TYPE_LIST_ZIPLIST:
    case RDB_TYPE_LIST_QUICKLIST:
        listLoadInit(load);
        break;
    case RDB_TYPE_ZSET:
    case RDB_TYPE_ZSET_2:
    case RDB_TYPE_ZSET_ZIPLIST:
        zsetLoadInit(load);
        break;
    case RDB_TYPE_BITMAP:
        bitmapLoadInit(load);
        break;
    default:
        retval = SWAP_ERR_RDB_LOAD_UNSUPPORTED;
        break;
    }
    return retval;
}

int swapRdbLoadObject(int rdbtype, rio *rdb, redisDb *db, sds key,
        long long expiretime, long long now, rdbKeyLoadData *load) {
    int error = 0, cont, cf;
    sds rawkey = NULL, rawval = NULL;

    if ((error = rdbKeyLoadDataInit(load,rdbtype,db,key,
                    expiretime,now))) {
        return error;
    }
    rdbKeyLoadStart(load,rdb,&cf,&rawkey,&rawval,&error);
    if (error) return error;
    if (rawkey) {
        swapRdbLoadObjectCtxFeed(server.swap_rdb_load_object_ctx,cf,rawkey,rawval);
        load->nfeeds++;
    }

    do {
        cont = rdbKeyLoad(load,rdb,&cf,&rawkey,&rawval,&error);
        if (!error && rawkey) {
            serverAssert(rawval);
            swapRdbLoadObjectCtxFeed(server.swap_rdb_load_object_ctx,cf,rawkey,rawval);
            load->nfeeds++;
        }
    } while (!error && cont);

    if (!error) error = rdbKeyLoadEnd(load,rdb);
    return error;
}

void _rdbSaveBackground(client *c, swapCtx *ctx) {
    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    rdbSaveInfoSetSfrctx(rsiptr,swapForkRocksdbCtxCreate(SWAP_FORK_ROCKSDB_TYPE_SNAPSHOT));
    rdbSaveBackground(server.rdb_filename,rsiptr);
    server.swap_req_submitted &= ~REQ_SUBMITTED_BGSAVE;
    clientReleaseLocks(c,ctx);
}

void rdbSaveInfoSetRordb(rdbSaveInfo *rsiptr, int rordb) {
    rsiptr->rordb = rordb;
}

int rdbSaveInfoSetSfrctx(rdbSaveInfo *rsiptr, swapForkRocksdbCtx *sfrctx) {
    rsiptr->sfrctx = sfrctx;
    return 1;
}

void swapRdbSaveCtxInit(swapRdbSaveCtx *ctx, int rdbflags, int rordb) {
    ctx->key_count = 0;
    ctx->processed = 0;
    ctx->info_updated_time = 0;
    ctx->hot_keys_extension = NULL;
    ctx->rehash_paused_db = NULL;
    ctx->rdbflags = rdbflags;
    ctx->rordb = rordb;
}

void swapRdbSaveCtxDeinit(swapRdbSaveCtx *ctx) {
    if (ctx->rehash_paused_db) {
        dbResumeRehash(ctx->rehash_paused_db);
        ctx->rehash_paused_db = NULL;
    }
    if (ctx->hot_keys_extension) {
        listRelease(ctx->hot_keys_extension);
        ctx->hot_keys_extension = NULL;
    }
}

int swapRdbSaveStart(rio *rdb, swapRdbSaveCtx *ctx) {
    if (ctx->rordb && rordbSaveAuxFields(rdb) == -1) goto werr;
    if (ctx->rordb && rordbSaveSST(rdb) == -1) goto werr;
werr:
    return -1;
}

void swapRdbSaveBeginDb(rio *rdb, redisDb *db, swapRdbSaveCtx *ctx) {
    UNUSED(rdb);
    ctx->hot_keys_extension = listCreate();
    ctx->rehash_paused_db = db;
    dbPauseRehash(db);
}

static inline int swapShouldSaveByRor(objectMeta *meta, robj *o) {
    return !keyIsHot(meta, o) || (meta != NULL && meta->swap_type == SWAP_TYPE_BITMAP);
}

static inline int swapShouldByRorAsHotExtention(objectMeta *meta, robj *o) {
    return meta != NULL && meta->swap_type == SWAP_TYPE_BITMAP && keyIsHot(meta, o);
}

/* Returns:
 * NOP  if we should proceed redisSaveKeyValuePair (typically pure hot key).
 * SUCC if we should skip redisSaveKeyValuePair (typically cold or warm key,
 *    which should be saved later by ror.
 * FAIL if write error; */
int swapRdbSaveKeyValuePair(rio *rdb, redisDb *db, robj *key, robj *o,
        swapRdbSaveCtx *ctx) {
    objectMeta *meta = lookupMeta(db,key);
    if (!ctx->rordb) {
        /* cold or warm key, will be saved later in rdbSaveRocks.
           Hot bitmap will be saved later in rdbSaveHotExtension. */
        if (swapShouldSaveByRor(meta, o)) {
            if (swapShouldByRorAsHotExtention(meta,o))
                listAddNodeTail(ctx->hot_keys_extension, key->ptr);
            return SWAP_SAVE_SUCC;
        } else {
            return SWAP_SAVE_NOP;
        }
    } else {
        /* Additional swap related object flags(e.g. dirty_meta...) */
        if (rordbSaveObjectFlags(rdb,o) == -1) return SWAP_SAVE_FAIL;
        /* Original key value pair still should be saved by redis */
        return SWAP_SAVE_NOP;
    }
}

void swapRdbSaveProgress(rio *rdb, swapRdbSaveCtx *ctx) {
    char *pname = (ctx->rdbflags & RDBFLAGS_AOF_PREAMBLE) ? "AOF rewrite" :  "RDB";

    /* When this RDB is produced as part of an AOF rewrite, move
     * accumulated diff from parent to child while rewriting in
     * order to have a smaller final write. */
    if (ctx->rdbflags & RDBFLAGS_AOF_PREAMBLE &&
            rdb->processed_bytes > ctx->processed+AOF_READ_DIFF_INTERVAL_BYTES)
    {
        ctx->processed = rdb->processed_bytes;
        aofReadDiffFromParent();
    }

    /* Update child info every 1 second (approximately).
     * in order to avoid calling mstime() on each iteration, we will
     * check the diff every 1024 keys */
    if ((ctx->key_count++ & 1023) == 0) {
        long long now = mstime();
        if (now - ctx->info_updated_time >= 1000) {
            sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, ctx->key_count, pname);
            ctx->info_updated_time = now;
        }
    }
}

int swapRdbSaveDb(rio *rdb, int *error, redisDb *db, swapRdbSaveCtx *ctx) {
    if (ctx->rordb) {
        if (rordbSaveDbRio(rdb,db) == -1) return -1;
    } else {
        if (swapRdbSaveHotExtension(rdb,error,db,ctx)) return -1;
        if (swapRdbSaveRocks(rdb,error,db,ctx)) return -1;
    }
    return 0;
}

void swapRdbSaveEndDb(rio *rdb, redisDb *db, swapRdbSaveCtx *ctx) {
    UNUSED(rdb);
    serverAssert(ctx->rehash_paused_db == db);
    dbResumeRehash(db);
    ctx->rehash_paused_db = NULL;
    serverAssert(ctx->hot_keys_extension != NULL);
    listRelease(ctx->hot_keys_extension);
    ctx->hot_keys_extension = NULL;
}

int swapRdbSaveStop(rio *rdb, swapRdbSaveCtx *ctx) {
    UNUSED(rdb);
    UNUSED(ctx);
    if (!ctx->rordb) {
        server.swap_lastsave = time(NULL);
        server.swap_rdb_size = rdb->processed_bytes;
    }
    return 0;
}

int RDB_LOAD_OBJECT_SKIP_EMPTY_CHECK = 0;

robj *SWAP_RDB_LOAD_MOCK_VALUE = (robj*)"SWAP_RDB_LOAD_MOCK_VALUE";

void swapRdbLoadCtxInit(swapRdbLoadCtx *ctx) {
    ctx->reopen_filter = 0;
    ctx->rordb = 0;
    ctx->rordb_sstloaded = 0;
    ctx->rordb_object_flags = -1;
}

void swapRdbLoadBegin(swapRdbLoadCtx *ctx) {
    if (getFilterState() == FILTER_STATE_OPEN) {
        setFilterState(FILTER_STATE_CLOSE);
        ctx->reopen_filter = 1;
    }
}

int swapRdbLoadAuxField(swapRdbLoadCtx *ctx, rio *rdb, robj *auxkey,
        robj *auxval) {
    if (rordbLoadAuxFields(auxkey, auxval)) {
        ctx->rordb = 1;
        if (rordbLoadSSTStart(rdb)) return SWAP_LOAD_FAIL;
        serverLog(LL_NOTICE, "[rordb] loading in rordb mode.");
        return SWAP_LOAD_SUCC;
    }
    return SWAP_LOAD_UNRECOGNIZED;
}

int swapRdbLoadNoKV(swapRdbLoadCtx *ctx, rio *rdb, redisDb *db, int type) {
    if (ctx->rordb && rordbOpcodeIsValid(type)) {
        if (rordbOpcodeIsSSTType(type)) {
            if (rordbLoadSSTType(rdb,type)) return SWAP_LOAD_FAIL;
        } else if (rordbOpcodeIsDbType(type)) {
            if (rordbLoadDbType(rdb,db,type)) return SWAP_LOAD_FAIL;
        } else if (rordbOpcodeIsObjectFlags(type)) {
            uint8_t byte;
            if (rioRead(rdb,&byte,1) == 0) return SWAP_LOAD_FAIL;
            ctx->rordb_object_flags = byte;
        }
        return SWAP_LOAD_SUCC;
    }
    return SWAP_LOAD_UNRECOGNIZED;
}

int swapRdbLoadKVBegin(swapRdbLoadCtx *ctx, rio *rdb) {
    /* KV begin implies sst load finished. */
    if (ctx->rordb && !ctx->rordb_sstloaded) {
        if (rordbLoadSSTFinished(rdb)) return C_ERR;
        ctx->rordb_sstloaded = 1;
    }
    return C_OK;
}

/* Return val not NULL if loaded without error, */
robj *swapRdbLoadKVLoadValue(swapRdbLoadCtx *ctx, rio *rdb, int *error,
        redisDb *db, int type, sds key, long long expiretime, long long now) {
    robj *val = NULL;

    if (error) *error = 0;

    if (ctx->rordb) {
        rdbLoadObjectSetSkipEmptyCheckFlag(1);
        val = rdbLoadObject(type,rdb,key,error);
        rdbLoadObjectSetSkipEmptyCheckFlag(0);
        /* Set rordb object flags(persist, dirty, ...) */
        if (val && ctx->rordb_object_flags != -1) {
            rordbSetObjectFlags(val,ctx->rordb_object_flags);
        }
    } else {
        rdbKeyLoadData _keydata = {0}, *keydata = &_keydata;
        int swap_load_error = swapRdbLoadObject(type,rdb,db,key,
                expiretime,now,keydata);
        if (swap_load_error == 0) {
            coldFilterAddKey(db->cold_filter,key);
            db->cold_keys++;
            val = SWAP_RDB_LOAD_MOCK_VALUE;
        } else if (swap_load_error == SWAP_ERR_RDB_LOAD_UNSUPPORTED) {
            val = rdbLoadObject(type,rdb,key,error);
        } else if (swap_load_error > 0) {
            /* reserve RDB_LOAD_ERR errno */
            if (error) *error = swap_load_error;
        } else {
            /* convert SWAP_ERR_RDB_LOAD_* to RDB_LOAD_ERR_OTHER */
            if (error) *error = RDB_LOAD_ERR_OTHER;
        }
        rdbKeyLoadDataDeinit(keydata);
    }

    return val;
}

/* Return 1 if KV loaded by swap as cold. */
int swapRdbLoadKVLoaded(swapRdbLoadCtx *ctx, redisDb *db, sds key, robj *val) {
    robj keyobj;

    UNUSED(ctx);

    /* Value wasn't loaded by swap: swap unsupported or rordb enabled. */
    if (val != SWAP_RDB_LOAD_MOCK_VALUE) return 0;

    /* When KV loaded by swap as cold, ror does not:
     * a) handle expired keys: expired keys handled later.
     * b) add key to db.dict: keys are loaded as cold. */
    initStaticStringObject(keyobj,key);
    moduleNotifyKeyspaceEvent(NOTIFY_LOADED, "loaded", &keyobj, db->id);
    sdsfree(key);

    return 1;
}

void swapRdbLoadKVEnd(swapRdbLoadCtx *ctx) {
    ctx->rordb_object_flags = -1;
}

void swapRdbLoadEnd(swapRdbLoadCtx *ctx, rio *rdb, int eoferr) {
    if (ctx->rordb && !ctx->rordb_sstloaded) {
        if (!eoferr) rordbLoadSSTFinished(rdb);
        ctx->rordb_sstloaded = 1;
    }

    if (ctx->reopen_filter) {
        setFilterState(FILTER_STATE_OPEN);
        ctx->reopen_filter = 0;
    }
}

#ifdef REDIS_TEST

sds dumpHashObject(robj *o) {
    hashTypeIterator *hi;

    sds repr = sdsempty();
    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != C_ERR) {
        sds subkey = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
        sds subval = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
        repr = sdscatprintf(repr, "(%s=>%s),",subkey,subval);
        sdsfree(subkey);
        sdsfree(subval);
    }
    hashTypeReleaseIterator(hi);
    return repr;
}

void initServerConfig(void);
int swapRdbTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    int error = 0, retval;
    robj *myhash, *mystring;
    sds myhash_key, mystring_key;
    redisDb *db;
    long long NOW = 1661657836000;

    TEST("rdb: init") {
        initServerConfig();
        ACLInit();
        server.hz = 10;
        initTestRedisDb();
        db = server.db;

        myhash_key = sdsnew("myhash"), mystring_key = sdsnew("mystring");
        myhash = createHashObject();
        hashTypeSet(myhash,sdsnew("f1"),sdsnew("v1"),HASH_SET_TAKE_FIELD|HASH_SET_TAKE_VALUE);
        hashTypeSet(myhash,sdsnew("f2"),sdsnew("v2"),HASH_SET_TAKE_FIELD|HASH_SET_TAKE_VALUE);
        hashTypeSet(myhash,sdsnew("f3"),sdsnew("v3"),HASH_SET_TAKE_FIELD|HASH_SET_TAKE_VALUE);
        hashTypeConvert(myhash, OBJ_ENCODING_HT);

        mystring = createStringObject("hello",5);
    }

    TEST("rdb: encode & decode ok in rocks format") {
        sds rawval = rocksEncodeValRdb(myhash);
        robj *decoded = rocksDecodeValRdb(rawval);
        test_assert(myhash->encoding != decoded->encoding);
        test_assert(hashTypeLength(myhash) == hashTypeLength(decoded));
    }

    TEST("rdb: save&load string ok in rocks format") {
        rio sdsrdb;
        int rdbtype;
        sds rawval = rocksEncodeValRdb(mystring);
        rioInitWithBuffer(&sdsrdb, rawval);
        rdbKeyLoadData _keydata, *load = &_keydata;

        swapStartLoading();
        rdbtype = rdbLoadObjectType(&sdsrdb);
        retval = swapRdbLoadObject(rdbtype,&sdsrdb,db,mystring_key,-1,NOW,load);
        test_assert(!retval);
        test_assert(load->rdbtype == RDB_TYPE_STRING);
        test_assert(load->swap_type == SWAP_TYPE_STRING);
        test_assert(load->nfeeds == 2);
    }

    TEST("rdb: save&load hash ok in rocks format") {
        rio sdsrdb;
        int rdbtype;
        sds rawval = rocksEncodeValRdb(myhash);
        rioInitWithBuffer(&sdsrdb, rawval);
        rdbKeyLoadData _keydata, *load = &_keydata;

        swapStartLoading();
        rdbtype = rdbLoadObjectType(&sdsrdb);
        retval = swapRdbLoadObject(rdbtype,&sdsrdb,db,myhash_key,-1,NOW,load);
        test_assert(!retval);
        test_assert(load->rdbtype == RDB_TYPE_HASH);
        test_assert(load->swap_type == SWAP_TYPE_HASH);
        test_assert(load->nfeeds == 4);
    }

    return error;
}

#endif

