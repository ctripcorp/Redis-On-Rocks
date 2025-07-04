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

static void createFakeHashForDeleteIfCold(swapData *data) {
	if (swapDataIsCold(data)) {
        /* empty hash allowed */
		dbAdd(data->db,data->key,createHashObject());
	}
}

#define SELECT_MAIN 0
#define SELECT_DSS  1

/* return 1 if noswap needed */
static int hashSwapAnaOutSelectSubkeys(swapData *data, hashDataCtx *datactx,
        int *may_keep_data) {
    int select_type, noswap;
    size_t count;
    robj *subkeys;
    unsigned long long evict_memory = 0;

    if (objectIsDataDirty(data->value)) { /* all subkeys might be dirty */
        select_type = SELECT_MAIN;
        subkeys = data->value;
        count = hashTypeLength(subkeys);
        noswap = 0;
    } else if (data->dirty_subkeys) { /* a subset of subkeys might be dirty */
        select_type = SELECT_DSS;
        subkeys = data->dirty_subkeys;
        count = dirtySubkeysLength(subkeys);
        noswap = 0;
    } else {
        /* If data dirty, meta will be persisted as an side effect.
         * If just meta dirty, we still persists meta.
         * If data & meta clean, we persists nothing (just free). */
        if (objectIsMetaDirty(data->value)) { /* meta dirty */
            /* meta dirty */
            select_type = SELECT_MAIN;
            subkeys = data->value;
            count = 0;
            noswap = 0;
        } else { /* clean */
            select_type = SELECT_MAIN;
            subkeys = data->value;
            count = hashTypeLength(subkeys);
            noswap = 1;
        }
    }

    count = MIN(count,(size_t)server.swap_evict_step_max_subkeys);
    datactx->ctx.type = BASE_SWAP_CTX_TYPE_SUBKEY;
    datactx->ctx.sub.subkeys = zmalloc(count*sizeof(robj*));

    *may_keep_data = 1;
    if (select_type == SELECT_MAIN) {
        hashTypeIterator *hi = hashTypeInitIterator(subkeys);
        while (hashTypeNext(hi) != C_ERR) {
            robj *subkey;
            unsigned char *vstr;
            unsigned int vlen;
            long long vll;

            if ((size_t)datactx->ctx.sub.num >= count ||
                    evict_memory >= server.swap_evict_step_max_memory) {
                /* Evict big hash in small steps.
                 * There still may be dirty subkeys in memory, cant set clean
                 * while keep data. */
                if (!noswap) *may_keep_data = 0;
                break;
            }

            hashTypeCurrentObject(hi,OBJ_HASH_KEY,&vstr,&vlen,&vll);
            if (vstr) {
                subkey = createStringObject((const char*)vstr,vlen);
            } else {
                subkey = createStringObjectFromLongLong(vll);
                subkey = unshareStringValue(subkey);
            }
            datactx->ctx.sub.subkeys[datactx->ctx.sub.num++] = subkey;

            hashTypeCurrentObject(hi,OBJ_HASH_VALUE,&vstr,&vlen,&vll);
            if (vstr)
                evict_memory += vlen;
            else
                evict_memory += sizeof(vll);
        }
        hashTypeReleaseIterator(hi);
    } else {
        robj *subkey;
        size_t sublen;
        dirtySubkeysIterator dss_iter;
        list *redundent_subkeys = listCreate();

        dirtySubkeysIteratorInit(&dss_iter, subkeys);
        while ((subkey = dirtySubkeysIteratorNext(&dss_iter,&sublen)) != NULL) {
            if ((size_t)datactx->ctx.sub.num >= count ||
                    evict_memory >= server.swap_evict_step_max_memory) {
                /* Evict big hash in small steps. */
                if (!noswap) *may_keep_data = 0;
                decrRefCount(subkey);
                break;
            }

            /* check with lock hold so that evicting subkeys must exist. */
            if (hashTypeExists(data->value,subkey->ptr)) {
                datactx->ctx.sub.subkeys[datactx->ctx.sub.num++] = subkey;
                evict_memory += sublen;
            } else {
                listAddNodeTail(redundent_subkeys, subkey);
            }
        }
        dirtySubkeysIteratorDeinit(&dss_iter);

        listIter li;
        listNode *ln;
        listRewind(redundent_subkeys, &li);
        while ((ln = listNext(&li))) {
            subkey = listNodeValue(ln);
            dirtySubkeysRemove(subkeys, subkey->ptr);
            decrRefCount(subkey);
        }
        listRelease(redundent_subkeys);
    }

    return noswap;
}

int hashSwapAna(swapData *data, int thd, struct keyRequest *req,
        int *intention, uint32_t *intention_flags, void *datactx_) {
    hashDataCtx *datactx = datactx_;
    int cmd_intention = req->cmd_intention;
    uint32_t cmd_intention_flags = req->cmd_intention_flags;
    serverAssert(req->cmd_flags != 0);


    switch (cmd_intention) {
    case SWAP_NOP:
        *intention = SWAP_NOP;
        *intention_flags = 0;
        break;
    case SWAP_IN:
        serverAssert(req->type == KEYREQUEST_TYPE_SUBKEY ||
                req->type == KEYREQUEST_TYPE_SAMPLE);
        if (!swapDataPersisted(data)) {
            /* No need to swap for pure hot key */
            *intention = SWAP_NOP;
            *intention_flags = 0;
        } else if (req->type == KEYREQUEST_TYPE_SAMPLE) {
            datactx->ctx.type = BASE_SWAP_CTX_TYPE_SAMPLE;
            datactx->ctx.spl.count = req->sp.count;
            *intention = SWAP_IN;
            *intention_flags = 0;
        } else if (req->b.num_subkeys == 0) {
            if (cmd_intention_flags == SWAP_IN_DEL_MOCK_VALUE) {
                /* DEL/UNLINK: Lazy delete current key. */
                datactx->ctx.ctx_flag |= BIG_DATA_CTX_FLAG_MOCK_VALUE;
                *intention = SWAP_DEL;
                *intention_flags = SWAP_FIN_DEL_SKIP;
            } else if (cmd_intention_flags & SWAP_IN_DEL
                || cmd_intention_flags & SWAP_IN_OVERWRITE
                || cmd_intention_flags & SWAP_IN_FORCE_HOT) {
                objectMeta *meta = swapDataObjectMeta(data);
                if (meta->len == 0) {
                    *intention = SWAP_DEL;
                    *intention_flags = SWAP_FIN_DEL_SKIP;
                } else {
                    *intention = SWAP_IN;
                    *intention_flags = SWAP_EXEC_IN_DEL;
                }
                if (cmd_intention_flags & SWAP_IN_FORCE_HOT) {
                    *intention_flags |= SWAP_EXEC_FORCE_HOT;
                }
            } else if (swapDataIsHot(data)) {
                /* No need to do swap for hot key(execept for SWAP_IN_DEl). */
                *intention = SWAP_NOP;
                *intention_flags = 0;
            } else if (cmd_intention_flags == SWAP_IN_META) {
                /* HLEN: swap in meta (with random field gets empty hash)
                 * also HLEN command will be modified like dbsize. */
                datactx->ctx.type = BASE_SWAP_CTX_TYPE_SUBKEY;
                datactx->ctx.sub.num = 0;
                datactx->ctx.sub.subkeys = zmalloc(sizeof(robj*));
                datactx->ctx.sub.subkeys[datactx->ctx.sub.num++] = createStringObject("foo",3);
                *intention = SWAP_IN;
                *intention_flags = 0;
            } else {
                /* HKEYS/HVALS/..., swap in all fields */
                datactx->ctx.type = BASE_SWAP_CTX_TYPE_SUBKEY;
                datactx->ctx.sub.num = 0;
                datactx->ctx.sub.subkeys = NULL;
                *intention = SWAP_IN;
                *intention_flags = 0;
            }
        } else { /* keyrequests with subkeys */
            datactx->ctx.type = BASE_SWAP_CTX_TYPE_SUBKEY;
            datactx->ctx.sub.num = 0;
            datactx->ctx.sub.subkeys = zmalloc(req->b.num_subkeys * sizeof(robj*));
            for (int i = 0; i < req->b.num_subkeys; i++) {
                robj *subkey = req->b.subkeys[i];
                /* HDEL: even if field is hot (exists in value), we still
                 * need to do ROCKS_DEL on those fields. */
                if (cmd_intention_flags == SWAP_IN_DEL ||
                        data->value == NULL ||
                        !hashTypeExists(data->value,subkey->ptr)) {
                    if (swapDataMayContainSubkey(data,thd,subkey)) {
                        incrRefCount(subkey);
                        datactx->ctx.sub.subkeys[datactx->ctx.sub.num++] = subkey;
                    }
                }
            }

            *intention = datactx->ctx.sub.num > 0 ? SWAP_IN : SWAP_NOP;
            if (cmd_intention_flags == SWAP_IN_DEL)
                *intention_flags = SWAP_EXEC_IN_DEL;
            else
                *intention_flags = 0;
        }

        if (cmd_intention_flags & SWAP_OOM_CHECK) {
            *intention_flags |= SWAP_EXEC_OOM_CHECK;
        }
        break;
    case SWAP_OUT:
        if (swapDataIsCold(data)) {
            *intention = SWAP_NOP;
            *intention_flags = 0;
        } else {
            /* may_keep_data is true if we could keep data in memory and clear dirty
             * after persisting data to rocksdb. */
            int may_keep_data;
            int noswap = hashSwapAnaOutSelectSubkeys(data,datactx,&may_keep_data);
            int keep_data = swapDataPersistKeepData(data,cmd_intention_flags,may_keep_data);

            /* create new meta if needed */
            if (!swapDataPersisted(data)) {
                swapDataSetNewObjectMeta(data,
                        createHashObjectMeta(swapGetAndIncrVersion(),0));
            }

            if (noswap) {
                /* directly evict value from db.dict if not dirty. */
                swapDataCleanObject(data,datactx,keep_data);
                if (hashTypeLength(data->value) == 0) {
                    swapDataTurnCold(data);
                }
                swapDataSwapOut(data,datactx,keep_data,NULL);

                *intention = SWAP_NOP;
                *intention_flags = 0;
            } else {
                *intention = SWAP_OUT;
                *intention_flags = keep_data ? SWAP_EXEC_OUT_KEEP_DATA : 0;
            }
        }
        break;
    case SWAP_DEL:
        /* TODO add if branch, refer to list. */
        *intention = SWAP_DEL;
        *intention_flags = 0;
        break;
    default:
        break;
    }

    return 0;
}

static inline sds hashEncodeSubkey(redisDb *db, sds key, uint64_t version,
        sds subkey) {
    return rocksEncodeDataKey(db,key,version,subkey);
}

int hashSwapAnaAction(swapData *data, int intention, void *datactx_, int *action) {
    UNUSED(data);
    hashDataCtx *datactx = datactx_;
    switch (intention) {
        case SWAP_IN:
            if (datactx->ctx.type == BASE_SWAP_CTX_TYPE_SUBKEY &&
                    datactx->ctx.sub.num > 0) {
                *action = ROCKS_GET; /* Swap in specific fields */
            } else {
                /* Swap in entire hash(HKEYS/HVALS...) or sample some
                 * subkey (MEMORY USAGE) */
                *action = ROCKS_ITERATE;
            }
            break;
        case SWAP_DEL:
            /* No need to del data (meta will be deleted by exec) */
            *action = ROCKS_NOP;
            break;
        case SWAP_OUT:
            *action = ROCKS_PUT;
            break;
        default:
            *action = ROCKS_NOP;
            return SWAP_ERR_DATA_FAIL;
    }
    return 0;
}

int hashEncodeKeys(swapData *data, int intention, void *datactx_,
        int *numkeys, int **pcfs, sds **prawkeys) {
    hashDataCtx *datactx = datactx_;
    sds *rawkeys = NULL;
    int *cfs = NULL;
    uint64_t version = swapDataObjectVersion(data);

    serverAssert(intention == SWAP_IN);
    cfs = zmalloc(sizeof(int)*datactx->ctx.sub.num);
    rawkeys = zmalloc(sizeof(sds)*datactx->ctx.sub.num);
    for (int i = 0; i < datactx->ctx.sub.num; i++) {
        cfs[i] = DATA_CF;
        rawkeys[i] = hashEncodeSubkey(data->db,data->key->ptr,
                                      version,datactx->ctx.sub.subkeys[i]->ptr);
    }
    *numkeys = datactx->ctx.sub.num;
    *pcfs = cfs;
    *prawkeys = rawkeys;

    return 0;
}

static inline sds hashEncodeSubval(robj *subval) {
    return rocksEncodeValRdb(subval);
}

int hashEncodeRange(struct swapData *data, int intention, void *datactx_, int *limit,
        uint32_t *flags, int *pcf, sds *start, sds *end) {
    UNUSED(intention);
    hashDataCtx *datactx = datactx_;
    uint64_t version = swapDataObjectVersion(data);

    *pcf = DATA_CF;
    *flags = 0;
    *start = rocksEncodeDataRangeStartKey(data->db,data->key->ptr,version);
    *end = rocksEncodeDataRangeEndKey(data->db,data->key->ptr,version);

    if (datactx->ctx.type == BASE_SWAP_CTX_TYPE_SAMPLE) {
        *limit = datactx->ctx.spl.count;
    } else {
        *limit =  ROCKS_ITERATE_NO_LIMIT;
    }
    return 0;
}

int hashEncodeData(swapData *data, int intention, void *datactx_,
        int *numkeys, int **pcfs, sds **prawkeys, sds **prawvals) {
    hashDataCtx *datactx = datactx_;
    int *cfs = zmalloc(datactx->ctx.sub.num*sizeof(int));
    sds *rawkeys = zmalloc(datactx->ctx.sub.num*sizeof(sds));
    sds *rawvals = zmalloc(datactx->ctx.sub.num*sizeof(sds));
    uint64_t version = swapDataObjectVersion(data);

    serverAssert(intention == SWAP_OUT);
    for (int i = 0; i < datactx->ctx.sub.num; i++) {
        cfs[i] = DATA_CF;
        rawkeys[i] = hashEncodeSubkey(data->db,data->key->ptr,
                version,datactx->ctx.sub.subkeys[i]->ptr);
        robj *subval = hashTypeGetValueObject(data->value,
                datactx->ctx.sub.subkeys[i]->ptr);
        serverAssert(subval);
        rawvals[i] = hashEncodeSubval(subval);
        decrRefCount(subval);
    }
    *numkeys = datactx->ctx.sub.num;
    *pcfs = cfs;
    *prawkeys = rawkeys;
    *prawvals = rawvals;
    return 0;
}

/* decoded object move to exec module */
int hashDecodeData(swapData *data, int num, int *cfs, sds *rawkeys,
        sds *rawvals, void **pdecoded) {
    int i;
    robj *decoded;
    uint64_t version = swapDataObjectVersion(data);

    serverAssert(num >= 0);
    UNUSED(cfs);

    /* Note that event if all subkeys are not found, still an empty hash
     * object will be returned: empty *warm* hash could can meta in memory,
     * so that we don't need to update rocks-meta right after call(). */
    decoded = createHashObject();

    for (i = 0; i < num; i++) {
        int dbid;
        sds subkey, subval;
        const char *keystr, *subkeystr;
        size_t klen, slen;
        robj *subvalobj;
        robj ssubkeyobj, ssubvalobj;
        robj *argv[2];
        uint64_t subkey_version;

        if (rawvals[i] == NULL)
            continue;
        if (rocksDecodeDataKey(rawkeys[i],sdslen(rawkeys[i]),
                &dbid,&keystr,&klen,&subkey_version,&subkeystr,&slen) < 0)
            continue;
        if (!swapDataPersisted(data))
            continue;
        if (version != subkey_version)
            continue;
        subkey = sdsnewlen(subkeystr,slen);

        subvalobj = rocksDecodeValRdb(rawvals[i]);
        serverAssert(subvalobj->type == OBJ_STRING);
        /* subvalobj might be shared integer, unshared it before
         * add to decoded. */
        subvalobj = unshareStringValue(subvalobj);
        /* steal subvalobj sds */
        subval = subvalobj->ptr;
        subvalobj->ptr = NULL;
        decrRefCount(subvalobj);

        initStaticStringObject(ssubkeyobj,subkey);
        initStaticStringObject(ssubvalobj,subval);
        argv[0] = &ssubkeyobj;
        argv[1] = &ssubvalobj;
        hashTypeTryConversion(decoded,argv,0,1);
        hashTypeSet(decoded,subkey,subval,
                HASH_SET_TAKE_FIELD|HASH_SET_TAKE_VALUE);
    }

    *pdecoded = decoded;
    return 0;
}

static inline robj *createSwapInObject(robj *newval) {
    robj *swapin = newval;
    serverAssert(newval && newval->type == OBJ_HASH);
    clearObjectDirty(swapin);
    clearObjectPersistKeep(swapin);
    return swapin;
}

/* Note: meta are kept as long as there are data in rocksdb. */
int hashSwapIn(swapData *data, void *result, void *datactx) {
    UNUSED(datactx);
    /* hot key no need to swap in, this must be a warm or cold key. */
    serverAssert(swapDataPersisted(data));
    if (swapDataIsCold(data) && result != NULL /* may be empty */) {
        /* cold key swapped in result (may be empty). */
        robj *swapin = createSwapInObject(result);
        /* mark persistent after data swap in without
         * persistence deleted, or mark non-persistent else */
        overwriteObjectPersistent(swapin,!data->persistence_deleted);
        dbAdd(data->db,data->key,swapin);
        /* expire will be swapped in later by swap framework. */
        if (data->cold_meta) {
            dbAddMeta(data->db,data->key,data->cold_meta);
            data->cold_meta = NULL; /* moved */
        }
    } else {
        if (result) decrRefCount(result);
        if (data->value) overwriteObjectPersistent(data->value,!data->persistence_deleted);
    }

    return 0;
}

/* subkeys already cleaned by cleanObject(to save cpu usage of main thread),
 * swapout only updates db.dict keyspace, meta (db.meta/db.expire) swapped
 * out by swap framework. */
int hashSwapOut(swapData *data, void *datactx, int keep_data, int *totally_out) {
    UNUSED(datactx);
    serverAssert(!swapDataIsCold(data));

    if (data->dirty_subkeys &&
            dirtySubkeysLength(data->dirty_subkeys) == 0) {
        dbDeleteDirtySubkeys(data->db,data->key);
    }

    if (keep_data) {
        clearObjectDataDirty(data->value);
        setObjectPersistent(data->value);
    }

    if (hashTypeLength(data->value) == 0) {
        /* all fields swapped out, key turnning into cold:
         * - rocks-meta should have already persisted.
         * - object_meta and value will be deleted by dbDelete, expire already
         *   deleted by swap framework. */
        dbDelete(data->db,data->key);
        /* new_meta exists if hot key turns cold directly, in which case
         * new_meta not moved to db.meta nor updated but abandonded. */
        if (data->new_meta) {
            freeObjectMeta(data->new_meta);
            data->new_meta = NULL;
        }
        if (totally_out) *totally_out = 1;
    } else { /* not all fields swapped out. */
        if (data->new_meta) {
            dbAddMeta(data->db,data->key,data->new_meta);
            data->new_meta = NULL; /* moved to db.meta */
            setObjectPersistent(data->value); /* loss pure hot and persistent data exist. */
        }
        if (totally_out) *totally_out = 0;
    }

    return 0;
}

int hashSwapDel(swapData *data, void *datactx_, int del_skip) {
    hashDataCtx* datactx = (hashDataCtx*)datactx_;
    if (datactx->ctx.ctx_flag & BIG_DATA_CTX_FLAG_MOCK_VALUE) {
        createFakeHashForDeleteIfCold(data);
    }
    if (del_skip) {
        if (!swapDataIsCold(data))
            dbDeleteMeta(data->db,data->key);
        return 0;
    } else {
        if (!swapDataIsCold(data))
            /* both value/object_meta/expire are deleted */
            dbDelete(data->db,data->key);
        return 0;
    }
}

/* Decoded moved back by exec to hashSwapData */
void *hashCreateOrMergeObject(swapData *data, void *decoded_, void *datactx) {
    robj *result, *decoded = decoded_;

    UNUSED(datactx);
    serverAssert(decoded == NULL || decoded->type == OBJ_HASH);

    if (swapDataIsCold(data) || decoded == NULL) {
        /* decoded moved back to swap framework again (result will later be
         * pass as swapIn param). */
        result = decoded;
        if (decoded) {
            swapDataObjectMetaModifyLen(data,-hashTypeLength(decoded));
        }
    } else {
        hashTypeIterator *hi;
        robj *argv[2], subkeyobj, subvalobj;
        hi = hashTypeInitIterator(decoded);
        while (hashTypeNext(hi) != C_ERR) {
            sds subkey = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            if (hashTypeExists(data->value, subkey)) {
                /* field exists in memory and skip. */
                sdsfree(subkey);
                continue;
            }

            sds subval = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            initStaticStringObject(subkeyobj,subkey);
            initStaticStringObject(subvalobj,subval);
            argv[0] = &subkeyobj;
            argv[1] = &subvalobj;
            hashTypeTryConversion(data->value,argv,0,1);
            hashTypeSet(data->value, subkey, subval, HASH_SET_TAKE_FIELD|HASH_SET_TAKE_VALUE);
            swapDataObjectMetaModifyLen(data,-1);
        }
        hashTypeReleaseIterator(hi);
        /* decoded merged, we can release it now. */
        decrRefCount(decoded);
        result = NULL;
    }
    return result;
}

int hashCleanObject(swapData *data, void *datactx_, int keep_data) {
    hashDataCtx *datactx = datactx_;
    if (swapDataIsCold(data)) return 0;
    for (int i = 0; i < datactx->ctx.sub.num; i++) {
        if (data->dirty_subkeys) {
            dirtySubkeysRemove(data->dirty_subkeys,
                    datactx->ctx.sub.subkeys[i]->ptr);
        }
        if (!keep_data && hashTypeDelete(data->value,datactx->ctx.sub.subkeys[i]->ptr)) {
            swapDataObjectMetaModifyLen(data,1);
        }
    }
    return 0;
}

/* Only free extend fields here, base fields (key/value/object_meta) freed
 * in swapDataFree */
void freeHashSwapData(swapData *data_, void *datactx_) {
    UNUSED(data_);
    hashDataCtx *datactx = datactx_;
    if (datactx->ctx.type == BASE_SWAP_CTX_TYPE_SUBKEY) {
        for (int i = 0; i < datactx->ctx.sub.num; i++) {
            decrRefCount(datactx->ctx.sub.subkeys[i]);
        }
        zfree(datactx->ctx.sub.subkeys);
    }
    zfree(datactx);
}

void *hashGetObjectMetaAux(swapData *data, void *datactx) {
    UNUSED(datactx);
    size_t hotlen = data->value ? hashTypeLength(data->value) : 0;
    return (void*)hotlen;
}

swapDataType hashSwapDataType = {
    .name = "hash",
    .cmd_swap_flags = CMD_SWAP_DATATYPE_HASH,
    .swapAna = hashSwapAna,
    .swapAnaAction = hashSwapAnaAction,
    .encodeKeys = hashEncodeKeys,
    .encodeRange = hashEncodeRange,
    .encodeData = hashEncodeData,
    .decodeData = hashDecodeData,
    .swapIn = hashSwapIn,
    .swapOut = hashSwapOut,
    .swapDel = hashSwapDel,
    .createOrMergeObject = hashCreateOrMergeObject,
    .cleanObject = hashCleanObject,
    .beforeCall = NULL,
    .free = freeHashSwapData,
    .rocksDel = NULL,
    .mergedIsHot = hashMergedIsHot,
    .getObjectMetaAux = hashGetObjectMetaAux,
};

int swapDataSetupHash(swapData *d, void **pdatactx) {
    d->type = &hashSwapDataType;
    d->omtype = &hashObjectMetaType;
    hashDataCtx *datactx = zmalloc(sizeof(hashDataCtx));
    datactx->ctx.sub.num = 0;
    datactx->ctx.sub.subkeys = NULL;
    *pdatactx = datactx;
    return 0;
}

/* Hash rdb save */
int hashSaveStart(rdbKeySaveData *save, rio *rdb) {
    robj *key = save->key;
    size_t nfields = 0;
    int ret = 0;

    /* save header */
    if (rdbSaveKeyHeader(rdb,key,key,RDB_TYPE_HASH,
                save->expire) == -1)
        return -1;

    /* nfields */
    if (save->value)
        nfields += hashTypeLength(save->value);
    if (save->object_meta)
        nfields += save->object_meta->len;
    if (rdbSaveLen(rdb,nfields) == -1)
        return -1;

    if (!save->value)
        return 0;

    /* save fields from value (db.dict) */
    hashTypeIterator *hi = hashTypeInitIterator(save->value);
    while (hashTypeNext(hi) != C_ERR) {
        sds subkey = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
        sds subval = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
        if (rdbSaveRawString(rdb,(unsigned char*)subkey,
                    sdslen(subkey)) == -1) {
            sdsfree(subkey);
            sdsfree(subval);
            ret = -1;
            break;
        }
        if (rdbSaveRawString(rdb,(unsigned char*)subval,
                    sdslen(subval)) == -1) {
            sdsfree(subkey);
            sdsfree(subval);
            ret = -1;
            break;
        }
        sdsfree(subkey);
        sdsfree(subval);
    }
    hashTypeReleaseIterator(hi);

    return ret;
}

int hashSave(rdbKeySaveData *save, rio *rdb, decodedData *decoded) {
    robj *key = save->key;
    serverAssert(!sdscmp(decoded->key, key->ptr));

    if (decoded->rdbtype != RDB_TYPE_STRING) {
        /* check failed, skip this key */
        return 0;
    }

    if (save->value != NULL) {
        if (hashTypeExists(save->value,
                    decoded->subkey)) {
            /* already save in save_start, skip this subkey */
            return 0;
        }
    }

    if (rdbSaveRawString(rdb,(unsigned char*)decoded->subkey,
                sdslen(decoded->subkey)) == -1) {
        return -1;
    }

    if (rdbWriteRaw(rdb,(unsigned char*)decoded->rdbraw,
                sdslen(decoded->rdbraw)) == -1) {
        return -1;
    }

    save->saved++;
    return 0;
}

int hashSaveEnd(rdbKeySaveData *save, rio *rdb, int save_result) {
    objectMeta *object_meta = save->object_meta;
    UNUSED(rdb);
    long expected = object_meta->len + server.swap_debug_bgsave_metalen_addition;
    if (save->saved != expected) {
        sds key  = save->key->ptr;
        sds repr = sdscatrepr(sdsempty(), key, sdslen(key));
        serverLog(LL_WARNING,
                "hashSave %s: saved(%d) != object_meta.len(%ld)",
                repr, save->saved, expected);
        sdsfree(repr);
        return SAVE_ERR_META_LEN_MISMATCH;
    }
    return save_result;
}

rdbKeySaveType hashSaveType = {
    .save_start = hashSaveStart,
    .save_hot_ext = NULL,
    .save = hashSave,
    .save_end = hashSaveEnd,
    .save_deinit = NULL,
};

int hashSaveInit(rdbKeySaveData *save, uint64_t version, const char *extend,
        size_t extlen) {
    int retval = 0;
    save->type = &hashSaveType;
    save->omtype = &hashObjectMetaType;
    if (extend) {
        serverAssert(save->object_meta == NULL);
        retval = buildObjectMeta(OBJ_HASH,version,extend,
                extlen,&save->object_meta);
    }
    return retval;
}

/* Hash rdb load */
void hashLoadStartZip(struct rdbKeyLoadData *load, rio *rdb, int *cf,
        sds *rawkey, sds *rawval, int *error) {
    sds extend = NULL;

    load->value = rdbLoadObject(load->rdbtype,rdb,load->key,error);
    if (load->value == NULL) return;

    if (load->value->type != OBJ_HASH) {
        serverLog(LL_WARNING,"Load rdb with rdbtype(%d) got (%d)",
                load->rdbtype, load->value->type);
        *error = RDB_LOAD_ERR_OTHER;
        return;
    }

    if (hashTypeLength(load->value) == 0) {
        *error = RDB_LOAD_ERR_EMPTY_KEY;
        return;
    }

    load->iter = hashTypeInitIterator(load->value);
    if (hashTypeNext(load->iter) == C_ERR) {
        serverLog(LL_WARNING,"Load rdb iter not valid.");
        *error = RDB_LOAD_ERR_OTHER;
        return;
    }

    load->total_fields = hashTypeLength(load->value);
    extend = rocksEncodeObjectMetaLen(load->total_fields);
    *cf = META_CF;
    *rawkey = rocksEncodeMetaKey(load->db,load->key);
    *rawval = rocksEncodeMetaVal(load->swap_type,load->expire,load->version,extend);
    sdsfree(extend);
}

void hashLoadStart(struct rdbKeyLoadData *load, rio *rdb, int *cf,
        sds *rawkey, sds *rawval, int *error) {
    switch (load->rdbtype) {
    case RDB_TYPE_HASH_ZIPMAP:
        hashLoadStartZip(load,rdb,cf,rawkey,rawval,error);
        break;
    case RDB_TYPE_HASH_ZIPLIST:
        hashLoadStartZip(load,rdb,cf,rawkey,rawval,error);
        break;
    case RDB_TYPE_HASH:
        rdbLoadStartHT(load,rdb,cf,rawkey,rawval,error);
        break;
    default:
        break;
    }
}

int hashLoadZip(struct rdbKeyLoadData *load, rio *rdb, int *cf, sds *rawkey,
        sds *rawval, int *error) {
    sds subkey, subval;
    robj subobj;

    UNUSED(rdb);

    subkey = hashTypeCurrentObjectNewSds(load->iter,OBJ_HASH_KEY);
    subval = hashTypeCurrentObjectNewSds(load->iter,OBJ_HASH_VALUE);
    initStaticStringObject(subobj,subval);

    *cf = DATA_CF;
    *rawkey = rocksEncodeDataKey(load->db,load->key,load->version,subkey);
    *rawval = rocksEncodeValRdb(&subobj);
    *error = 0;

    sdsfree(subkey);
    sdsfree(subval);

    return hashTypeNext(load->iter) != C_ERR;
}

int hashLoadHT(struct rdbKeyLoadData *load, rio *rdb, int *cf, sds *rawkey,
        sds *rawval, int *error) {
    sds subkey, rdbval;

    *error = RDB_LOAD_ERR_OTHER;
    if ((subkey = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) {
        return 0;
    }

    rdbval = rdbVerbatimNew(RDB_TYPE_STRING);
    if (rdbLoadStringVerbatim(rdb,&rdbval)) {
        sdsfree(rdbval);
        sdsfree(subkey);
        return 0;
    }

    *cf = DATA_CF;
    *rawkey = rocksEncodeDataKey(load->db,load->key,load->version,subkey);
    *rawval = rdbval;
    *error = 0;
    sdsfree(subkey);
    load->loaded_fields++;
    return load->loaded_fields < load->total_fields;
}

int hashLoad(struct rdbKeyLoadData *load, rio *rdb, int *cf,
        sds *rawkey, sds *rawval, int *error) {
    int retval;

    switch (load->rdbtype) {
    case RDB_TYPE_HASH:
        retval = hashLoadHT(load,rdb,cf,rawkey,rawval,error);
        break;
    case RDB_TYPE_HASH_ZIPMAP:
    case RDB_TYPE_HASH_ZIPLIST:
        retval = hashLoadZip(load,rdb,cf,rawkey,rawval,error);
        break;
    default:
        retval = RDB_LOAD_ERR_OTHER;
    }

    return retval;
}

void hashLoadDeinit(struct rdbKeyLoadData *load) {
    if (load->iter) {
        hashTypeReleaseIterator(load->iter);
        load->iter = NULL;
    }

    if (load->value) {
        decrRefCount(load->value);
        load->value = NULL;
    }
}

rdbKeyLoadType hashLoadType = {
    .load_start = hashLoadStart,
    .load = hashLoad,
    .load_end = NULL,
    .load_deinit = hashLoadDeinit,
};

void hashLoadInit(rdbKeyLoadData *load) {
    load->type = &hashLoadType;
    load->omtype = &hashObjectMetaType;
    load->swap_type = SWAP_TYPE_HASH;
}

size_t hashMetaLength(redisDb *db, robj *key) {
    objectMeta *om = lookupMeta(db, key);
    return om ? om->len : 0;
}

#ifdef REDIS_TEST

#define SWAP_EVICT_STEP 2
#define SWAP_EVICT_MEM  (1*1024*1024)

#define INIT_SAVE_SKIP -2

int swapDataHashTest(int argc, char **argv, int accurate) {
    UNUSED(argc), UNUSED(argv), UNUSED(accurate);

    initTestRedisServer();
    redisDb* db = server.db + 0;
    int error = 0;
    swapData *hash1_data, *cold1_data;
    hashDataCtx *hash1_ctx, *cold1_ctx = NULL;
    robj *key1, *hash1, *cold1;
    objectMeta *cold1_meta;
    keyRequest _kr1, *kr1 = &_kr1, _cold_kr1, *cold_kr1 = &_cold_kr1;
    int intention;
    uint32_t intention_flags;
    robj *subkeys1[4];
    sds f1,f2,f3,f4;
    sds sds1, sds2, int1, int2;
    int action, numkeys, *cfs;
    sds *rawkeys, *rawvals;
    server.child_pid = -1;
    int originEvictStepMaxSubkey = server.swap_evict_step_max_subkeys;
    int originEvictStepMaxMemory = server.swap_evict_step_max_memory;

    TEST("hash - init") {
        server.swap_evict_step_max_subkeys = SWAP_EVICT_STEP;
        server.swap_evict_step_max_memory = SWAP_EVICT_MEM;

        key1 = createStringObject("key1",4);
        cold1 = createStringObject("cold1",5);
        cold1_meta = createHashObjectMeta(0,4);
        f1 = sdsnew("f1"), f2 = sdsnew("f2"), f3 = sdsnew("f3"), f4 = sdsnew("f4");
        sds1 = sdsnew("sds_v1"), sds2 = sdsnew("sds_v2");
        int1 = sdsnew("1"), int2 = sdsnew("2");
        hash1 = createHashObject();
        hashTypeSet(hash1,f1,sds1,HASH_SET_COPY);
        hashTypeSet(hash1,f2,sds2,HASH_SET_COPY);
        hashTypeSet(hash1,f3,int1,HASH_SET_COPY);
        hashTypeSet(hash1,f4,int2,HASH_SET_COPY);
        incrRefCount(key1);
        kr1->key = key1;
        kr1->type = KEYREQUEST_TYPE_SUBKEY;
        kr1->level = REQUEST_LEVEL_KEY;
        kr1->b.num_subkeys = 0;
        kr1->b.subkeys = NULL;
        kr1->cmd_flags = CMD_SWAP_DATATYPE_HASH;
        incrRefCount(key1);
        cold_kr1->key = key1;
        cold_kr1->level = REQUEST_LEVEL_KEY;
        cold_kr1->type = KEYREQUEST_TYPE_SUBKEY;
        cold_kr1->b.num_subkeys = 0;
        cold_kr1->b.subkeys = NULL;
        cold_kr1->cmd_flags = CMD_SWAP_DATATYPE_HASH;
        dbAdd(db,key1,hash1);

        hash1_data = createSwapData(db,key1,hash1,NULL);
        swapDataSetupMeta(hash1_data,OBJ_HASH,-1,(void**)&hash1_ctx);
        swapDataSetObjectMeta(hash1_data, NULL);

        cold1_data = createSwapData(db,cold1,NULL,NULL);
        swapDataSetupMeta(cold1_data,OBJ_HASH,-1,(void**)&cold1_ctx);
        swapDataSetObjectMeta(cold1_data, cold1_meta);
    }

    TEST("hash - swapAna") {
        /* nop: NOP/IN_META/IN_DEL/IN hot/OUT cold... */
        kr1->cmd_intention = SWAP_NOP, kr1->cmd_intention_flags = 0;
        swapDataAna(hash1_data,0,kr1,&intention,&intention_flags,hash1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        kr1->cmd_intention = SWAP_IN, kr1->cmd_intention_flags = SWAP_IN_META;
        swapDataAna(hash1_data,0,kr1,&intention,&intention_flags,hash1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        kr1->cmd_intention = SWAP_IN, kr1->cmd_intention_flags = SWAP_IN_DEL;
        swapDataAna(hash1_data,0,kr1,&intention,&intention_flags,hash1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        kr1->cmd_intention = SWAP_IN, kr1->cmd_intention_flags = 0;
        swapDataAna(hash1_data,0,kr1,&intention,&intention_flags,hash1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        kr1->cmd_intention = SWAP_IN, kr1->cmd_intention_flags = 0;
        swapDataAna(hash1_data,0,kr1,&intention,&intention_flags,hash1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        cold_kr1->cmd_intention = SWAP_OUT, cold_kr1->cmd_intention_flags = 0;
        swapDataAna(cold1_data,0,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_NOP && intention_flags == 0);
        cold_kr1->cmd_intention = SWAP_DEL, cold_kr1->cmd_intention_flags = 0;
        swapDataAna(cold1_data,0,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_DEL && intention_flags == 0);
        /* in: entire or with subkeys */
        cold_kr1->cmd_intention = SWAP_IN, cold_kr1->cmd_intention_flags = 0;
        swapDataAna(cold1_data,0,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold1_ctx->ctx.sub.num == 0 && cold1_ctx->ctx.sub.subkeys == NULL);
        subkeys1[0] = createStringObject(f1,sdslen(f1));
        subkeys1[1] = createStringObject(f2,sdslen(f2));
        cold_kr1->b.num_subkeys = 2;
        cold_kr1->b.subkeys = subkeys1;
        cold_kr1->cmd_intention = SWAP_IN, cold_kr1->cmd_intention_flags = 0;
        swapDataAna(cold1_data,0,cold_kr1,&intention,&intention_flags,cold1_ctx);
        test_assert(intention == SWAP_IN && intention_flags == 0);
        test_assert(cold1_ctx->ctx.sub.num == 2 && cold1_ctx->ctx.sub.subkeys != NULL);
        /* out: evict by small steps */
        kr1->b.num_subkeys = 0;
        kr1->b.subkeys = NULL;
        kr1->cmd_intention = SWAP_OUT, kr1->cmd_intention_flags = 0;
        swapDataAna(hash1_data,0,kr1,&intention,&intention_flags,hash1_ctx);
        test_assert(intention == SWAP_OUT && intention_flags == 0);
        test_assert(cold1_ctx->ctx.sub.num == SWAP_EVICT_STEP && cold1_ctx->ctx.sub.subkeys != NULL);
    }

    TEST("hash - encodeData/DecodeData") {
        void *decoded;
        size_t old = server.swap_evict_step_max_subkeys;
        hashSwapData *hash1_data_ = (hashSwapData*)hash1_data;
        server.swap_evict_step_max_subkeys = 1024;
        kr1->b.num_subkeys = 0;
        kr1->b.subkeys = NULL;
        kr1->cmd_intention = SWAP_OUT, kr1->cmd_intention_flags = 0;
        zfree(hash1_ctx->ctx.sub.subkeys), hash1_ctx->ctx.sub.subkeys = NULL;
        hash1_ctx->ctx.sub.num = 0;
        hash1_data_->d.object_meta = createHashObjectMeta(0,1);
        swapDataAna(hash1_data,0,kr1,&intention,&intention_flags,hash1_ctx);
        test_assert(intention == SWAP_OUT && intention_flags == 0);
        test_assert(hash1_ctx->ctx.sub.num == (int)hashTypeLength(hash1_data_->d.value));
        serverAssert(hash1_ctx->ctx.sub.subkeys != NULL);

        hashSwapAnaAction(hash1_data,intention,hash1_ctx,&action);
        hashEncodeData(hash1_data,intention,hash1_ctx,&numkeys,&cfs,&rawkeys,&rawvals);
        test_assert(action == ROCKS_PUT);
        test_assert(numkeys == hash1_ctx->ctx.sub.num);

        hashDecodeData(hash1_data,numkeys,cfs,rawkeys,rawvals,&decoded);
        test_assert(hashTypeLength(decoded) == hashTypeLength(hash1));

        freeObjectMeta(hash1_data_->d.object_meta);
        hash1_data_->d.object_meta = NULL;
        server.swap_evict_step_max_subkeys = old;
    }

    TEST("hash - swapIn/swapOut") {
        robj *h, *decoded;
        objectMeta *m, *sm = createHashObjectMeta(0,0), *sm1, *sm2;
        hashSwapData _data = *(hashSwapData*)hash1_data, *data = &_data;
        test_assert(lookupMeta(db,key1) == NULL);

        /* hot => warm => cold */
        hashTypeDelete(hash1,f1);
        hashTypeDelete(hash1,f2);
        data->d.object_meta = NULL, data->d.new_meta = sm, sm->len = 2;
        hashSwapOut((swapData*)data, hash1_ctx, 0, NULL);
        test_assert((m =lookupMeta(db,key1)) != NULL && m->len == 2);
        test_assert(lookupKey(db,key1,LOOKUP_NOTOUCH) != NULL);
        test_assert(lookupKey(db, key1, LOOKUP_NOTOUCH)->persistent);

        hashTypeDelete(hash1,f3);
        hashTypeDelete(hash1,f4);
        data->d.object_meta = sm, data->d.new_meta = NULL, sm->len = 2;
        hashSwapOut((swapData*)data, hash1_ctx, 0, NULL);
        test_assert((m = lookupMeta(db,key1)) == NULL);
        test_assert((h = lookupKey(db,key1,LOOKUP_NOTOUCH)) == NULL);

        /* cold => warm => hot */
        decoded = createHashObject();
        sm1 = createHashObjectMeta(0,2);
        hashTypeSet(decoded,f1,sds1,HASH_SET_COPY);
        hashTypeSet(decoded,f2,sds2,HASH_SET_COPY);
        data->d.value = h;
        data->d.cold_meta = sm1, data->d.new_meta = NULL;
        hashSwapIn((swapData*)data,decoded,hash1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL && m->len == 2);
        test_assert((h = lookupKey(db,key1,LOOKUP_NOTOUCH)) != NULL);
        test_assert(hashTypeLength(h) == 2);
        test_assert(lookupKey(db, key1, LOOKUP_NOTOUCH)->persistent);

        decoded = createHashObject();
        hashTypeSet(decoded,f3,int1,HASH_SET_COPY);
        hashTypeSet(decoded,f4,int2,HASH_SET_COPY);
        data->d.value = h;
        data->d.object_meta = m;
        hashCreateOrMergeObject((swapData*)data,decoded,hash1_ctx);
        hashSwapIn((swapData*)data,NULL,hash1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL);
        test_assert((h = lookupKey(db,key1,LOOKUP_NOTOUCH)) != NULL);
        test_assert(hashTypeLength(h) == 4);
        test_assert(lookupKey(db, key1, LOOKUP_NOTOUCH)->persistent);

        /* hot => cold */
        hash1 = h;
        sm2 = createHashObjectMeta(0,4);
        hashTypeDelete(hash1,f1);
        hashTypeDelete(hash1,f2);
        hashTypeDelete(hash1,f3);
        hashTypeDelete(hash1,f4);
        *data = *(hashSwapData*)hash1_data;
        data->d.object_meta = NULL, data->d.new_meta = sm2;
        hashSwapOut((swapData*)data, hash1_ctx, 0, NULL);
        test_assert((m = lookupMeta(db,key1)) == NULL);
        test_assert((h = lookupKey(db,key1,LOOKUP_NOTOUCH)) == NULL);

        /* cold => hot */
        decoded = createHashObject();
        hashTypeSet(decoded,f1,sds1,HASH_SET_COPY);
        hashTypeSet(decoded,f2,sds2,HASH_SET_COPY);
        hashTypeSet(decoded,f3,int1,HASH_SET_COPY);
        hashTypeSet(decoded,f4,int2,HASH_SET_COPY);
        data->d.value = h;
        data->d.cold_meta = createHashObjectMeta(0,0);
        hashSwapIn((swapData*)data,decoded,hash1_ctx);
        test_assert((m = lookupMeta(db,key1)) != NULL);
        test_assert((h = lookupKey(db,key1,LOOKUP_NOTOUCH)) != NULL);
        test_assert(hashTypeLength(h) == 4);
        test_assert(lookupKey(db, key1, LOOKUP_NOTOUCH)->persistent);

        dbDelete(db,key1);
    }

    TEST("hash - rdbLoad & rdbSave") {
        int init_result;
        uint64_t V0 = 0, V1 = 1, Vcur = 2;
        server.hash_max_ziplist_entries = 16;
        int err = 0;
        sds myhash_key = sdsnew("myhash");
        robj *myhash = createHashObject();
        sds f1 = sdsnew("f1"), f2 = sdsnew("f2"), v1 = sdsnew("v1"), v2 = sdsnew("v2");
        sds rdbv1 = rocksEncodeValRdb(createStringObject("v1", 2));
        sds rdbv2 = rocksEncodeValRdb(createStringObject("v2", 2));
        hashTypeSet(myhash,f1,v1,HASH_SET_COPY);
        hashTypeSet(myhash,f2,v2,HASH_SET_COPY);
        hashTypeConvert(myhash,OBJ_ENCODING_HT);

        /* rdbLoad */
        rio sdsrdb;
        long long NOW = 1662552125000;
        sds rawval = rocksEncodeValRdb(myhash);
        rioInitWithBuffer(&sdsrdb,sdsnewlen(rawval+1,sdslen(rawval)-1));
        rdbKeyLoadData _load, *load = &_load;
        rdbKeyLoadDataInit(load,rawval[0],db,myhash_key,-1,NOW);
        sds metakey, metaval, subkey, subraw;
        int cont, cf;
        hashLoadStart(load,&sdsrdb,&cf,&metakey,&metaval,&err);
        test_assert(cf == META_CF && err == 0);
        test_assert(!sdscmp(metakey,rocksEncodeMetaKey(db,myhash_key)));
        cont = hashLoad(load,&sdsrdb,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 1 && err == 0);
        sdsfree(subkey), sdsfree(subraw);
        cont = hashLoad(load,&sdsrdb,&cf,&subkey,&subraw,&err);
        test_assert(cf == DATA_CF && cont == 0 && err == 0);
        test_assert(load->loaded_fields == 2);
        test_assert(load->swap_type == SWAP_TYPE_HASH);
        sdsfree(subkey), sdsfree(subraw);

        sds coldraw,warmraw,hotraw;
        objectMeta *object_meta = createHashObjectMeta(Vcur,2);
        sds extend = hashObjectMetaType.encodeObjectMeta(object_meta,0, NORMAL_MODE);

        rio rdbcold, rdbwarm, rdbhot;
        rdbKeySaveData _save, *save = &_save;

        /* save cold */
        decodedMeta _decoded_meta, *decoded_meta = &_decoded_meta;
        decoded_meta->cf = META_CF;
        decoded_meta->dbid = db->id;
        decoded_meta->expire = -1;
        decoded_meta->version = Vcur;
        decoded_meta->extend = extend;
        decoded_meta->key = myhash_key;
        decoded_meta->swap_type = SWAP_TYPE_HASH;

        rioInitWithBuffer(&rdbcold,sdsempty());

        decodedData _decoded_fx, *decoded_fx = &_decoded_fx;
        decoded_fx->cf = DATA_CF;
        decoded_fx->dbid = db->id;
        decoded_fx->key = myhash_key;
        decoded_fx->rdbtype = rdbv2[0];

        /* cold: skip orphan subkey */
        init_result = rdbKeySaveWarmColdInit(save,db,(decodedResult*)decoded_fx);
        test_assert(INIT_SAVE_SKIP == init_result);

        test_assert(!rdbKeySaveWarmColdInit(save,db,(decodedResult*)decoded_meta));
        test_assert(rdbKeySaveStart(save,&rdbcold) == 0);

        /* cold: skip old version subkey */
        decoded_fx->version = V0;
        test_assert(rdbKeySave(save,&rdbcold,decoded_fx) == 0);
        decoded_fx->version = V1;
        test_assert(rdbKeySave(save,&rdbcold,decoded_fx) == 0);

        decoded_fx->version = Vcur;
        decoded_fx->subkey = f2, decoded_fx->rdbraw = sdsnewlen(rdbv2+1, sdslen(rdbv2)-1);
        test_assert(rdbKeySave(save,&rdbcold,decoded_fx) == 0);
        sdsfree(decoded_fx->rdbraw);

        decoded_fx->key = myhash_key;
        decoded_fx->subkey = f1, decoded_fx->rdbraw = sdsnewlen(rdbv1+1,sdslen(rdbv1)-1);
        test_assert(rdbKeySave(save,&rdbcold,decoded_fx) == 0);
        sdsfree(decoded_fx->rdbraw);
        coldraw = rdbcold.io.buffer.ptr;

        /* save warm */
        rioInitWithBuffer(&rdbwarm,sdsempty());
        robj *value = createHashObject(), keyobj;
        hashTypeSet(value,f2,sdsnew("v2"),HASH_SET_TAKE_VALUE);
        initStaticStringObject(keyobj,myhash_key);
        dbAdd(db,&keyobj,value);
        object_meta->len = 1;
        dbAddMeta(db,&keyobj,object_meta);

        /* warm: skip orphan subkey */
        init_result = rdbKeySaveWarmColdInit(save,db,(decodedResult*)decoded_fx);
        test_assert(INIT_SAVE_SKIP == init_result);

        test_assert(!rdbKeySaveWarmColdInit(save,db,(decodedResult*)decoded_meta));
        test_assert(rdbKeySaveStart(save,&rdbwarm) == 0);

        /* warm: skip old version subkey */
        decoded_fx->version = V0;
        test_assert(rdbKeySave(save,&rdbwarm,decoded_fx) == 0);
        decoded_fx->version = V1;
        test_assert(rdbKeySave(save,&rdbwarm,decoded_fx) == 0);

        decoded_fx->version = Vcur;
        decoded_fx->subkey = f1, decoded_fx->rdbraw = sdsnewlen(rdbv1+1,sdslen(rdbv1)-1);
        test_assert(rdbKeySave(save,&rdbwarm,decoded_fx) == 0);
        sdsfree(decoded_fx->rdbraw);

        warmraw = rdbwarm.io.buffer.ptr;
        dbDelete(db,&keyobj);

        /* save hot */
        rioInitWithBuffer(&rdbhot,sdsempty());
        initStaticStringObject(keyobj,myhash_key);
        test_assert(rdbSaveKeyValuePair(&rdbhot,&keyobj,myhash,-1) != -1);
        hotraw = rdbhot.io.buffer.ptr;

        test_assert(!sdscmp(hotraw,coldraw) && !sdscmp(hotraw,warmraw));

        sdsfree(f1), sdsfree(f2), sdsfree(v1), sdsfree(v2);
        sdsfree(rdbv1), sdsfree(rdbv2);
        sdsfree(myhash_key);
    }

    TEST("hash - deinit") {
        sdsfree(f1), sdsfree(f2), sdsfree(f3), sdsfree(f4);
        sdsfree(sds1), sdsfree(sds2);
        sdsfree(int1), sdsfree(int2);
    }

    server.swap_evict_step_max_subkeys = originEvictStepMaxSubkey;
    server.swap_evict_step_max_memory = originEvictStepMaxMemory;

    return error;
}

#endif

