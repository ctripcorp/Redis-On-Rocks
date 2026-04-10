#include "ctrip_storage_request.h"
#include "ctrip_storage_client.h"
#include "ctrip_storage_request_utils.h"
#include "ctrip_storage_scan.h"
#include <ctype.h>
/* MetaScanDataCtx */
void metaScanDataCtxSwapAna(metaScanDataCtx *datactx, int *intention,
        uint32_t *intention_flags) {
    if (datactx->type->swapAna) {
        datactx->type->swapAna(datactx,intention,intention_flags);
    }
}

/* MetaScan */
int metaScanSwapAna(swapData *data, int thd, struct keyRequest *req,
        int *intention, uint32_t *intention_flags, void *datactx_) {
    UNUSED(data),UNUSED(req),UNUSED(thd);
    metaScanDataCtx *datactx = datactx_;
    metaScanDataCtxSwapAna(datactx,intention,intention_flags);
    return 0;
}

int SwapAnaAction(swapData *data, int intention, void *datactx_, int *action) {
    UNUSED(data), UNUSED(datactx_);
    switch (intention) {
        case SWAP_IN:
            *action = ROCKS_ITERATE;
            break;
        default:
            *action = ROCKS_NOP;
            return SWAP_ERR_DATA_UNEXPECTED_INTENTION;
    }
    return 0;
}

int metaScanEncodeRange(struct swapData *data, int intention, void *datactx_, int *limit,
        uint32_t *flags, int *pcf, sds *start, sds *end) {
    metaScanDataCtx *datactx = datactx_;
    serverAssert(SWAP_IN == intention);
    *pcf = META_CF;
    /* IMPORTANT:
     * - Meta keys are encoded with dbid prefix (see encodeMetaKey()).
     * - SCAN should only iterate the current db's meta range, otherwise
     *   it may walk into other dbs' meta keys and never finish "within db",
     *   causing extremely long scans and huge amounts of "type none" work.
     *
     * So we set an explicit high bound (dbid+1) for the meta scan range. */
    *flags |= ROCKS_ITERATE_CONTINUOUSLY_SEEK;
    *start = rocksEncodeMetaKey(data->db,datactx->seek);
    *end = rocksEncodeDbRangeEndKey(data->db->id);
    *limit = datactx->limit;
    return 0;
}

void metaScanResultMakeRoom(metaScanResult *result, int num) {
	/* Resize if necessary */
	if (num > result->size) {
		if (result->metas != result->buffer) {
			/* We're not using a static buffer, just (re)alloc */
			result->metas = zrealloc(result->metas,
                    num*sizeof(scanMeta));
		} else {
			/* We are using a static buffer, copy its contents */
			result->metas = zmalloc(num*sizeof(scanMeta));
			if (result->num) {
				memcpy(result->metas,result->buffer,
                        result->num*sizeof(scanMeta));
            }
		}
		result->size = num;
	}
}

void metaScanResultAppend(metaScanResult *result, int swap_type, sds key, long long expire) {
    if (result->num == result->size) {
        int newsize = result->size +
            (result->size > 1024 ? 1024 : result->size);
        metaScanResultMakeRoom(result, newsize);
    }

    scanMeta *meta = &result->metas[result->num++];
    scanMetaInit(meta,swap_type,key,expire);
}

void metaScanResultSetNextSeek(metaScanResult *result, sds nextseek) {
    result->nextseek = nextseek;
}


int metaScanDecodeData(swapData *data, int num, int *cfs, sds *rawkeys,
        sds *rawvals, void **pdecoded) {
    int i, retval = 0;
    metaScanResult *result = metaScanResultCreate();
    sds nextseek_rawkey = data->nextseek;

    /* last entry in rawkeys is nextseek, NULL if iterate EOF. */
    if (nextseek_rawkey) {
        const char *nextseek;
        size_t seeklen;
        int dbid = -1;
        if (rocksDecodeMetaKey(nextseek_rawkey,sdslen(nextseek_rawkey),&dbid,
                &nextseek,&seeklen) == 0 && dbid == data->db->id) {
            metaScanResultSetNextSeek(result, sdsnewlen(nextseek,seeklen));
        } else {
            /* Out of current db's range: treat as EOF for this db scan. */
            metaScanResultSetNextSeek(result, NULL);
        }
        sdsfree(data->nextseek);
        data->nextseek = NULL, nextseek_rawkey = NULL;
    }

    for (i = 0; i < num; i++) {
        const char *key;
        size_t keylen;
        long long expire;
        int swap_type;
        int dbid = -1;

        serverAssert(cfs[i] == META_CF);
        if (rocksDecodeMetaKey(rawkeys[i],sdslen(rawkeys[i]),
                &dbid,&key,&keylen)) {
            retval = SWAP_ERR_DATA_DECODE_FAIL;
            break;
        }
        /* Skip keys that are not in this db (shouldn't happen with end bound,
         * but keep it defensive for safety). */
        if (dbid != data->db->id) continue;
        if (rocksDecodeMetaVal(rawvals[i],sdslen(rawvals[i]),
                &swap_type,&expire,NULL,NULL,NULL)) {
            retval = SWAP_ERR_DATA_DECODE_FAIL;
            break;
        }

        metaScanResultAppend(result,swap_type,sdsnewlen(key,keylen),expire);
    }

    if (pdecoded) *pdecoded = (robj*)result;

    return retval;
}

void metaScanDataCtxSwapIn(metaScanDataCtx *datactx, metaScanResult *result) {
    if (datactx->type->swapIn) {
        datactx->type->swapIn(datactx,result);
    }
}

void scanMetaDeinit(scanMeta *meta) {
    if (meta->key) sdsfree(meta->key);
    meta->key = NULL;
    meta->expire = -1;
    meta->swap_type = -1;
}


void freeScanMetaResult(metaScanResult *result) {
    if (result == NULL) return;
    if (result->nextseek) {
        sdsfree(result->nextseek);
        result->nextseek = NULL;
    }
    for (int i = 0; i < result->num; i++) {
        scanMetaDeinit(&result->metas[i]);
    }
    result->num = 0;
    if (result->metas != result->buffer) {
        zfree(result->metas);
        result->metas = NULL;
    }
    zfree(result);
}

int metaScanSwapIn(swapData *data, void **result_, void *datactx_) {
    metaScanDataCtx *datactx = datactx_;
    metaScanResult *result = *result_;
    client *c = datactx->c;
    UNUSED(data);
    if (c->deferred_cmd->swap_metas) freeScanMetaResult(c->deferred_cmd->swap_metas);
    c->deferred_cmd->swap_metas = result;
    metaScanDataCtxSwapIn(datactx,result);
    return 0;
}

void *metaScanCreateOrMergeObject(swapData *data, void *decoded, void *datactx) {
    UNUSED(data), UNUSED(datactx);
    return decoded;
}

void freeMetaScanSwapData(swapData *data, void *datactx_) {
    UNUSED(data);
    metaScanDataCtx *datactx = datactx_;
    if (datactx == NULL) return;
    if (datactx->extend) {
        if (datactx->type->freeExtend)
            datactx->type->freeExtend(datactx->extend);
        else
            zfree(datactx->extend);
        datactx->extend = NULL;
    }
    if (datactx->seek) {
        sdsfree(datactx->seek);
        datactx->seek = NULL;
    }
    zfree(datactx);
}


swapDataType metaScanSwapDataType = {
    .name = "metascan",
    .swapAna = metaScanSwapAna,
    .swapAnaAction = SwapAnaAction,
    .encodeKeys = NULL,
    .encodeData = NULL,
    .encodeRange = metaScanEncodeRange,
    .decodeData = metaScanDecodeData,
    .swapIn = metaScanSwapIn,
    .swapOut = NULL,
    .swapDel = NULL,
    .createOrMergeObject = metaScanCreateOrMergeObject,
    .cleanObject = NULL,
    .beforeCall = NULL,
    .free = freeMetaScanSwapData,
};


/* setupMetaScanDataCtx4Scan */


swapScanSession *swapScanSessionsFind(swapScanSessions *sessions,
        unsigned long outer_cursor) {
    uint64_t id = sessionId2RaxKey(cursorGetSessionId(outer_cursor));

    swapScanSession *session = NULL;
    if (!raxFind(sessions->assigned,
            (unsigned char*)&id, sizeof(id), &session)) session = NULL;

    // TODO remove serverLog(LL_WARNING, "[xxx] find %lu => %lu", id, (session ? session->session_id : 999));

    return session;
}

swapScanSession *swapScanSessionsBind(swapScanSessions *sessions,
        unsigned long outer_cursor, int *reason) {
    swapScanSession *session;

    /* session not found: invalid cursor, can't scan cold keys from
     * arbitary cursor. */
    session = swapScanSessionsFind(server.storage.swap_scan_sessions, outer_cursor);
    if (session == NULL) {
        if (reason) *reason = SWAP_ERR_METASCAN_SESSION_UNASSIGNED;
        goto fail;
    }

    serverAssert(cursorGetSessionId(outer_cursor) == session->session_id);

    /* session inprogress: can't scan concurrently */
    if (session->binded) {
        if (reason) *reason = SWAP_ERR_METASCAN_SESSION_INPROGRESS;
        goto fail;
    }

    /* cursor not continuos: must use cursor return previously. */
    if (session->nextcursor != cursorOuterToInternal(outer_cursor)) {
        if (reason) *reason = SWAP_ERR_METASCAN_SESSION_SEQUNMATCH;
        goto fail;
    }

    session->last_active = server.mstime;
    session->binded = 1;
    sessions->stat.bind_succeded++;

    return session;

fail:
    sessions->stat.bind_failed++;
    return NULL;
}

void swapScanSessionUnbind(swapScanSession *session, MOVE sds nextseek) {
    if (session->nextseek) {
        sdsfree(session->nextseek);
        session->nextseek = NULL;
    }
    session->nextseek = nextseek;

    if (nextseek) {
        swapScanSessionIncrNextCursor(session);
    } else {
        swapScanSessionZeroNextCursor(session);
    }

    session->binded = 0;
    session->last_active = server.mstime;
}


void metaScanDataCtxScanSwapAna(metaScanDataCtx *datactx,
        int *intention, uint32_t *intention_flags) {
    metaScanDataCtxScan *scanctx = datactx->extend;
    if (scanctx == NULL) { /* Hot cursor */
        *intention = SWAP_NOP;
        *intention_flags = 0;
    } else {
        *intention = SWAP_IN;
        *intention_flags = 0;
    }
}

void metaScanDataCtxScanSwapIn(struct metaScanDataCtx *datactx, metaScanResult *result) {
    metaScanDataCtxScan *scanctx = datactx->extend;
    swapScanSessionUnbind(scanctx->session, result->nextseek);
    result->nextseek = NULL; /* moved */
}

metaScanDataCtxType scanMetaScanDataCtxType = {
    .swapAna = metaScanDataCtxScanSwapAna,
    .swapIn = metaScanDataCtxScanSwapIn,
    .freeExtend = NULL,
};

static inline int parseScanCursor(robj *o, unsigned long *cursor) {
    char *eptr;
    errno = 0;
    *cursor = strtoul(o->ptr, &eptr, 10);
    if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' || errno == ERANGE)
        return -1;
    return 0;
}
int setupMetaScanDataCtx4Scan(metaScanDataCtx *datactx, client *c) {
    int i, j, reason = 0;
    unsigned long outer_cursor;
    swapScanSession *session;

    datactx->type = &scanMetaScanDataCtxType;

    /* Not supported yet (maybe encode encode cursor to requestkey). */
    if (c->argc < 2 || c->argv[1] == NULL) {
        return SWAP_ERR_METASCAN_UNSUPPORTED_IN_MULTI;
    }

    /* No swap needed if cursor is invalid or hot. */
    if (parseScanCursor(c->argv[1],&outer_cursor) ||
            cursorIsHot(outer_cursor)) {
        datactx->extend = NULL;
        return 0;
    }

    session = swapScanSessionsBind(server.storage.swap_scan_sessions,
            outer_cursor, &reason);
    if (session == NULL) return reason;

    datactx->limit = 10;
    for (i = 2; i < c->argc; i+=2) {
        j = c->argc - i;
        if (!strcasecmp(c->argv[i]->ptr, "count") && j >= 2) {
            long long value;
            if (getLongLongFromObject(c->argv[i+1],&value) == C_OK) {
                datactx->limit = value;
                break;
            }
        }
    }

    metaScanDataCtxScan *scanctx = zmalloc(sizeof(metaScanDataCtxScan));
    scanctx->session = session;
    if (session->nextseek) datactx->seek = sdsdup(session->nextseek);
    datactx->extend = scanctx;

    return 0;
}

/* setupMetaScanDataCtx4Randomkey */
/* metaScanDataCtx - Randomkey */
#define METASCAN_RANDOMKEY_DEFAULT_LIMIT 16

void metaScanDataCtxRandomkeySwapAna(metaScanDataCtx *datactx,
        int *intention, uint32_t *intention_flags) {
    UNUSED(datactx);
    *intention = SWAP_IN;
    *intention_flags = 0;
}

void metaScanDataCtxRandomkeySwapIn(struct metaScanDataCtx *datactx,
        metaScanResult *result) {
    metaScanDataCtxRandomkey *randomkeyctx = datactx->extend;
    redisDb *db = randomkeyctx->db;

    if (db->storage.randomkey_nextseek) {
        sdsfree(db->storage.randomkey_nextseek);
        db->storage.randomkey_nextseek = NULL;
    }

    if (result->nextseek) {
        db->storage.randomkey_nextseek = result->nextseek;
        result->nextseek = NULL;
    }
}

metaScanDataCtxType randomkeyMetaScanDataCtxType = {
    .swapAna = metaScanDataCtxRandomkeySwapAna,
    .swapIn = metaScanDataCtxRandomkeySwapIn,
    .freeExtend = NULL,
};

int setupMetaScanDataCtx4Randomkey(metaScanDataCtx *datactx, client *c) {
    metaScanDataCtxRandomkey *randomkeyctx;
    redisDb *db = c->db;
    datactx->type = &randomkeyMetaScanDataCtxType;
    datactx->limit = METASCAN_RANDOMKEY_DEFAULT_LIMIT;
    if (db->storage.randomkey_nextseek)
        datactx->seek = sdsdup(db->storage.randomkey_nextseek);
    else
        datactx->seek = NULL;
    randomkeyctx = zmalloc(sizeof(metaScanDataCtxRandomkey));
    randomkeyctx->db = c->db;
    datactx->extend = randomkeyctx;
    return 0;
}

/* setupMetaScanDataCtx4ScanExpire */
void metaScanDataCtxScanExpireSwapAna(metaScanDataCtx *datactx,
        int *intention, uint32_t *intention_flags) {
    UNUSED(datactx);
    *intention = SWAP_IN;
    *intention_flags = 0;
}

void metaScanDataCtxScanExpireSwapIn(struct metaScanDataCtx *datactx,
        metaScanResult *result) {
    metaScanDataCtxScanExpire *expirectx = datactx->extend;
    scanExpire *scan_expire = expirectx->scan_expire;
    if (scan_expire->nextseek) {
        sdsfree(scan_expire->nextseek);
        scan_expire->nextseek = NULL;
    }

    if (result->nextseek) {
        scan_expire->nextseek = result->nextseek;
        result->nextseek = NULL;
    }
}

metaScanDataCtxType expireMetaScanDataCtxType = {
    .swapAna = metaScanDataCtxScanExpireSwapAna,
    .swapIn = metaScanDataCtxScanExpireSwapIn,
    .freeExtend = NULL,
};

int setupMetaScanDataCtx4ScanExpire(metaScanDataCtx *datactx, client *c) {
    metaScanDataCtxScanExpire *expirectx;
    scanExpire *scan_expire = c->db->storage.scan_expire;
    datactx->type = &expireMetaScanDataCtxType;
    datactx->limit = scan_expire->limit;
    if (scan_expire->nextseek)
        datactx->seek = sdsdup(scan_expire->nextseek);
    else
        datactx->seek = NULL;
    expirectx = zmalloc(sizeof(metaScanDataCtxScanExpire));
    expirectx->scan_expire = scan_expire;
    datactx->extend = expirectx;
    return 0;
}


#define METASCAN_DEFAULT_LIMIT 16
int swapDataSetupMetaScan(swapData *data, uint32_t intention_flags,
        client *c, void **pdatactx) {
    int retval;

    data->type = &metaScanSwapDataType;
    data->expire = -1;
    /* use shared object to mock that metascan is a hot swapin, so
     * db.cold_keys wouldn't be wrongly updated by exec.  */
    data->key = shared.redacted;
    data->value = shared.redacted;

    metaScanDataCtx *datactx = NULL;
    datactx = zmalloc(sizeof(metaScanDataCtx));
    datactx->c = c;
    datactx->limit = METASCAN_DEFAULT_LIMIT;
    datactx->seek = NULL;
    datactx->extend = NULL;

    if (c == NULL) {
        return SWAP_ERR_SETUP_FAIL;
    } else if (intention_flags & SWAP_METASCAN_SCAN) {
        retval = setupMetaScanDataCtx4Scan(datactx,c);
    } else if (intention_flags & SWAP_METASCAN_RANDOMKEY) {
        retval = setupMetaScanDataCtx4Randomkey(datactx,c);
    } else if (intention_flags & SWAP_METASCAN_EXPIRE) {
        retval = setupMetaScanDataCtx4ScanExpire(datactx,c);
    } else {
        retval = SWAP_ERR_SETUP_FAIL;
    }

    *pdatactx = datactx;
    return retval;
}