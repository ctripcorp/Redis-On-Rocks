#include "ctrip_storage_objects.h"

#include "ctrip_storage_request.h" 
#include "ctrip_storage_request_utils.h"

/* ------------------- whole key swap data ----------------------------- */
int wholeKeySwapAna(swapData *data, int thd, struct keyRequest *req,
        int *intention, uint32_t *intention_flags, void *datactx_) {
    /* for string type, store ctx_flag in struct swapData's `void *extends[2];` */
    long *datactx = datactx_;
    int cmd_intention = req->cmd_intention;
    uint32_t cmd_intention_flags = req->cmd_intention_flags;

    switch(cmd_intention) {
    case SWAP_NOP:
        *intention = SWAP_NOP;
        *intention_flags = 0;
        break;
    case SWAP_IN:
        if (!data->value) {
            if (cmd_intention_flags & SWAP_IN_DEL) {
                *intention = SWAP_IN;
                *intention_flags = SWAP_EXEC_IN_DEL;
            } else if (cmd_intention_flags & SWAP_IN_DEL_MOCK_VALUE) {
                /* DEL/UNLINK: Lazy delete current key. */
                *datactx |= BIG_DATA_CTX_FLAG_MOCK_VALUE;
                *intention = SWAP_DEL;
                *intention_flags = SWAP_FIN_DEL_SKIP;
            } else {
                *intention = SWAP_IN;
                *intention_flags = 0;
            }
        } else if (data->value) {
            if ((cmd_intention_flags & SWAP_IN_DEL) ||
                    (cmd_intention_flags & SWAP_IN_DEL_MOCK_VALUE) ||
                    cmd_intention_flags & SWAP_IN_FORCE_HOT) {
                *intention = SWAP_DEL;
                *intention_flags = SWAP_FIN_DEL_SKIP;
            } else {
                *intention = SWAP_NOP;
                *intention_flags = 0;
            }
        } else {
            *intention = SWAP_NOP;
            *intention_flags = 0;
        }
        break;
    case SWAP_OUT:
        if (data->value) {
            /* we can always keep data and clear dirty after persist string */
            int keep_data = swapDataPersistKeepData(data,cmd_intention_flags,1);
            if (objectIsDirty(data->value)) {
                *intention = SWAP_OUT;
                *intention_flags = keep_data ? SWAP_EXEC_OUT_KEEP_DATA : 0;
            } else {
                serverAssert(thd == SWAP_ANA_THD_MAIN);
                /* Not dirty: swapout right away without swap. */
                if (!keep_data) swapDataTurnCold(data);
                swapDataSwapOut(data,NULL,keep_data,NULL);
                *intention = SWAP_NOP;
                *intention_flags = 0;
            }
        } else {
            *intention = SWAP_NOP;
            *intention_flags = 0;
        }
        break;
    case SWAP_DEL:
        *intention = SWAP_DEL;
        *intention_flags = 0;
        break;
    default:
        break;
    }

    return 0;
}

int wholeKeySwapAnaAction(swapData *data, int intention, void *datactx_, int *action) {
    UNUSED(data), UNUSED(datactx_);
    switch (intention) {
        case SWAP_IN:
            *action = ROCKS_GET;
            break;
        case SWAP_DEL:
            *action = ROCKS_DEL;
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

int wholeKeyEncodeKeys(swapData *data, int intention, void *datactx,
        int *numkeys, int **pcfs, sds **prawkeys) {
    sds *rawkeys = zmalloc(sizeof(sds));
    int *cfs = zmalloc(sizeof(int));

    UNUSED(datactx);
    serverAssert(intention == SWAP_IN || intention == SWAP_DEL);
    rawkeys[0] = rocksEncodeDataKey(data->db,data->key->ptr,SWAP_VERSION_ZERO,NULL);
    cfs[0] = DATA_CF;
    *numkeys = 1;
    *prawkeys = rawkeys;
    *pcfs = cfs;

    return 0;
}

static sds wholeKeyEncodeDataKey(swapData *data) {
    return data->key ? rocksEncodeDataKey(data->db,data->key->ptr,SWAP_VERSION_ZERO,NULL) : NULL;
}

sds rocksEncodeValRdb(robj *value) {
    rio sdsrdb;
    rioInitWithBuffer(&sdsrdb,sdsempty());
    rdbSaveObjectType(&sdsrdb,value) ;
    rdbSaveObject(&sdsrdb,value,NULL,0);
    return sdsrdb.io.buffer.ptr;
}
static sds wholeKeyEncodeDataVal(swapData *data) {
    return data->value ? rocksEncodeValRdb(data->value) : NULL;
}

int wholeKeyEncodeData(swapData *data, int intention, void *datactx,
        int *numkeys, int **pcfs, sds **prawkeys, sds **prawvals) {
    UNUSED(datactx);
    serverAssert(intention == SWAP_OUT);
    sds *rawkeys = zmalloc(sizeof(sds));
    sds *rawvals = zmalloc(sizeof(sds));
    int *cfs = zmalloc(sizeof(int));
    rawkeys[0] = wholeKeyEncodeDataKey(data);
    rawvals[0] = wholeKeyEncodeDataVal(data);
    cfs[0] = DATA_CF;
    *numkeys = 1;
    *prawkeys = rawkeys;
    *prawvals = rawvals;
    *pcfs = cfs;
    return 0;
}

/* decoded move to exec module */
int wholeKeyDecodeData(swapData *data, int num, int *cfs, sds *rawkeys,
        sds *rawvals, void **pdecoded) {
    serverAssert(num == 1);
    UNUSED(data);
    UNUSED(rawkeys);
    UNUSED(cfs);
    sds rawval = rawvals[0];
    *pdecoded = rocksDecodeValRdb(rawval);
    return 0;
}

/* If maxmemory policy is not LRU/LFU, rdbLoadObject might return shared
 * object, but swap needs individual object to track dirty/evict flags. */
robj *dupSharedObject(robj *o) {
    switch(o->type) {
    case OBJ_STRING:
        return dupStringObject(o);
    case OBJ_HASH:
    case OBJ_LIST:
    case OBJ_SET:
    case OBJ_ZSET:
    default:
        return NULL;
    }
}
static robj *createSwapInObject(MOVE robj *newval) {
    robj *swapin = newval;
    serverAssert(newval);
    /* Copy swapin object before modifing If newval is shared object. */
    if (newval->refcount == OBJ_SHARED_REFCOUNT)
        swapin = dupSharedObject(newval);
    clearObjectDirty(swapin);
    clearObjectPersistKeep(swapin);
    return swapin;
}

int wholeKeySwapIn(swapData *data, MOVE void **result, void *datactx) {
    UNUSED(datactx);
    robj *swapin;
    serverAssert(data->value == NULL);
    swapin = createSwapInObject(*result);
    /* mark persistent after data swap in without
     * persistence deleted, or mark non-persistent else */
    overwriteObjectPersistent(swapin,!data->persistence_deleted);
    *result = dbAdd(data->db,data->key,&swapin);
    return 0;
}

int wholeKeySwapOut(swapData *data, void *datactx, int keep_data, int *totally_out) {
    UNUSED(datactx);
    redisDb *db = data->db;
    robj *key = data->key;
    if (keep_data) {
        setObjectPersistent(data->value);
        clearObjectDataDirty(data->value);
        if (totally_out) *totally_out = 0;
    } else {
        if (kvstoreSize(db->keys) > 0) dbDelete(db, key);
        if (totally_out) *totally_out = 1;
    }
    return 0;
}

static void createFakeWholeKeyForDeleteIfCold(swapData *data) {
	if (swapDataIsCold(data)) {
        /* empty whole key allowed */
        robj* val = createStringObject("", 0);
        dbAdd(data->db,data->key,&val);
	}
}

int wholeKeySwapDel(swapData *data, void *datactx_, int async) {
    redisDb *db = data->db;
    robj *key = data->key;

    long *datactx = datactx_;
    if (*datactx & BIG_DATA_CTX_FLAG_MOCK_VALUE) {
        createFakeWholeKeyForDeleteIfCold(data);
    }

    if (async) return 0;
    if (data->value) kvstoreDictDelete(db->keys, getKeySlot(key->ptr),key->ptr);
    return 0;
}

/* decoded moved back by exec to wholekey then moved to exec again. */
void *wholeKeyCreateOrMergeObject(swapData *data, void *decoded, void *datactx) {
    UNUSED(data);
    UNUSED(datactx);
    serverAssert(decoded);
    return decoded;
}

static void tryTransStringToBitmap(redisDb *db, robj *key) {
    if (!server.storage.swap_bitmap_subkeys_enabled) return;
    if (bitmapSetObjectMarkerIfNotExist(db,key) == 1) {
        atomicIncr(server.storage.swap_string_switched_to_bitmap_count, 1);
    }
}


int wholeKeyBeforeCall(swapData *data, keyRequest *key_request,
        client *c, void *datactx)  {
    UNUSED(data), UNUSED(c), UNUSED(datactx);
    robj *o = lookupKeyReadWithFlags(data->db, data->key, LOOKUP_NOTOUCH);
    if ((key_request->cmd_flags & CMD_SWAP_DATATYPE_BITMAP) && o) {
        tryTransStringToBitmap(data->db,data->key);
    }
    return 0;
}
#define wholeKeyMergedIsHot swapDataObjectMergedIsHot
swapDataType wholeKeySwapDataType = {
    .name = "wholekey",
    .cmd_swap_flags = CMD_SWAP_DATATYPE_STRING,
    .swapAna = wholeKeySwapAna,
    .swapAnaAction = wholeKeySwapAnaAction,
    .encodeKeys = wholeKeyEncodeKeys,
    .encodeData = wholeKeyEncodeData,
    .decodeData = wholeKeyDecodeData,
    .encodeRange = NULL,
    .swapIn = wholeKeySwapIn,
    .swapOut = wholeKeySwapOut,
    .swapDel = wholeKeySwapDel,
    .createOrMergeObject = wholeKeyCreateOrMergeObject,
    .cleanObject = NULL,
    .beforeCall = wholeKeyBeforeCall,
    .free = NULL,
    .rocksDel = NULL,
    .mergedIsHot = wholeKeyMergedIsHot,
};

int wholeKeyIsHot(struct objectMeta *om, robj *value) {
    UNUSED(om);
    return value != NULL;
}

objectMetaType wholekeyObjectMetaType = {
    .encodeObjectMeta = NULL,
    .decodeObjectMeta = NULL,
    .objectIsHot = wholeKeyIsHot
};
int swapDataSetupWholeKey(swapData *d, OUT void **pdatactx) {
    d->type = &wholeKeySwapDataType;
    d->omtype = &wholekeyObjectMetaType;
    /* for string type, store ctx_flag in struct swapData's `void *extends[2];` */
    long *datactx = (long*)d->extends;
    *datactx = BIG_DATA_CTX_FLAG_NONE;
    *pdatactx = d->extends;
    return 0;
}