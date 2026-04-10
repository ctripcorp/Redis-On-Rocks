#include "ctrip_storage_objects.h"
#include "ctrip_storage_filter.h"

objectMetaType lenObjectMetaType = {
    // .encodeObjectMeta = encodeLenObjectMeta,
    // .decodeObjectMeta = decodeLenObjectMeta,
    // .objectIsHot = lenObjectMetaIsHot,
    // .free = NULL,
    // .duplicate = NULL,
    // .equal = lenObjectMetaEqual,
    // .rebuildFeed = lenObjectMetaRebuildFeed,
};



#define SWAP_PERSIST_STATE_TODO  0
#define SWAP_PERSIST_STATE_DOING 1
persistingKeyEntry *persistingKeyEntryNew(listNode *ln, uint64_t version,
        mstime_t mstime) {
    persistingKeyEntry *e = zmalloc(sizeof(persistingKeyEntry));
    e->ln = ln;
    e->version = version;
    e->mstime = mstime;
    e->state = SWAP_PERSIST_STATE_TODO;
    return e;
}
int persistingKeysPut(persistingKeys *keys, sds key, uint64_t version,
        mstime_t time) {
    dictEntry *de;
    listNode *ln;
	persistingKeyEntry *entry;

	if ((de = dictFind(keys->map,key))) {
		entry = dictGetVal(de);
		serverAssert(version > entry->version);
		entry->version = version;
        /* old time will be reserved */
        return 0;
    } else {
        sds dup = sdsdup(key);
        listAddNodeTail(keys->todo,dup);
        ln = listLast(keys->todo);
		entry = persistingKeyEntryNew(ln,version,time);
		dictAdd(keys->map,dup,entry);
        return 1;
    }
}

void swapPersistCtxAddKey(swapPersistCtx *ctx, redisDb *db, robj *key) {
    persistingKeys *keys = ctx->keys[db->id];
    uint64_t persist_version = ctx->version++;
    if (persistingKeysPut(keys,key->ptr,persist_version,server.mstime))
        ctx->stat.add_succ++;
    else
        ctx->stat.add_ignored++;
}


static sds _rocksEncodeDataKey(int dbid, sds key, uint64_t version,
        uint8_t subkeyflag, sds subkey) {
    keylen_t keylen = key ? sdslen(key) : 0;
    keylen_t subkeylen = subkey ? sdslen(subkey) : 0;
    uint64_t encoded_version = rocksEncodeVersion(version);
    size_t rawkeylen = sizeof(dbid)+sizeof(keylen)+keylen+
        sizeof(encoded_version)+1+subkeylen;
    sds rawkey = sdsnewlen(SDS_NOINIT,rawkeylen), ptr = rawkey;
    memcpy(ptr, &dbid, sizeof(dbid)), ptr += sizeof(dbid);
    memcpy(ptr, &keylen, sizeof(keylen_t)), ptr += sizeof(keylen_t);
    memcpy(ptr, key, keylen), ptr += keylen;
    memcpy(ptr, &encoded_version, sizeof(encoded_version));
    ptr += sizeof(encoded_version);
    ptr[0] = subkeyflag, ptr++;
    if (subkeyflag == ROCKS_KEY_FLAG_SUBKEY) {
        memcpy(ptr, subkey, subkeylen), ptr += subkeylen;
    }
    return rawkey;
}


sds rocksEncodeDataKey(redisDb *db, sds key, uint64_t version, sds subkey) {
    if (subkey) {
        return _rocksEncodeDataKey(db->id,key,version,ROCKS_KEY_FLAG_SUBKEY,subkey);
    } else {
        return _rocksEncodeDataKey(db->id,key,version,ROCKS_KEY_FLAG_NONE,NULL);
    }
}

robj *rocksDecodeValRdb(sds raw) {
    robj *value;
    rio sdsrdb;
    int rdbtype;
    rioInitWithBuffer(&sdsrdb,raw);
    rdbtype = rdbLoadObjectType(&sdsrdb);
    value = rdbLoadObject(rdbtype,&sdsrdb,NULL,0,NULL);
    return value;
}

objectMeta *lookupMeta(redisDb *db, robj *key) {
    return dictFetchValue(db->storage.meta,key->ptr);
}

void dbAddMeta(redisDb *db, robj *key, objectMeta *m) {
    dictEntry *kde;
    // kde = dictFind(db->dict,key->ptr);
    kde = kvstoreDictFind(db->keys, getKeySlot(key->ptr), key->ptr);
    kvobj* kv = dictGetKV(kde);
    serverAssertWithInfo(NULL,key,kv != NULL);
    serverAssert(dictAdd(db->storage.meta,kvobjGetKey(kv),m) == DICT_OK);
    serverAssert(dictSize(db->storage.meta) != 0);
}





int swapDataObjectMergedIsHot(swapData *d, void *result, void *datactx) {
    objectMeta *object_meta = swapDataObjectMeta(d);
    robj *value = swapDataIsCold(d) ? result : d->value;
    UNUSED(datactx);
    return keyIsHot(object_meta,value);
}

int rocksDecodeDataKey(const char *raw, size_t rawlen, int *dbid,
        const char **key, size_t *keylen, uint64_t *version,
        const char **subkey, size_t *subkeylen) {
    keylen_t keylen_;
    uint64_t encoded_version;
    if (raw == NULL || rawlen < sizeof(int)+sizeof(keylen_t)+sizeof(encoded_version)+1) return -1;
    if (dbid) *dbid = *(int*)raw;
    raw += sizeof(int), rawlen -= sizeof(int);
    keylen_ = *(keylen_t*)raw;
    if (keylen) *keylen = keylen_;
    raw += sizeof(keylen_t), rawlen -= sizeof(keylen_t);
    if (key) *key = raw;
    if (rawlen < keylen_) return -1;
    raw += keylen_, rawlen -= keylen_;
    if (rawlen < sizeof(encoded_version)) return -1;
    if (version) {
        encoded_version = *(uint64_t*)raw;
        *version = rocksDecodeVersion(encoded_version);
    }
    raw += sizeof(encoded_version), rawlen -= sizeof(encoded_version);
    if (subkeylen) *subkeylen = rawlen - 1;
    if (subkey) {
        *subkey = raw[0] == ROCKS_KEY_FLAG_SUBKEY ? raw + 1 : NULL;
    }
    return 0;
}

/* Swap-thread: decide how to encode keys by data and intention. */
inline int swapDataEncodeKeys(swapData *d, int intention, void *datactx,
        int *numkeys, int **cfs, sds **rawkeys) {
    if (d->type->encodeKeys)
        return d->type->encodeKeys(d,intention,datactx,numkeys,cfs,rawkeys);
    else
        return 0;
}

/* Swap-thread: decode how to encode val/subval by data and intention.
 * dataactx can be used store context of which subvals are encoded. */
inline int swapDataEncodeData(swapData *d, int intention, void *datactx,
        int *numkeys, int **cfs, sds **rawkeys, sds **rawvals) {
    if (d->type->encodeData)
        return d->type->encodeData(d,intention,datactx,numkeys,cfs,rawkeys,rawvals);
    else
        return 0;
}