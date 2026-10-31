#ifndef __CTRIP_COMPAT_H__
#define __CTRIP_COMPAT_H__

#include "server.h"
#include "ordered_index.h"

/* Type aliases for Redis -> Valkey naming transition */
#define redisObject serverObject

#include "xredis_gtid.h"
#include "module.h"

/* Module events compatibility */
#define REDISMODULE_EVENT_MASTER_LINK_CHANGE VALKEYMODULE_EVENT_PRIMARY_LINK_CHANGE
#define REDISMODULE_SUBEVENT_MASTER_LINK_DOWN VALKEYMODULE_SUBEVENT_PRIMARY_LINK_DOWN
#define REDISMODULE_SUBEVENT_MASTER_LINK_UP VALKEYMODULE_SUBEVENT_PRIMARY_LINK_UP

/* Server / replication terminology compatibility */
#define masterhost primary_host
#define masterport primary_port
#define master primary
#define cached_master cached_primary
#define slaves replicas
#define replicationSetMaster(ip, port) replicationSetPrimary((ip), (port), 0, true)
#define replicationUnsetMaster replicationUnsetPrimary
#define replicationHandleMasterDisconnection() replicationHandlePrimaryDisconnection()
#define iAmMaster() iAmPrimary()
#ifndef LONG_LONG_MIN
#define LONG_LONG_MIN LLONG_MIN
#endif
#ifndef LONG_LONG_MAX
#define LONG_LONG_MAX LLONG_MAX
#endif
#define disconnectSlaves() disconnectReplicas()
#define ctrip_freeReplicationBacklog() freeReplicationBacklog()
#define slowlog commandlog[COMMANDLOG_TYPE_SLOW].entries
#define slave_capa repl_data->replica_capa
#define replicationFeedSlaves(slaves, dbid, argv, argc) replicationFeedReplicas((dbid), (argv), (argc))

/* Tracking heartbeat option flags */
#define CLIENT_TRACKING_HEARTBEAT_SYSTIME (1ULL << 12)
#define CLIENT_TRACKING_HEARTBEAT_MKPS (1ULL << 13)

#define CLIENT_TYPE_MASTER CLIENT_TYPE_PRIMARY

/* Client pause and expire compatibility */
#define checkClientPauseTimeoutAndReturnIfPaused() isPausedActionsWithUpdate(PAUSE_ACTION_EXPIRE)

static inline void deleteExpiredKeyAndPropagate(serverDb *db, robj *keyobj) {
    int dict_index = getKVStoreIndexForKey((sds)objectGetVal(keyobj));
    deleteExpiredKeyAndPropagateWithDictIndex(db, keyobj, dict_index);
}

static inline unsigned long long estimateObjectIdleTime(robj *o) {
    return (unsigned long long)objectGetLRUIdleSecs(o) * 1000;
}

#define emptyDb(dbnum, flags, callback) emptyData(dbnum, flags, callback)

/* getKeysResult init compatibility */
#define GETKEYS_RESULT_INIT         \
    {                               \
        0, MAX_KEYS_BUFFER, NULL, { \
            { 0, 0 }                \
        }                           \
    }

#define OBJ_HASH_KEY OBJ_HASH_FIELD
#define OBJ_ENCODING_HT OBJ_ENCODING_HASHTABLE
#define createZsetZiplistObject() createZsetListpackObject()
#define zset_max_ziplist_entries zset_max_listpack_entries
#define SentReplyOnKeyMiss(c, reply) addReplyOrErrorObject((c), (reply))
#define redis_set_thread_title(name) valkey_set_thread_title(name)
#define LFUDecrAndReturn(x) objectGetLFUFrequency(x)
#define sdsMakeRoomForExact(s, len) sdsMakeRoomForNonGreedy((s), (len))
#ifndef RDB_TYPE_BITMAP
#define RDB_TYPE_BITMAP 24
#endif
#define AOF_READ_DIFF_INTERVAL_BYTES (1024 * 10)
#define aofReadDiffFromParent() ((void)0)

static inline int _ctripRdbSaveBackground2(char *filename, rdbSaveInfo *rsi) {
    return (rdbSaveBackground)(REPLICA_REQ_NONE, filename, rsi, RDBFLAGS_NONE);
}
static inline int _ctripRdbSaveBackground4(int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    return (rdbSaveBackground)(req, filename, rsi, rdbflags);
}
#define _GET_MACRO_RSB(_1, _2, _3, _4, NAME, ...) NAME
#define rdbSaveBackground(...) _GET_MACRO_RSB(__VA_ARGS__, _ctripRdbSaveBackground4, placeholder, _ctripRdbSaveBackground2, 0)(__VA_ARGS__)

static inline robj *_ctripSetTypeCreate1(sds value) {
    return (setTypeCreate)(value, 0);
}
static inline robj *_ctripSetTypeCreate2(sds value, size_t size_hint) {
    return (setTypeCreate)(value, size_hint);
}
#define _GET_MACRO_STC(_1, _2, NAME, ...) NAME
#define setTypeCreate(...) _GET_MACRO_STC(__VA_ARGS__, _ctripSetTypeCreate2, _ctripSetTypeCreate1, 0)(__VA_ARGS__)

static inline size_t _ctripObjectComputeSize2(robj *o, size_t sample_size) {
    return (objectComputeSize)(NULL, o, sample_size, 0);
}
static inline size_t _ctripObjectComputeSize4(robj *key, robj *o, size_t sample_size, int dbid) {
    return (objectComputeSize)(key, o, sample_size, dbid);
}
#define _GET_MACRO_OCS(_1, _2, _3, _4, NAME, ...) NAME
#define objectComputeSize(...) _GET_MACRO_OCS(__VA_ARGS__, _ctripObjectComputeSize4, placeholder, _ctripObjectComputeSize2, 0)(__VA_ARGS__)

static inline int rdbSaveObjectType(rio *rdb, robj *o) {
    int rdbtype = rdbGetObjectType(o, RDB_VERSION);
    if (rdbtype == -1) return -1;
    return rdbSaveType(rdb, rdbtype);
}

static inline ssize_t _ctripRdbSaveObject3(rio *rdb, robj *o, robj *key) {
    int rdbtype = rdbGetObjectType(o, RDB_VERSION);
    return (rdbSaveObject)(rdb, o, key, 0, (unsigned char)rdbtype);
}
static inline ssize_t _ctripRdbSaveObject5(rio *rdb, robj *o, robj *key, int dbid, unsigned char type) {
    return (rdbSaveObject)(rdb, o, key, dbid, type);
}
#define _GET_MACRO_RSO(_1, _2, _3, _4, _5, NAME, ...) NAME
#define rdbSaveObject(...) _GET_MACRO_RSO(__VA_ARGS__, _ctripRdbSaveObject5, placeholder, _ctripRdbSaveObject3, 0)(__VA_ARGS__)

static inline void listTypeConvert(robj *o, int enc) {
    if (enc == OBJ_ENCODING_QUICKLIST) {
        if (objectGetEncoding(o) == OBJ_ENCODING_QUICKLIST) return;
        if (objectGetEncoding(o) == OBJ_ENCODING_LISTPACK) {
            quicklist *ql = quicklistNew(server.list_max_listpack_size, server.list_compress_depth);
            unsigned char *lp = (unsigned char *)objectGetVal(o);
            if (lpLength(lp))
                quicklistAppendListpack(ql, lp);
            else
                lpFree(lp);
            objectSetVal(o, ql);
            objectSetEncoding(o, OBJ_ENCODING_QUICKLIST);
        }
    }
}

static inline robj *_compat_createQuicklistObject(void) {
    return (createQuicklistObject)(server.list_max_listpack_size, server.list_compress_depth);
}

static inline void _ctripHashTypeInitIterator2(robj *subject, hashTypeIterator *hi) {
    (hashTypeInitIterator)(subject, hi);
}
static inline hashTypeIterator *_ctripHashTypeInitIterator1(robj *subject) {
    hashTypeIterator *hi = (hashTypeIterator *)zmalloc(sizeof(*hi));
    (hashTypeInitIterator)(subject, hi);
    return hi;
}
#define _GET_MACRO_HTI(_1, _2, NAME, ...) NAME
#define hashTypeInitIterator(...) _GET_MACRO_HTI(__VA_ARGS__, _ctripHashTypeInitIterator2, _ctripHashTypeInitIterator1, 0)(__VA_ARGS__)
#define hashTypeReleaseIterator(hi) \
    do {                            \
        hashTypeResetIterator(hi);  \
        zfree(hi);                  \
    } while (0)
static inline void hashTypeCurrentObject(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    if (hi->encoding == OBJ_ENCODING_LISTPACK) {
        *vstr = NULL;
        hashTypeCurrentFromListpack(hi, what, vstr, vlen, vll);
    } else if (hi->encoding == OBJ_ENCODING_HASHTABLE) {
        size_t len;
        char *str = hashTypeCurrentFromHashTable(hi, what, &len);
        *vstr = (unsigned char *)str;
        *vlen = (unsigned int)len;
        *vll = 0;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

static inline int _ctripHashTypeSet4(robj *o, sds field, sds value, int flags) {
    return (hashTypeSet)(o, field, value, EXPIRY_NONE, flags, NULL);
}
static inline int _ctripHashTypeSet6(robj *o, sds field, sds value, mstime_t expiry, int flags, bool *expired_overwritten) {
    return (hashTypeSet)(o, field, value, expiry, flags, expired_overwritten);
}
#define _GET_MACRO_HTS(_1, _2, _3, _4, _5, _6, NAME, ...) NAME
#define hashTypeSet(...) _GET_MACRO_HTS(__VA_ARGS__, _ctripHashTypeSet6, placeholder, _ctripHashTypeSet4, 0)(__VA_ARGS__)

#define EVICT_NORMAL 0
#define EVICT_FIFO 1

typedef struct importingEvictKeyInfo {
    sds key;
    int dbid;
} importingEvictKeyInfo;

importingEvictKeyInfo *importingEvictKeyInfoCreate(void);
void importingEvictKeyInfoFree(void *key_info);
int isImportingExpireDisabled(void);
int isImportingFifoEnabled(void);
void importingStart(long long ttl);
void importingEnd(void);
long long importingTtl(void);
void importCommand(client *c);

void ratelimitPauseClient(client *c);
void ratelimitResumeClient(client *c);
#define loadingProgress(size) loadingIncrProgress(size)
#define redis_fsync valkey_fsync
#define slaveKeysWithExpire replicaKeysWithExpire

/* dictCreate compatibility (supports 1 or 2 arguments) */
#undef dictCreate
#define GET_MACRO_DICTCREATE(_1, _2, NAME, ...) NAME
#define dictCreate1(type) hashtableCreate(type)
#define dictCreate2(type, privdata) hashtableCreate(type)
#define dictCreate(...) GET_MACRO_DICTCREATE(__VA_ARGS__, dictCreate2, dictCreate1, 0)(__VA_ARGS__)

/* Shared internal declarations for Swap engine */
extern dictType objectMetaDictType;

ssize_t rdbSaveAuxFieldStrStr(rio *rdb, char *key, char *val);
int rmdirRecursive(const char *path);
void handleBioThreadFinishedRDBDownload(void);
void shiftReplicationId(void);
void rejectCommand(client *c, robj *reply, int notify_modules);
int activeExpireCycleTryExpire(serverDb *db, robj *val, mstime_t now, int didx);
int zslParseRange(robj *min, robj *max, zrangespec *spec);
unsigned long getClusterConnectionsCount(void);

#endif /* __CTRIP_COMPAT_H__ */
