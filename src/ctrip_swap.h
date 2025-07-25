#ifndef  __CTRIP_SWAP_H__
#define  __CTRIP_SWAP_H__

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

#include "server.h"
#include <rocksdb/c.h>
#include "atomicvar.h"
#include "ctrip_lru_cache.h"
#include "ctrip_cuckoo_filter.h"
#include "ctrip_swap_adlist.h"
#include "ctrip_wtdigest.h"

#define IN        /* Input parameter */
#define OUT       /* Output parameter */
#define INOUT     /* Input/Output parameter */
#define MOVE      /* Moved ownership */

#define DATA_CF 0
#define META_CF 1
#define SCORE_CF 2
#define CF_COUNT 3

#define data_cf_name "default"
#define meta_cf_name "meta"
#define score_cf_name "score"
extern const char *swap_cf_names[CF_COUNT];
#define rocksdb_stats_section "rocksdb.stats"
#define rocksdb_stats_section_len 13

/* --- cmd intention flags --- */
/* Delete key in rocksdb when swap in. */
#define SWAP_IN_DEL (1U<<0)
/* Only need to swap meta for hash/set/zset/list/bitmap  */
#define SWAP_IN_META (1U<<1)
/* Delete key in rocksdb and mock value needed to be swapped in. */
#define SWAP_IN_DEL_MOCK_VALUE (1U<<2)
/* Data swap in will be overwritten by fun dbOverwrite
 * same as SWAP_IN_DEL for collection type(SET, ZSET, LISH, HASH...), same as SWAP_IN for STRING */
#define SWAP_IN_OVERWRITE (1U<<3)
/* When swap finished, meta will be deleted(so that key will turn pure hot).*/
#define SWAP_IN_FORCE_HOT (1U<<4)
/* whether to expire Keys with generated in writtable slave is decided
 * before submitExpireClientRequest and should not skip expire even
 * if current role is slave. */
#define SWAP_EXPIRE_FORCE (1U<<5)
/* If oom would happen during RIO, swap will abort. */
#define SWAP_OOM_CHECK (1U<<6)
/* This is a metascan request for scan command. */
#define SWAP_METASCAN_SCAN (1U<<7)
/* This is a metascan request for randomkey command. */
#define SWAP_METASCAN_RANDOMKEY (1U<<8)
/* This is a metascan request for active-expire. */
#define SWAP_METASCAN_EXPIRE (1U<<9)
/* This is a persist requset. */
#define SWAP_OUT_PERSIST (1U<<10)
/* Keep data in memory because memory is sufficient. */
#define SWAP_OUT_KEEP_DATA (1U<<11)

/* --- swap intention flags --- */
/* Delete rocksdb data key when swap in */
#define SWAP_EXEC_IN_DEL (1U<<0)
/* object meta will be deleted from db.meta */
#define SWAP_EXEC_FORCE_HOT (1U<<1)
/* check whether oom would happend during RIO */
#define SWAP_EXEC_OOM_CHECK (1U<<2)
/* Don't delete key in keyspace when swap (Delete key in rocksdb) finish. */
#define SWAP_FIN_DEL_SKIP (1U<<3)
/* Reserve data when swap out. */
#define SWAP_EXEC_OUT_KEEP_DATA (1U<<4)


#define SWAP_UNSET -1
#define SWAP_NOP    0
#define SWAP_IN     1
#define SWAP_OUT    2
#define SWAP_DEL    3
#define SWAP_UTILS  4
#define SWAP_TYPES  5

/* next of OBJ_STREAM */
#define OBJ_BITMAP  7

#define SWAP_TYPE_STRING    OBJ_STRING
#define SWAP_TYPE_LIST      OBJ_LIST
#define SWAP_TYPE_SET       OBJ_SET
#define SWAP_TYPE_ZSET      OBJ_ZSET
#define SWAP_TYPE_HASH      OBJ_HASH
#define SWAP_TYPE_STREAM    OBJ_STREAM
#define SWAP_TYPE_BITMAP    OBJ_BITMAP

static inline const char *swapIntentionName(int intention) {
  const char *name = "?";
  const char *intentions[] = {"NOP", "IN", "OUT", "DEL", "UTILS"};
  if (intention >= 0 && intention < SWAP_TYPES)
    name = intentions[intention];
  return name;
}
static inline int getSwapIntentionByName(char *name) {
  const char *intentions[] = {"NOP", "IN", "OUT", "DEL", "UTILS"};
  for (int intention = 0; intention < SWAP_TYPES; intention++) {
    if (!strcasecmp(intentions[intention], name)) {
      return intention;
    }
  }
  return SWAP_UNSET;
}

static inline int isMetaScanRequest(uint32_t intention_flag) {
    return (intention_flag & SWAP_METASCAN_SCAN) ||
           (intention_flag & SWAP_METASCAN_RANDOMKEY) ||
           (intention_flag & SWAP_METASCAN_EXPIRE);
}

#define MIN(a,b) ((a) < (b) ? (a) : (b))
#define MAX(a,b) ((a) < (b) ? (b) : (a))

/* Cmd */
#define REQUEST_LEVEL_SVR  0
#define REQUEST_LEVEL_DB   1
#define REQUEST_LEVEL_KEY  2
#define REQUEST_LEVEL_TYPES  3

#define MAX_KEYREQUESTS_BUFFER 8

#define SWAP_CMD_COUNT 238
extern struct redisCommand redisCommandTable[SWAP_CMD_COUNT];

typedef struct swapCmdTrace swapCmdTrace;
typedef struct swapTrace swapTrace;

typedef void (*freefunc)(void *);

static inline const char *requestLevelName(int level) {
  const char *name = "?";
  const char *levels[] = {"SVR","DB","KEY"};
  if (level >= 0 && level < REQUEST_LEVEL_TYPES)
    name = levels[level];
  return name;
}

#define SEGMENT_TYPE_HOT 0
#define SEGMENT_TYPE_COLD 1
#define SEGMENT_TYPE_BOTH 2

/* Both start and end are inclusive, see addListRangeReply for details. */
typedef struct range {
  long long start;
  long long end;
  int reverse; /* LTRIM command specifies range to keep, so swap in the reverse range */
} range;

#define KEYREQUEST_TYPE_KEY    0
#define KEYREQUEST_TYPE_SUBKEY 1
#define KEYREQUEST_TYPE_RANGE  2
#define KEYREQUEST_TYPE_SCORE  3
#define KEYREQUEST_TYPE_SAMPLE 4
#define KEYREQUEST_TYPE_BTIMAP_OFFSET  5
#define KEYREQUEST_TYPE_BTIMAP_RANGE  6

typedef struct argRewriteRequest {
  int mstate_idx; /* >=0 if current command is a exec, means index in mstate; -1 means req not in multi/exec */
  int arg_idx; /* index of argument to use for rewrite func */
} argRewriteRequest;

static inline void argRewriteRequestInit(argRewriteRequest *arg_req) {
  arg_req->mstate_idx = -1;
  arg_req->arg_idx = -1;
}

typedef struct keyRequest{
  int dbid;
  int level;
  int cmd_intention;
  int cmd_intention_flags;
  uint64_t cmd_flags;
  int type; /* request type */
  int deferred;
  robj *key;
  union {
    struct {
      int num_subkeys;
      robj **subkeys;
    } b; /* subkey: hash, set */
    struct {
      int num_ranges;
      range *ranges;
    } l; /* range: list */
    struct {
      zrangespec* rangespec;
      int reverse;
      int limit;
    } zs; /* zset score*/
    struct {
      int count;
    } sp; /* sample */
    struct {
      long long offset;
    } bo; /* bitmap offset*/
    struct {
      long long start;
      long long end;
    } br; /* bitmap range*/
  };
  argRewriteRequest arg_rewrite[2];
  swapCmdTrace *swap_cmd;
  swapTrace *trace;
} keyRequest;

void copyKeyRequest(keyRequest *dst, keyRequest *src);
void moveKeyRequest(keyRequest *dst, keyRequest *src);
void keyRequestDeinit(keyRequest *key_request);
void getKeyRequests(client *c, struct getKeyRequestsResult *result);
void releaseKeyRequests(struct getKeyRequestsResult *result);
int getKeyRequestsNone(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsGlobal(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsMetaScan(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

int getKeyRequestsSort(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

#define getKeyRequestsHsetnx getKeyRequestsHset
#define getKeyRequestsHget getKeyRequestsHmget
#define getKeyRequestsHdel getKeyRequestsHmget
#define getKeyRequestsHstrlen getKeyRequestsHmget
#define getKeyRequestsHincrby getKeyRequestsHget
#define getKeyRequestsHincrbyfloat getKeyRequestsHmget
#define getKeyRequestsHexists getKeyRequestsHmget
int getKeyRequestsHset(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsHmget(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsHlen(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

#define getKeyRequestsSadd getKeyRequestSmembers
#define getKeyRequestsSrem getKeyRequestSmembers
#define getKeyRequestsSdiffstore getKeyRequestsSinterstore
#define getKeyRequestsSunionstore getKeyRequestsSinterstore
int getKeyRequestSmembers(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestSmove(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsSinterstore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

int getKeyRequestsZunionstore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZinterstore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZdiffstore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

int getKeyRequestsRpop(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsBrpop(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsLpop(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsBlpop(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsRpoplpush(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsLmove(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsLindex(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
#define getKeyRequestsLset getKeyRequestsLindex
int getKeyRequestsLrange(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsLtrim(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

int getKeyRequestsZAdd(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZScore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZMScore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZincrby(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZrange(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZrangestore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsSinterstore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZpopMin(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZpopMax(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZrangeByScore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZrevrangeByScore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZremRangeByScore1(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
#define getKeyRequestsZremRangeByScore getKeyRequestsZrangeByScore
int getKeyRequestsZrevrangeByLex(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZrangeByLex(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZremRangeByLex(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsZlexCount(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

#define getKeyRequestsSdiffstore getKeyRequestsSinterstore
#define getKeyRequestsSunionstore getKeyRequestsSinterstore
#define getKeyRequestsZrem getKeyRequestsZScore

int getKeyRequestsGeoAdd(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsGeoRadius(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsGeoHash(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsGeoDist(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsGeoSearch(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsGeoSearchStore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
#define getKeyRequestsGeoRadiusByMember getKeyRequestsGeoRadius
#define getKeyRequestsGeoPos getKeyRequestsGeoHash

int getKeyRequestsGtid(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

int getKeyRequestsDebug(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsSetbit(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsGetbit(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsBitcount(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsBitpos(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsBitop(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);
int getKeyRequestsBitField(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

int getKeyRequestsMemory(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

int getKeyRequestsMemory(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result);

#define GET_KEYREQUESTS_RESULT_INIT { {{0}}, NULL, NULL, 0, MAX_KEYREQUESTS_BUFFER}

typedef struct getKeyRequestsResult {
	keyRequest buffer[MAX_KEYREQUESTS_BUFFER];
	keyRequest *key_requests;
	swapCmdTrace *swap_cmd;
	int num;
	int size;
} getKeyRequestsResult;

void getKeyRequestsPrepareResult(getKeyRequestsResult *result, int numswaps);
void getKeyRequestsAppendSubkeyResult(getKeyRequestsResult *result, int level, MOVE robj *key, int num_subkeys, MOVE robj **subkeys, int cmd_intention, int cmd_intention_flags, uint64_t cmd_flags, int dbid);
void getKeyRequestsFreeResult(getKeyRequestsResult *result);
void getKeyRequestsAttachSwapTrace(getKeyRequestsResult * result, swapCmdTrace *swap_cmd, int from_include, int to_exclude);

void getKeyRequestsAppendRangeResult(getKeyRequestsResult *result, int level, MOVE robj *key, int arg_rewrite0, int arg_rewrite1, int num_ranges, MOVE range *ranges, int cmd_intention, int cmd_intention_flags, uint64_t cmd_flags, int dbid);


#define SWAP_PERSIST_VERSION_NO      0
#define SWAP_PERSIST_VERSION_INITIAL 1

typedef struct persistingKeyEntry {
    listNode *ln;
    uint64_t version;
    mstime_t mstime;
    int state;
} persistingKeyEntry;

persistingKeyEntry *persistingKeyEntryNew(listNode *ln, uint64_t version, mstime_t mstime);
void persistingKeyEntryFree(void *privdata, void *val);

typedef struct persistingKeys {
    list *todo;
    list *doing;
    dict *map;
} persistingKeys;

typedef struct persistingKeysTodoIter {
  persistingKeys *keys;
  listIter li;
} persistingKeysTodoIter;

persistingKeys *persistingKeysNew(void);
void persistingKeysFree(persistingKeys *keys);
int persistingKeysPut(persistingKeys *keys, sds key, uint64_t version, mstime_t time);
persistingKeyEntry *persistingKeysLookup(persistingKeys *keys, sds key);
int persistingKeysDelete(persistingKeys *keys, sds key);
size_t persistingKeysCount(persistingKeys *keys);
size_t persistingKeysUsedMemory(persistingKeys *keys);

void persistingKeysInitTodoIterator(persistingKeysTodoIter *iter, persistingKeys *keys);
void persistingKeysDeinitTodoIterator(persistingKeysTodoIter *iter);
sds persistingKeysTodoIterNext(persistingKeysTodoIter *iter, persistingKeyEntry **entry);

#define SWAP_PERSIST_MAX_KEYS_PER_LOOP 1024

typedef struct swapPersistStat {
  long long add_succ;
  long long add_ignored;
  long long started;
  long long rewind_dirty;
  long long rewind_newer;
  long long ended;
  long long keep_data;
  long long dont_keep;
} swapPersistStat;

typedef struct swapPersistCtx {
  int keep;
  uint64_t version;
  persistingKeys **keys; /* one for each db */
  long long inprogress_count; /* current inprogrss persist count */
  long long inprogress_limit; /* current inprogress limit */
  swapPersistStat stat;
} swapPersistCtx;

swapPersistCtx *swapPersistCtxNew(void);
void swapPersistCtxFree(swapPersistCtx *ctx);
size_t swapPersistCtxKeysCount(swapPersistCtx *ctx);
size_t swapPersistCtxUsedMemory(swapPersistCtx *ctx);
mstime_t swapPersistCtxLag(swapPersistCtx *ctx);
void swapPersistCtxAddKey(swapPersistCtx *ctx, redisDb *db, robj *key);
void swapPersistCtxPersistKeys(swapPersistCtx *ctx);
sds genSwapPersistInfoString(sds info);
void swapPersistKeyRequestFinished(swapPersistCtx *ctx, int dbid, robj *key, uint64_t persist_version);
void loadDataFromDisk(void);
void swap_loadDataFromDisk(void);
int submitEvictClientRequest(client *c, robj *key, int persist_keep, uint64_t persist_version);

#define setObjectPersistKeep(o) do { \
    if (o) o->persist_keep = 1; \
} while(0)

#define clearObjectPersistKeep(o) do { \
    if (o) o->persist_keep = 0; \
} while(0)

#define overwriteObjectPersistKeep(o,pk) do { \
    if (o) o->persist_keep = pk; \
} while(0)


#define getObjectPersistKeep(o) ((o) ? o->persist_keep : 0)

#define setObjectPersistent(o) do { \
    if (o) o->persistent = 1; \
} while(0)

#define clearObjectPersistent(o) do { \
    if (o) o->persistent = 0; \
} while(0)

#define overwriteObjectPersistent(o,pk) do { \
    if (o) o->persistent = pk; \
} while(0)

#define getObjectPersistent(o) ((o) ? o->persistent : 0)

#define setObjectMetaDirty(o) do { \
    if (o) o->dirty_meta = 1; \
} while(0)

#define setObjectDataDirty(o) do { \
    if (o) o->dirty_data = 1; \
} while(0)

#define setObjectDirty(o) do { \
  setObjectMetaDirty(o); \
  setObjectDataDirty(o); \
} while(0)

#define clearObjectMetaDirty(o) do { \
    if (o) o->dirty_meta = 0; \
} while(0)

#define clearObjectDataDirty(o) do { \
    if (o) o->dirty_data = 0; \
} while(0)

#define clearObjectDirty(o) do { \
  clearObjectMetaDirty(o); \
  clearObjectDataDirty(o); \
} while(0)

#define schedulePersistIfNeeded(dbid,key) do { \
  if (server.swap_persist_enabled) swapPersistCtxAddKey(server.swap_persist_ctx,server.db+dbid,key); \
} while (0)

#define setObjectMetaDirtyPersist(dbid,key,o) do { \
  setObjectMetaDirty(o); \
  schedulePersistIfNeeded(dbid,key); \
} while (0)

#define setObjectDataDirtyPersist(dbid,key,o) do { \
  setObjectDataDirty(o); \
  schedulePersistIfNeeded(dbid,key); \
} while (0)

#define setObjectDirtyPersist(dbid,key,o) do { \
  setObjectDirty(o); \
  schedulePersistIfNeeded(dbid,key); \
} while (0)

#define objectIsMetaDirty(o) ((o)->dirty_meta)
#define objectIsDataDirty(o) ((o)->dirty_data)

#define objectIsDirty(o) (objectIsMetaDirty(o) || objectIsDataDirty(o))

static inline void dbSetDirty(redisDb *db, robj *key) {
    robj *o = lookupKey(db,key,LOOKUP_NOTOUCH);
    if (o) setObjectDirtyPersist(db->id,key,o);
}

static inline void dbSetMetaDirty(redisDb *db, robj *key) {
    robj *o = lookupKey(db,key,LOOKUP_NOTOUCH);
    if (o) setObjectMetaDirty(o);
}


/* Object meta */
#define SWAP_VERSION_ZERO 0
#define SWAP_VERSION_MAX  UINT64_MAX

typedef enum {
    NORMAL_MODE = 0,
    RORDB_MODE,
} meta_encode_mode;

extern dictType objectMetaDictType;

struct objectMeta;
typedef struct objectMetaType {
  sds (*encodeObjectMeta) (struct objectMeta *object_meta, void *aux, int meta_enc_mode);
  int (*decodeObjectMeta) (struct objectMeta *object_meta, const char* extend, size_t extlen);
  int (*objectIsHot)(struct objectMeta *object_meta, robj *value);
  void (*free)(struct objectMeta *object_meta);
  void (*duplicate)(struct objectMeta *dup_meta, struct objectMeta *object_meta);
  int (*equal)(struct objectMeta *oma, struct objectMeta *omb);
  int (*rebuildFeed)(struct objectMeta *rebuild_meta, uint64_t version, const char *subkey, size_t sublen, robj *subval);
} objectMetaType;

typedef struct objectMeta {
  uint64_t version;
  unsigned swap_type:4;
  union {
    long long len:60;
    unsigned long long ptr:60;
  };
} objectMeta;

extern objectMetaType lenObjectMetaType;
extern objectMetaType listObjectMetaType;
extern objectMetaType bitmapObjectMetaType;

static inline void swapInitVersion(void) { server.swap_key_version = 1; }
static inline void swapSetVersion(uint64_t version) { server.swap_key_version = version; }
static inline uint64_t swapGetAndIncrVersion(void) { return server.swap_key_version++; }

int buildObjectMeta(int swap_type, uint64_t version, const char *extend, size_t extlen, OUT objectMeta **pobject_meta);
objectMeta *dupObjectMeta(objectMeta *object_meta);
void freeObjectMeta(objectMeta *object_meta);
sds objectMetaEncode(struct objectMeta *object_meta, int meta_enc_mode);
int objectMetaDecode(struct objectMeta *object_meta, const char *extend, size_t extlen);
int keyIsHot(objectMeta *object_meta, robj *value);
int keyIsPureHot(objectMeta *object_meta, robj *value);
sds dumpObjectMeta(objectMeta *object_meta);
int objectMetaEqual(struct objectMeta *oma, struct objectMeta *omb);
int objectMetaRebuildFeed(struct objectMeta *rebuild_meta, uint64_t version, const char *subkey, size_t sublen, robj *subval);

static inline void *objectMetaGetPtr(objectMeta *object_meta) {
  return (void*)(unsigned long long)object_meta->ptr;
}
static inline void objectMetaSetPtr(objectMeta *object_meta, void *ptr) {
  object_meta->ptr = (unsigned long long)ptr;
}

objectMeta *createObjectMeta(int swap_type, uint64_t version);

objectMeta *createLenObjectMeta(int swap_type, uint64_t version, size_t len);
sds encodeLenObjectMeta(struct objectMeta *object_meta, void *aux, int meta_enc_mode);
int decodeLenObjectMeta(struct objectMeta *object_meta, const char *extend, size_t extlen);
int lenObjectMetaIsHot(struct objectMeta *object_meta, robj *value);

objectMeta *lookupMeta(redisDb *db, robj *key);
void dbAddMeta(redisDb *db, robj *key, objectMeta *m);
int dbDeleteMeta(redisDb *db, robj *key);

typedef struct swapObjectMeta {
  objectMetaType *omtype;
  objectMeta *object_meta;
  objectMeta *cold_meta;
  robj *value;
} swapObjectMeta;

#define initStaticSwapObjectMeta(_som,_omtype,_object_meta,_value) do { \
    _som.omtype = _omtype; \
    _som.object_meta = _object_meta; \
    _som.value = _value; \
} while(0)

static inline int swapObjectMetaIsHot(swapObjectMeta *som) {
    if (som->value == NULL) return 0;
    serverAssert((som->object_meta->swap_type == SWAP_TYPE_BITMAP && som->value->type == OBJ_STRING) || som->object_meta->swap_type == som->value->type);
    if (som->omtype->objectIsHot) {
      return som->omtype->objectIsHot(som->object_meta,som->value);
    } else {
      return 0;
    }
}

/* bitmap marker */
objectMeta *createBitmapObjectMarker();
int bitmapObjectMetaIsMarker(objectMeta *object_meta);
int bitmapSetObjectMarkerIfNotExist(redisDb *db, robj *key);
int bitmapClearObjectMarkerIfExist(redisDb *db, robj *key);
void bitmapMetaTransToMarkerIfNeeded(objectMeta *object_meta);
void bitmapMarkerTransToMetaIfNeeded(objectMeta *object_meta, robj *value);

/* Data */
#define SWAP_DATA_ABSENT_SUBKEYS_INIT 4
#define SWAP_DATA_ABSENT_SUBKEYS_LINEAR 1024

typedef struct swapDataAbsentSubkey {
  size_t count;
  size_t capacity;
  sds *subkeys;
} swapDataAbsentSubkey;

#define SWAP_ANA_THD_MAIN 0
#define SWAP_ANA_THD_SWAP 1

static inline int swapDataAnaSwapType(robj *value, objectMeta *object_meta) {
    if (object_meta) return object_meta->swap_type;
    if (value) return value->type;
    return -1;
}

/* SwapData represents key state when swap start. It is stable during
 * key swapping, misc dynamic data are save in dataCtx. */
typedef struct swapData {
  struct swapDataType *type;
  struct objectMetaType *omtype;
  redisDb *db;
  robj *key; /*own*/
  robj *value; /*own*/
  long long expire;
  objectMeta *object_meta; /* ref */
  objectMeta *cold_meta; /* own, moved from exec */
  objectMeta *new_meta; /* own */
  int swap_type;
  unsigned propagate_expire:1;
  unsigned set_dirty:1;
  unsigned set_dirty_meta:1;
  unsigned persistence_deleted:1;
  unsigned set_persist_keep:1;
  unsigned reserved:27;
  sds nextseek; /* own, moved from exec */
  swapDataAbsentSubkey *absent;
  robj *dirty_subkeys;
  void *extends[2];
} swapData;

/* keyRequest: client request parse from command.
 * swapData: key state when swap start.
 * dataCtx: dynamic data when swapping.  */
typedef struct swapDataType {
  char* name;
  uint64_t cmd_swap_flags;
  int (*swapAna)(struct swapData *data, int thd, struct keyRequest *key_request, OUT int *intention, OUT uint32_t *intention_flags, void *datactx);
  int (*swapAnaAction)(struct swapData *data, int intention, void *datactx, OUT int *action);
  int (*encodeKeys)(struct swapData *data, int intention, void *datactx, OUT int *num, OUT int **cfs, OUT sds **rawkeys);
  int (*encodeRange)(struct swapData *data, int intention, void *datactx, OUT int *limit, OUT uint32_t *flags, OUT int *cf, OUT sds *start, OUT sds *end);
  int (*encodeData)(struct swapData *data, int intention, void *datactx, OUT int *num, OUT int **cfs, OUT sds **rawkeys, OUT sds **rawvals);
  int (*decodeData)(struct swapData *data, int num, int *cfs, sds *rawkeys, sds *rawvals, OUT void **decoded);
  int (*swapIn)(struct swapData *data, MOVE void *result, void *datactx);
  int (*swapOut)(struct swapData *data, void *datactx, int keep_data, OUT int *totally_out);
  int (*swapDel)(struct swapData *data, void *datactx, int async);
  void *(*createOrMergeObject)(struct swapData *data, MOVE void *decoded, void *datactx);
  int (*cleanObject)(struct swapData *data, void *datactx, int keep_data);
  int (*beforeCall)(struct swapData *data, keyRequest *key_request, client *c, void *datactx);
  void (*free)(struct swapData *data, void *datactx);
  int (*rocksDel)(struct swapData *data_,  void *datactx_, int inaction, int num, int* cfs, sds *rawkeys, sds *rawvals, OUT int *outaction, OUT int *outnum, OUT int** outcfs,OUT sds **outrawkeys);
  int (*mergedIsHot)(struct swapData *data, MOVE void *result, void *datactx);
  void* (*getObjectMetaAux)(struct swapData *data, void *datactx);
} swapDataType;

swapData *createSwapData(redisDb *db, robj *key, robj *value, robj *dirty_subkeys);
int swapDataSetupMeta(swapData *d, int swap_type, long long expire, OUT void **datactx);
int swapDataAlreadySetup(swapData *d);
void swapDataMarkPropagateExpire(swapData *data);
int swapDataAna(swapData *d, int thd, struct keyRequest *key_request, int *intention, uint32_t *intention_flag, void *datactx);
int swapDataSwapAnaAction(swapData *data, int intention, void *datactx_, int *action);
sds swapDataEncodeMetaKey(swapData *d);
sds swapDataEncodeMetaVal(swapData *d, void *datactx);
int swapDataEncodeKeys(swapData *d, int intention, void *datactx, int *num, int **cfs, sds **rawkeys);
int swapDataEncodeData(swapData *d, int intention, void *datactx, int *num, int **cfs, sds **rawkeys, sds **rawvals);
int swapDataEncodeRange(struct swapData *data, int intention, void *datactx_, int *limit, uint32_t *flags, int *pcf, sds *start, sds *end);
int swapDataDecodeAndSetupMeta(swapData *d, sds rawval, OUT void **datactx);
int swapDataDecodeData(swapData *d, int num, int *cfs, sds *rawkeys, sds *rawvals, void **decoded);
int swapDataSwapIn(swapData *d, void *result, void *datactx);
int swapDataSwapOut(swapData *d, void *datactx, int keep_data, OUT int *totally_out);
int swapDataSwapDel(swapData *d, void *datactx, int async);
void *swapDataCreateOrMergeObject(swapData *d, MOVE void *decoded, void *datactx);
int swapDataCleanObject(swapData *d, void *datactx, int keep_data);
int swapDataBeforeCall(swapData *d, keyRequest *key_request, client *c, void *datactx);
int swapDataKeyRequestFinished(swapData *data);
char swapDataGetObjectAbbrev(robj *value);
void swapDataFree(swapData *data, void *datactx);
int swapDataMergedIsHot(swapData *d, void *result, void *datactx);
void swapDataRetainAbsentSubkeys(swapData *data, int num, int *cfs, sds *rawkeys, sds *rawvals);
void swapDataMergeAbsentSubkey(swapData *data);
int swapDataMayContainSubkey(swapData *data, int thd, robj *subkey);
void *swapDataGetObjectMetaAux(swapData *data, void *datactx);

static inline void swapDataSetObjectMeta(swapData *d, objectMeta *object_meta) {
    d->object_meta = object_meta;
}
static inline void swapDataSetColdObjectMeta(swapData *d, MOVE objectMeta *cold_meta) {
    d->cold_meta = cold_meta;
}
static inline void swapDataSetNewObjectMeta(swapData *d, MOVE objectMeta *new_meta) {
    d->new_meta = new_meta;
}
static inline int swapDataIsCold(swapData *data) {
  return data->value == NULL;
}
static inline int swapDataIsHot(swapData *data) {
  swapObjectMeta som;
  initStaticSwapObjectMeta(som,data->omtype,data->object_meta,data->value);
  return swapObjectMetaIsHot(&som);
}
static inline objectMeta *swapDataObjectMeta(swapData *d) {
    serverAssert(
        !(d->object_meta && d->new_meta) ||
        !(d->object_meta && d->cold_meta) ||
        !(d->new_meta && d->cold_meta));

    if (d->object_meta) return d->object_meta;
    if (d->cold_meta) return d->cold_meta;
    return d->new_meta;
}

static inline uint64_t swapDataObjectVersion(swapData *d) {
    objectMeta *object_meta = swapDataObjectMeta(d);
    return object_meta ? object_meta->version : 0;
}

static inline int swapDataPersisted(swapData *d) {
    if (d->swap_type == SWAP_TYPE_BITMAP && d->object_meta) {
        return !bitmapObjectMetaIsMarker(d->object_meta);
    }
    return d->object_meta || d->cold_meta;
}
static inline void swapDataObjectMetaModifyLen(swapData *d, int delta) {
    objectMeta *object_meta = swapDataObjectMeta(d);
    object_meta->len += delta;
    serverAssert(object_meta->len >= 0);
}
static inline void swapDataObjectMetaSetPtr(swapData *d, void *ptr) {
    objectMeta *object_meta = swapDataObjectMeta(d);
    object_meta->ptr = (unsigned long long)(long)ptr;
}
/* persist request keeps value in memory when maxmemory not reached or
 * data originally not cold (no need to swap in). */
static inline int swapDataPersistKeepData(swapData *d, uint32_t cmd_intention_flags, int may_keep_data) {
  int keep_data = (cmd_intention_flags & SWAP_OUT_PERSIST) &&
         (getObjectPersistKeep(d->value) || cmd_intention_flags&SWAP_OUT_KEEP_DATA) &&
         may_keep_data;
  if (server.swap_persist_enabled && server.swap_persist_ctx) {
    swapPersistStat *stat = &server.swap_persist_ctx->stat;
    if (keep_data)
      stat->keep_data++;
    else
      stat->dont_keep++;
  }
  return keep_data;
}
void swapDataTurnWarmOrHot(swapData *data);
void swapDataTurnCold(swapData *data);
void swapDataTurnDeleted(swapData *data,int del_skip);

int swapDataObjectMergedIsHot(swapData *data, void *result, void *datactx);
#define setMergedIsHot swapDataObjectMergedIsHot
#define hashMergedIsHot swapDataObjectMergedIsHot
#define zsetMergedIsHot swapDataObjectMergedIsHot
#define wholeKeyMergedIsHot swapDataObjectMergedIsHot

/* Debug msgs */
#ifdef SWAP_DEBUG
#define MAX_MSG    64
#define MAX_STEPS  16

typedef struct swapDebugMsgs {
  char identity[MAX_MSG];
  struct swapCtxStep {
    char name[MAX_MSG];
    char info[MAX_MSG];
  } steps[MAX_STEPS];
  int index;
} swapDebugMsgs;

void swapDebugMsgsInit(swapDebugMsgs *msgs, char *identity);
#ifdef __GNUC__
void swapDebugMsgsAppend(swapDebugMsgs *msgs, char *step, char *fmt, ...)
  __attribute__((format(printf, 3, 4)));
void swapDebugBatchMsgsAppend(swapExecBatch *batch, char *step, char *fmt, ...)
  __attribute__((format(printf, 3, 4)));
#else
void swapDebugMsgsAppend(swapDebugMsgs *msgs, char *step, char *fmt, ...);
void swapDebugBatchMsgsAppend(swapExecBatch *batch, char *step, char *fmt, ...);
#endif
void swapDebugMsgsDump(swapDebugMsgs *msgs);

#define DEBUG_MSGS_INIT(msgs, identity) do { if (msgs) swapDebugMsgsInit(msgs, identity); } while (0)
#define DEBUG_MSGS_APPEND(msgs, step, ...) do { if (msgs) swapDebugMsgsAppend(msgs, step, __VA_ARGS__); } while (0)
#define DEBUG_BATCH_MSGS_APPEND(batch, step, ...) do { if (batch) swapDebugBatchMsgsAppend(batch, step, __VA_ARGS__); } while (0)
#else
#define DEBUG_MSGS_INIT(msgs, identity)
#define DEBUG_MSGS_APPEND(msgs, step, ...)
#define DEBUG_BATCH_MSGS_APPEND(batch, step, ...)
#endif

/* Swap */
#define SWAP_ERR_SETUP_FAIL -100
#define SWAP_ERR_SETUP_UNEXPECTED_SWAP_TYPE -101
#define SWAP_ERR_SETUP_UNSUPPORTED -102
#define SWAP_ERR_DATA_FAIL -200
#define SWAP_ERR_DATA_ANA_FAIL -201
#define SWAP_ERR_DATA_DECODE_FAIL -202
#define SWAP_ERR_DATA_FIN_FAIL -203
#define SWAP_ERR_DATA_UNEXPECTED_INTENTION -204
#define SWAP_ERR_DATA_DECODE_META_FAILED -205
#define SWAP_ERR_DATA_WRONG_TYPE_ERROR -206
#define SWAP_ERR_EXEC_FAIL -300
#define SWAP_ERR_EXEC_UNEXPECTED_ACTION -302
#define SWAP_ERR_EXEC_ROCKSDB_FLUSH_FAIL -303
#define SWAP_ERR_EXEC_UNEXPECTED_UTIL -304
#define SWAP_ERR_METASCAN_FAIL -400
#define SWAP_ERR_METASCAN_UNSUPPORTED_IN_MULTI -401
#define SWAP_ERR_METASCAN_SESSION_UNASSIGNED -402
#define SWAP_ERR_METASCAN_SESSION_INPROGRESS -403
#define SWAP_ERR_METASCAN_SESSION_SEQUNMATCH -404
#define SWAP_ERR_RIO_FAIL -500
#define SWAP_ERR_RIO_GET_FAIL -501
#define SWAP_ERR_RIO_PUT_FAIL -502
#define SWAP_ERR_RIO_DEL_FAIL -503
#define SWAP_ERR_RIO_ITER_FAIL -504
#define SWAP_ERR_RIO_OOM      -505

struct swapCtx;

typedef void (*clientKeyRequestFinished)(client *c, struct swapCtx *ctx);

typedef struct swapCtx {
  client *c;
  keyRequest key_request[1];
  swapData *data;
  void *datactx;
  clientKeyRequestFinished finished;
  int errcode;
  void *swap_lock;
#ifdef SWAP_DEBUG
  swapDebugMsgs msgs;
#endif
  void *pd;
} swapCtx;

swapCtx *swapCtxCreate(client *c, keyRequest *key_request, clientKeyRequestFinished finished, void* pd);
void swapCtxSetSwapData(swapCtx *ctx, MOVE swapData *data, MOVE void *datactx);
void swapCtxFree(swapCtx *ctx);

typedef enum {
    SWAP_REWIND_OFF = 0, /* rewind off */
    SWAP_REWIND_WRITE,   /* rewind client with write commands */
    SWAP_REWIND_ALL,     /* rewind client with any commands */
} swap_rewind_type;

void startSwapRewind(swap_rewind_type rewind_type);
void endSwapRewind(void);
void freeClientSwapCmdTrace(client *c);

/* see server.swap_req_submitted */
#define REQ_SUBMITTED_NONE 0
#define REQ_SUBMITTED_BGSAVE (1ULL<<0)
#define REQ_SUBMITTED_REPL_START (1ULL<<1)

/* Hash */
typedef struct hashSwapData {
  swapData d;
} hashSwapData;

#define BIG_DATA_CTX_FLAG_NONE 0
#define BIG_DATA_CTX_FLAG_MOCK_VALUE (1U<<0)

#define BASE_SWAP_CTX_TYPE_SUBKEY 0
#define BASE_SWAP_CTX_TYPE_SAMPLE 1

typedef struct baseBigDataCtx {
    int type;
    union {
      struct {
        int num;
        robj **subkeys;
      } sub;
      struct {
        int count;
      } spl;
    };
    int ctx_flag;
} baseBigDataCtx;

typedef struct hashDataCtx {
    baseBigDataCtx ctx;
} hashDataCtx;

int swapDataSetupHash(swapData *d, OUT void **datactx);

#define hashObjectMetaType lenObjectMetaType
#define createHashObjectMeta(version, len) createLenObjectMeta(OBJ_HASH, version, len)
size_t hashMetaLength(redisDb *db, robj *key);

/* String */
typedef struct wholeKeySwapData {
  swapData d;
} wholeKeySwapData;

int swapDataSetupWholeKey(swapData *d, OUT void **datactx);

/* Set */
typedef struct setSwapData {
    swapData d;
} setSwapData;

typedef struct setDataCtx {
    baseBigDataCtx ctx;
} setDataCtx;

int swapDataSetupSet(swapData *d, OUT void **datactx);

#define setObjectMetaType lenObjectMetaType
#define createSetObjectMeta(version, len) createLenObjectMeta(OBJ_SET, version, len)

unsigned long swap_setTypeSize(const objectMeta *meta, const robj *o);
unsigned long swap_setTypeSizeLookup(redisDb *db, robj *key, const robj *o);
void swap_scardCommand(client *c);

/* List */
typedef struct listSwapData {
  swapData d;
} listSwapData;

typedef struct listDataCtx {
  struct listMeta *swap_meta;
  argRewriteRequest arg_reqs[2];
  int ctx_flag;
} listDataCtx;

objectMeta *createListObjectMeta(uint64_t version, MOVE struct listMeta *list_meta);
int swapDataSetupList(swapData *d, void **pdatactx);

typedef struct argRewrite {
  argRewriteRequest arg_req;
  robj *orig_arg; /* own */
} argRewrite;

#define ARG_REWRITES_MAX 2
typedef struct argRewrites {
  int num;
  argRewrite rewrites[ARG_REWRITES_MAX];
} argRewrites;

argRewrites *argRewritesCreate(void);
void argRewritesAdd(argRewrites *arg_rewrites, argRewriteRequest arg_req, MOVE robj *orig_arg);
void argRewritesReset(argRewrites *arg_rewrites);
void argRewritesFree(argRewrites *arg_rewrites);
void argRewritesAdd(argRewrites *arg_rewrites, argRewriteRequest arg_req, MOVE robj *orig_arg);

void clientArgRewritesRestore(client *c);
void clientArgRewrite(client *c, argRewriteRequest arg_req, MOVE robj *new_arg);


long swapListTypeLength(robj *list, objectMeta *object_meta);
void swapListTypePush(robj *subject, robj *value, int where, redisDb *db, robj *key);
robj *swapListTypePop(robj *subject, int where, redisDb *db, robj *key);
void swapListMetaDelRange(redisDb *db, robj *key, long ltrim, long rtrim);
/* zset */
typedef struct zsetSwapData {
  swapData sd;
} zsetSwapData;

#define ZSET_SWAP_CTX_TYPE_NONE 0
#define ZSET_SWAP_CTX_TYPE_ZS 1

typedef struct zsetDataCtx {
	baseBigDataCtx bdc;
  int type;
  union {
    struct {
      zrangespec* rangespec;
      int reverse;
      int limit;
    } zs;
  };

} zsetDataCtx;
int swapDataSetupZSet(swapData *d, OUT void **datactx);
#define createZsetObjectMeta(version, len) createLenObjectMeta(OBJ_ZSET, version, len)
#define zsetObjectMetaType lenObjectMetaType

size_t swap_zsetLengthLookup(redisDb *db, robj *key, robj* zobj);

/* bitmap */
struct bitmapMeta;

void bitmapMetaFree(struct bitmapMeta *bitmap_meta);

size_t bitmapMetaGetSize(struct bitmapMeta *bitmap_meta);

size_t bitmapMetaGetSubkeySize(struct bitmapMeta *bitmap_meta);

objectMeta *createBitmapObjectMeta(uint64_t version, MOVE struct bitmapMeta *bitmap_meta);

int swapDataSetupBitmap(swapData *d, void **pdatactx);

sds genSwapBitmapStringSwitchedInfoString(sds info);

/* Meta bitmap */
/* meta != NULL, bitmap with hole, which means cold subkey, it is not entire bitmap in memory.
 * meta == NULL,  no hole in bitmap, it is entire bitmap in memory. */
typedef struct metaBitmap {
    struct bitmapMeta *meta;
    robj *bitmap;
} metaBitmap;

void metaBitmapInit(metaBitmap *meta_bitmap, struct bitmapMeta *bitmap_meta, robj *bitmap);
unsigned long metaBitmapGetSize(metaBitmap *meta_bitmap);
void metaBitmapGrow(metaBitmap *meta_bitmap, size_t byte);
unsigned long metaBitmapGetColdSubkeysSize(metaBitmap *meta_bitmap, unsigned long offset);
void metaBitmapBitpos(metaBitmap *meta_bitmap, client *c, unsigned long bit);
void metaBitmapBitcount(metaBitmap *meta_bitmap, client *c);

/* MetaScan */
#define DEFAULT_SCANMETA_BUFFER 16

typedef struct scanMeta {
  sds key;
  long long expire;
  int swap_type;
} scanMeta;

int scanMetaExpireIfNeeded(redisDb *db, scanMeta *meta);

typedef struct metaScanResult {
  scanMeta buffer[DEFAULT_SCANMETA_BUFFER];
  scanMeta *metas;
  int num;
  int size;
  sds nextseek;
} metaScanResult;

metaScanResult *metaScanResultCreate(void);
void metaScanResultAppend(metaScanResult *result, int swap_type, MOVE sds key, long long expire);
void metaScanResultSetNextSeek(metaScanResult *result, MOVE sds nextseek);
void freeScanMetaResult(metaScanResult *result);

typedef struct metaScanSwapData {
  swapData d;
} metaScanSwapData;

/* There are three kinds of meta scan ctx: scan/randomkey/activeexpire */
struct metaScanDataCtx;

typedef struct metaScanDataCtxType {
    void (*swapAna)(struct metaScanDataCtx *datactx, int *intention, uint32_t *intention_flags);
    void (*swapIn)(struct metaScanDataCtx *datactx, metaScanResult *result);
    void (*freeExtend)(struct metaScanDataCtx *datactx);
} metaScanDataCtxType;

typedef struct metaScanDataCtx {
    metaScanDataCtxType *type;
    client *c;
    int limit;
    sds seek;
    void *extend;
} metaScanDataCtx;

int swapDataSetupMetaScan(swapData *d, uint32_t intention_flags, client *c, OUT void **datactx);

robj *metaScanResultRandomKey(redisDb *db, metaScanResult *result);

#define EXPIRESCAN_DEFAULT_LIMIT 32
#define EXPIRESCAN_DEFAULT_CANDIDATES (16*1024)

extern dict *slaveKeysWithExpire;

typedef struct expireCandidates {
    robj *zobj;
    size_t capacity;
} expireCandidates;

expireCandidates *expireCandidatesCreate(size_t capacity);
void freeExpireCandidates(expireCandidates *candidates);

typedef struct scanExpire {
    expireCandidates *candidates;
    int inprogress;
    sds nextseek;
    int limit;
    double stale_percent;
    long long stat_estimated_cycle_seconds;
    size_t stat_scan_per_sec;
    size_t stat_expired_per_sec;
    long long stat_scan_time_used;
    long long stat_expire_time_used;
} scanExpire;

scanExpire *scanExpireCreate(void);
void scanExpireFree(scanExpire *scan_expire);
void scanExpireEmpty(scanExpire *scan_expire);

void swapScanexpireCommand(client *c);
int scanExpireDbCycle(redisDb *db, int type, long long timelimit);
sds genSwapScanExpireInfoString(sds info);

void expireSlaveKeysSwapMode(void);

/* swap scan sessions */

#define cursorIsHot(outer_cursor) ((outer_cursor & 0x1UL) == 0)
#define cursorOuterToInternal(outer_cursor) (outer_cursor >> 1)
#define cursorInternalToOuter(outer_cursor, cursor) (cursor << 1 | (outer_cursor & 0x1UL))

static inline unsigned long cursorGetSessionId(unsigned long outer_cursor) {
    return cursorOuterToInternal(outer_cursor) & ((1<<server.swap_scan_session_bits)-1);
}

static inline unsigned long cursorGetSessionSeq(unsigned long outer_cursor) {
    return cursorOuterToInternal(outer_cursor) >> server.swap_scan_session_bits;
}

typedef struct swapScanSession {
    time_t last_active;
    unsigned long session_id; /* inner */
    sds nextseek;
    unsigned long nextcursor; /* inner cursor */
    int binded;
} swapScanSession;

typedef struct swapScanSessionsStat {
    uint64_t assigned_failed;
    uint64_t assigned_succeded;
    uint64_t bind_failed;
    uint64_t bind_succeded;
} swapScanSessionsStat;

typedef struct swapScanSessions {
    rax *assigned;
    list *free;
    swapScanSession *array;
    swapScanSessionsStat stat;
} swapScanSessions;

static inline unsigned long swapScanSessionGetNextCursor(swapScanSession *session) {
    return session->nextcursor;
}

static inline void swapScanSessionIncrNextCursor(swapScanSession *session) {
    session->nextcursor += 1 << server.swap_scan_session_bits;
}

static inline void swapScanSessionZeroNextCursor(swapScanSession *session) {
    session->nextcursor = session->session_id;
}

static inline int swapScanSessionFinished(swapScanSession *session) {
    return session->nextseek == NULL;
}

swapScanSessions *swapScanSessionsCreate(int bits);
void swapScanSessionRelease(swapScanSessions *sessions);
swapScanSession *swapScanSessionsAssign(swapScanSessions *sessions);
swapScanSession *swapScanSessionsBind(swapScanSessions *sessions, unsigned long cursor, int *reason);
void swapScanSessionUnbind(swapScanSession *session, sds nextseek);
swapScanSession *swapScanSessionsFind(swapScanSessions *sessions, unsigned long outer_cursor);
void swapScanSessionUnassign(swapScanSessions *sessions, swapScanSession *session);

sds genSwapScanSessionStatString(sds info);
sds getAllSwapScanSessionsInfoString(long long outer_cursor);

/* Exec */
#define SWAP_MODE_ASYNC 0
#define SWAP_MODE_PARALLEL_SYNC 1

struct swapRequestBatch;

typedef void (*swapRequestFinishedCallback)(swapData *data, void *pd, int errcode);

#define SWAP_REQUEST_TYPE_DATA 0
#define SWAP_REQUEST_TYPE_META 0

typedef struct swapRequest {
  keyRequest *key_request; /* key_request for meta swap request */
  int intention; /* intention for data swap request */
  uint32_t intention_flags;
  swapCtx *swap_ctx;
  swapData *data;
  void *datactx;
  void *result; /* ref (create in decodeData, moved to swapIn) */
  swapRequestFinishedCallback finish_cb;
  void *finish_pd;
  redisAtomic size_t swap_memory;
#ifdef SWAP_DEBUG
  swapDebugMsgs *msgs;
#endif
  int errcode;
  swapTrace *trace;
} swapRequest;

swapRequest *swapRequestNew(keyRequest *key_request, int intention,
    uint32_t intention_flags, swapCtx *ctx, swapData *data, void *datactx,
    swapTrace *trace, swapRequestFinishedCallback cb, void *pd, void *msgs);

static inline swapRequest *swapDataRequestNew(
    int intention, uint32_t intention_flags, swapCtx *ctx, swapData *data,
    void *datactx, swapTrace *trace, swapRequestFinishedCallback cb, void *pd,
    void *msgs) {
  return swapRequestNew(NULL,intention,intention_flags,ctx,data,datactx,trace,cb,pd,msgs);
}
static inline swapRequest *swapMetaRequestNew(
    keyRequest *key_request, swapCtx *ctx, swapData *data,
    void *datactx, swapTrace *trace, swapRequestFinishedCallback cb, void *pd,
    void *msgs) {
  return swapRequestNew(key_request,SWAP_UNSET,-1,ctx,data,datactx,trace,cb,pd,msgs);
}
static inline int swapRequestGetError(swapRequest *req) {
  return req->errcode;
}
static inline void swapRequestSetError(swapRequest *req, int errcode) {
  req->errcode = errcode;
}
static inline int swapRequestIsMetaType(swapRequest *req) {
    return req->key_request != NULL;
}

void swapRequestFree(swapRequest *req);
void swapRequestUpdateStatsExecuted(swapRequest *req);
void swapRequestUpdateStatsCallback(swapRequest *req);
void swapRequestMerge(swapRequest *req);


#define SWAP_BATCH_DEFAULT_SIZE 16
#define SWAP_BATCH_LINEAR_SIZE  4096

typedef void (*swapRequestBatchNotifyCallback)(struct swapRequestBatch *req, void *pd);

typedef struct swapRequestBatch {
  swapRequest *req_buf[SWAP_BATCH_DEFAULT_SIZE];
  swapRequest **reqs;
  size_t capacity;
  size_t count;
  swapRequestBatchNotifyCallback notify_cb;
  void *notify_pd;
  monotime notify_queue_timer;
  monotime swap_queue_timer;
} swapRequestBatch;

swapRequestBatch *swapRequestBatchNew(void);
void swapRequestBatchFree(swapRequestBatch *reqs);
void swapRequestBatchAppend(swapRequestBatch *reqs, swapRequest *req);
void swapRequestBatchExecute(swapRequestBatch *reqs);
void swapRequestBatchProcess(swapRequestBatch *reqs);
void swapRequestBatchCallback(swapRequestBatch *reqs);
void swapRequestBatchDispatched(swapRequestBatch *reqs);
void swapRequestBatchStart(swapRequestBatch *reqs);
void swapRequestBatchEnd(swapRequestBatch *reqs);

typedef struct swapExecBatch {
    swapRequest *req_buf[SWAP_BATCH_DEFAULT_SIZE];
    swapRequest **reqs;
    size_t count;
    size_t capacity;
    int intention;
    int action;
    monotime swap_timer;
} swapExecBatch;

void swapExecBatchInit(swapExecBatch *exec_batch);
void swapExecBatchDeinit(swapExecBatch *exec_batch);
void swapExecBatchAppend(swapExecBatch *exec_batch, swapRequest *req);
void swapExecBatchPreprocess(swapExecBatch *meta_batch);
void swapExecBatchExecute(swapExecBatch *exec_batch);
static inline int swapExecBatchEmpty(swapExecBatch *exec_batch) {
    return exec_batch->count == 0;
}
static inline void swapExecBatchSetError(swapExecBatch *exec_batch, int errcode) {
    for (size_t i = 0; i < exec_batch->count; i++) {
        swapRequestSetError(exec_batch->reqs[i],errcode);
    }
}
static inline int swapExecBatchGetError(swapExecBatch *exec_batch) {
    int errcode;
    for (size_t i = 0; i < exec_batch->count; i++) {
        if ((errcode = swapRequestGetError(exec_batch->reqs[i])))
            return errcode;
    }
    return 0;
}

/* swapExecBatchCtx struct is identical with swapExecBatch */
typedef swapExecBatch swapExecBatchCtx;

#define swapExecBatchCtxInit swapExecBatchInit
#define swapExecBatchCtxDeinit swapExecBatchDeinit
#define swapExecBatchCtxEmpty swapExecBatchEmpty
static inline void swapExecBatchCtxReset(swapExecBatch *exec_batch, int intention, int action) {
    exec_batch->count = 0;
    exec_batch->intention = intention;
    exec_batch->action = action;
}
void swapExecBatchCtxFeed(swapExecBatch *exec_ctx, swapRequest *req);


/* Threads (encode/rio/decode/finish) */
#define SWAP_THREADS_DEFAULT     4
#define SWAP_THREADS_MAX         64

typedef struct swapThread {
    int id;
    pthread_t thread_id;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    list *pending_reqs;
    redisAtomic unsigned long is_running_rio;
} swapThread;

int swapThreadsInit(void);
void swapThreadsDeinit(void);
void swapThreadsDispatch(struct swapRequestBatch *reqs, int idx);
int swapThreadsDrained(void);
sds genSwapThreadInfoString(sds info);


/* RIO */
#define ROCKS_UNSET             -1
#define ROCKS_NOP               0
#define ROCKS_GET             	1
#define ROCKS_PUT            	  2
#define ROCKS_DEL              	3
#define ROCKS_ITERATE           4
#define ROCKS_TYPES             5

static inline const char *rocksActionName(int action) {
  const char *name = "?";
  const char *actions[] = {"NOP", "GET", "PUT", "DEL", "ITERATE"};
  if (action >= 0 && (size_t)action < sizeof(actions)/sizeof(char*))
    name = actions[action];
  return name;
}

#define SWAP_DEBUG_LOCK_WAIT            0
#define SWAP_DEBUG_SWAP_QUEUE_WAIT      1
#define SWAP_DEBUG_NOTIFY_QUEUE_WAIT    2
#define SWAP_DEBUG_NOTIFY_QUEUE_HANDLES 3
#define SWAP_DEBUG_NOTIFY_QUEUE_HANDLE_TIME 4
#define SWAP_DEBUG_INFO_TYPE            5

static inline int rocksActionFromName(const char* name) {
    const char *actions[] = {"NOP", "GET", "PUT", "DEL", "WRITE", "MULTIGET", "ITERATE"};
    for (int i = 0; (size_t)i < sizeof(actions); i++) {
        if (!strcasecmp(name,actions[i])) {
            return i;
        }
    }

    return -1;
}

static inline const char *swapDebugName(int type) {
    const char *name = "?";
    const char *names[] = {"LOCK_WAIT", "SWAP_QUEUE_WAIT", "NOTIFY_QUEUE_WAIT", "NOTIFT_QUEUE_HANDLES", "NOTIFT_QUEUE_HANDLE_TIME"};
    if (type >= 0 && (size_t)type < sizeof(names)/sizeof(char*))
        name = names[type];
    return name;
}

typedef struct RIO {
	int action;
	union {
		struct {
			int numkeys;
      int *cfs;
			sds *rawkeys;
			sds *rawvals;
      int notfound;
		} get, put, del, generic;

    struct {
        int cf;
        uint32_t flags;
        sds start;
        sds end;
        size_t limit;
        int numkeys;
        sds *rawkeys;
        sds *rawvals;
        sds nextseek; /* own */
    } iterate;
	};
  sds err;
  int errcode;
  int oom_check;
  void *privdata;
} RIO;

#define ROCKS_ITERATE_NO_LIMIT 0
#define ROCKS_ITERATE_REVERSE (1<<0)
#define ROCKS_ITERATE_CONTINUOUSLY_SEEK (1<<1) /* return next seek start key */
#define ROCKS_ITERATE_LOW_BOUND_EXCLUDE (1<<2)
#define ROCKS_ITERATE_HIGH_BOUND_EXCLUDE (1<<3)
#define ROCKS_ITERATE_DISABLE_CACHE (1<<4)
#define ROCKS_ITERATE_PREFIX_MATCH (1<<5)

void RIOInitGet(RIO *rio, int numkeys, int *cfs, sds *rawkeys);
void RIOInitPut(RIO *rio, int numkeys, int *cfs, sds *rawkeys, sds *rawvals);
void RIOInitDel(RIO *rio, int numkeys, int *cfs, sds *rawkeys);
void RIOInitIterate(RIO *rio, int cf, uint32_t flags, sds start, sds end, size_t limit);
void RIODeinit(RIO *rio);
void RIODo(RIO *rio);

size_t RIOEstimatePayloadSize(RIO *rio);
void RIOUpdateStatsDo(RIO *rio, long duration);
void RIOUpdateStatsDataNotFound(RIO *rio);

static inline int RIOGetError(RIO *rio) {
  return rio->errcode;
}
static inline int RIOGetNotFound(RIO *rio) {
  return rio->get.notfound;
}
static inline void RIOSetError(RIO *rio, int errcode, MOVE sds err) {
  serverAssert(rio->errcode == 0 && rio->err == NULL);
  rio->errcode = errcode;
  rio->err = err;
}

/* rios with same type */
typedef struct RIOBatch {
   RIO rio_buf[SWAP_BATCH_DEFAULT_SIZE];
   RIO *rios;
   size_t capacity;
   size_t count;
   int action;
} RIOBatch;

void RIOBatchInit(RIOBatch *rios, int action);
void RIOBatchDeinit(RIOBatch *rios);
RIO *RIOBatchAlloc(RIOBatch *rios);
void RIOBatchDo(RIOBatch *rios);
void RIOBatchUpdateStatsDo(RIOBatch *rios, long duration);
void RIOBatchUpdateStatsDataNotFound(RIOBatch *rios);

#define SWAP_BATCH_FLUSH_FORCE_FLUSH    0
#define SWAP_BATCH_FLUSH_REACH_LIMIT    1
#define SWAP_BATCH_FLUSH_UTILS_TYPE     2
#define SWAP_BATCH_FLUSH_THREAD_SWITCH  3
#define SWAP_BATCH_FLUSH_INTENT_SWITCH  4
#define SWAP_BATCH_FLUSH_BEFORE_SLEEP   5
#define SWAP_BATCH_FLUSH_TYPES          6

static inline const char *swapBatchFlushTypeName(int type) {
    const char *name = "?";
    const char *names[] = {"FORCE_FLUSH", "REACH_LIMIT", "UTILS_TYPE", "THREAD_SWITCH", "INTENT_SWITCH", "BEFORE_SLEEP"};
    if (type >= 0 && (size_t)type < sizeof(names)/sizeof(char*))
        name = names[type];
    return name;
}

typedef struct swapBatchCtxStat {
  int stats_metric_idx_request;
  int stats_metric_idx_batch;
  long long submit_batch_count;
  long long submit_request_count;
  long long submit_batch_flush[SWAP_BATCH_FLUSH_TYPES];
} swapBatchCtxStat;

typedef struct swapBatchCtx {
  swapBatchCtxStat stat;
  swapRequestBatch *batch;
  int thread_idx;
  int cmd_intention;
} swapBatchCtx;

swapBatchCtx *swapBatchCtxNew(void);
void swapBatchCtxFree(swapBatchCtx *batch_ctx);
void swapBatchCtxFeed(swapBatchCtx *batch_ctx, int force_flush, swapRequest *req, int thread_idx);
size_t swapBatchCtxFlush(swapBatchCtx *batch_ctx, int reason);

void trackSwapBatchInstantaneousMetrics(void);
void resetSwapBatchInstantaneousMetrics(void);
sds genSwapBatchInfoString(sds info);

extern swapBatchLimitsConfig swapBatchLimitsDefaults[SWAP_TYPES];

/* Async */
#define ASYNC_COMPLETE_QUEUE_NOTIFY_READ_MAX  512

typedef struct asyncCompleteQueue {
    int notify_recv_fd;
    int notify_send_fd;
    pthread_mutex_t lock;
    list *complete_queue;
} asyncCompleteQueue;

int asyncCompleteQueueInit(void);
void asyncCompleteQueueDeinit(asyncCompleteQueue *cq);
void asyncCompleteQueueAppend(asyncCompleteQueue *cq, swapRequestBatch *reqs);
int asyncCompleteQueueDrain(mstime_t time_limit);

void asyncSwapRequestBatchSubmit(swapRequestBatch *reqs, int idx);

/* Parallel sync */
typedef struct {
    int inprogress;         /* swap entry in progress? */
    int pipe_read_fd;       /* read end to wait rio swap finish. */
    int pipe_write_fd;      /* write end to notify swap finish by rio. */
    swapRequestBatch *reqs;
} swapEntry;

typedef struct parallelSync {
    list *entries;
    int parallel;
    int mode;
} parallelSync;

int parallelSyncInit(int parallel);
void parallelSyncDeinit(void);
int parallelSyncDrain(void);

int parallelSyncSwapRequestBatchSubmit(swapRequestBatch *reqs, int idx);

static inline void submitSwapRequestBatch(int mode,swapRequestBatch *reqs, int idx) {
    if (mode == SWAP_MODE_ASYNC) {
        asyncSwapRequestBatchSubmit(reqs,idx);
    } else {
        parallelSyncSwapRequestBatchSubmit(reqs,idx);
    }
}

static inline void submitSwapRequest(int mode,swapRequest *req, int idx) {
  swapRequestBatch *reqs = swapRequestBatchNew();
  swapRequestBatchAppend(reqs,req);
  submitSwapRequestBatch(mode,reqs,idx);
}


/* Lock */
#define LOCK_LINKS_BUF_SIZE 2

typedef void (*lockProceedCallback)(void *lock, int flush, redisDb *db, robj *key, client *c, void *pd);

typedef struct lockLinkTarget {
  int signaled;
  int linked;
} lockLinkTarget;

typedef struct lockLinks {
  struct lockLink *buf[LOCK_LINKS_BUF_SIZE];
  struct lockLink **links;
  int capacity;
  int count;
  unsigned proceeded:1;
  unsigned unlocked:1;
  unsigned reserved:30;
} lockLinks;

typedef struct lockLink {
  int64_t txid;
  lockLinkTarget target;
  lockLinks links;
} lockLink;

typedef struct lock {
  lockLink link;
  struct locks *locks;
  listNode* locks_ln;
  redisDb *db;
  robj *key;
  client *c;
  lockProceedCallback proceed;
  void *pd;
  freefunc pdfree;
  int conflict;
  monotime lock_timer;
  long long start_time;
#ifdef SWAP_DEBUG
  swapDebugMsgs *msgs;
#endif
} lock;

typedef struct locks {
  int level;
  list *lock_list;
  struct locks *parent;
  union {
    struct {
      int dbnum;
      struct locks **dbs;
    } svr;
    struct {
      redisDb *db;
      dict *keys;
    } db;
    struct {
      robj *key;
    } key;
  };
} locks;

typedef struct lockInstantaneouStat {
    const char *name;
    redisAtomic long long request_count;
    redisAtomic long long conflict_count;
    redisAtomic long long wait_time;
    redisAtomic long long proceed_count;
    long long wait_time_maxs[STATS_METRIC_SAMPLES];
    int wait_time_max_index;
    int stats_metric_idx_request;
    int stats_metric_idx_conflict;
    int stats_metric_idx_wait_time;
    int stats_metric_idx_proceed_count;
} lockInstantaneouStat;

typedef struct lockCumulativeStat {
  size_t request_count;
  size_t conflict_count;
} lockCumulativeStat;

typedef struct lockStat {
  lockCumulativeStat cumulative;
  lockInstantaneouStat *instant; /* array of swap lock stats (one for each level). */
} lockStat;

typedef struct swapLock {
  locks *svrlocks;
  lockStat *stat;
} swapLock;

void swapLockCreate(void);
void swapLockDestroy(void);
int lockWouldBlock(int64_t txid, redisDb *db, robj *key);
int lockLock(int64_t txid, redisDb *db, robj *key, lockProceedCallback cb, client *c, void *pd, freefunc pdfree, void *msgs);
void lockProceeded(void *lock);
void lockUnlock(void *lock);

void trackSwapLockInstantaneousMetrics(void);
void resetSwapLockInstantaneousMetrics(void);
sds genSwapLockInfoString(sds info);

list *clientRenewLocks(client *c);
void clientGotLock(client *c, swapCtx *ctx, void *lock);
void clientReleaseLocks(client *c, swapCtx *ctx);

/* Evict */
#define EVICT_SUCC_SWAPPED      0
#define EVICT_SUCC_FREED        1
#define EVICT_FAIL_ABSENT       2
#define EVICT_FAIL_EVICTED      3
#define EVICT_FAIL_SWAPPING     4
#define EVICT_FAIL_UNSUPPORTED  5
#define EVICT_RESULT_TYPES      6

static inline const char *evictResultName(int evict_result) {
    const char *name = "?";
    const char *names[] = {"SUCC_SWAPPED", "SUCC_FREED", "FAIL_ABSENT", "FAIL_EVICTED", "FAIL_SWAPPING", "FAIL_UNSUPPORTED"};
    if (evict_result >= 0 && (size_t)evict_result < sizeof(names)/sizeof(char*))
        name = names[evict_result];
    return name;
}

static inline int evictResultIsSucc(int evict_result) {
  return evict_result <= EVICT_SUCC_FREED;
}

static inline int evictResultIsFreed(int evict_result) {
  return evict_result == EVICT_SUCC_FREED;
}

typedef struct swapEvictionStat {
    long long evict_result[EVICT_RESULT_TYPES];
} swapEvictionStat;

typedef struct swapEvictionCtx {
    long long inprogress_count; /* current inprogrss evict count */
    long long inprogress_limit; /* current inprogress limit,
                                   updated on performEviction start */
    long long failed_inrow;
    long long freed_inrow;
    swapEvictionStat stat;
} swapEvictionCtx;

#define swapEvictionFreedInrowIncr(ctx) do { ctx->freed_inrow++; } while(0)
#define swapEvictionFreedInrowReset(ctx) do { ctx->freed_inrow=0; } while(0)

swapEvictionCtx *swapEvictionCtxCreate(void);
void swapEvictionCtxFree(swapEvictionCtx *ctx);


int tryEvictKey(redisDb *db, robj *key, int *evict_result);
void tryEvictKeyAsapLater(redisDb *db, robj *key);
void swapEvictCommand(client *c);
void swapDebugEvictKeys(void);
unsigned long long calculateNextMemoryLimit(size_t mem_used, unsigned long long from, unsigned long long to);
void updateMaxMemoryScaleFrom(void);
int swapEvictGetInprogressLimit(size_t mem_tofree);
int swapEvictionReachedInprogressLimit(void);
sds genSwapEvictionInfoString(sds info);

#define EVICT_ASAP_OK 0
#define EVICT_ASAP_AGAIN 1
int swapEvictAsap(void);

typedef struct swapEvictKeysCtx {
    size_t mem_used;
    size_t mem_tofree;
    long long keys_scanned;
    long long swap_trigged;
    int ended;
} swapEvictKeysCtx;

void swap_startEvictionTimeProc(void);
size_t swap_getMemoryToFree(size_t mem_used);
void swap_performEvictionStart(swapEvictKeysCtx *sectx);
int swap_performEvictionLoopStartShouldBreak(swapEvictKeysCtx *sectx);
size_t performEvictionSwapSelectedKey(swapEvictKeysCtx *sectx, redisDb *db, robj *keyobj);
int swap_performEvictionLoopCheckShouldBreak(swapEvictKeysCtx *sectx);
void swap_performEvictionEnd(swapEvictKeysCtx *sectx);
static inline int swap_performEvictionLoopCheckInterval(int keys_freed) {
    return keys_freed % server.swap_evict_loop_check_interval == 0;
}
/* used memory in disk swap mode */
size_t coldFiltersUsedMemory(void); /* cuckoo filter not counted in maxmemory */
static inline size_t swap_getUsedMemory(void) {
  int swap_inprogress_memory;
  atomicGet(server.swap_inprogress_memory, swap_inprogress_memory);
  return zmalloc_used_memory() - swap_inprogress_memory -
      coldFiltersUsedMemory() - swapPersistCtxUsedMemory(server.swap_persist_ctx);
}
static inline int swap_evictionTimeProcGetDelayMillis(void) {
  if (swapEvictionReachedInprogressLimit()) return 1;
  else return 0;
}

#define SWAP_RATELIMIT_POLICY_PAUSE 0
#define SWAP_RATELIMIT_POLICY_REJECT_OOM 1
#define SWAP_RATELIMIT_POLICY_REJECT_ALL 2
#define SWAP_RATELIMIT_POLICY_DISABLED 3

#define SWAP_RATELIMIT_PAUSE_MAX_MS 200

typedef struct swapRatelimitCtx {
  int is_write_command;
  int is_read_command;
  int is_denyoom_command;
  int keyrequests_count;
} swapRatelimitCtx;

void swapRatelimitStart(swapRatelimitCtx *rlctx, client *c);
int swapRatelimitMaxmemoryNeeded(swapRatelimitCtx *rlctx, int policy, int *pms);
int swapRatelimitPersistNeeded(swapRatelimitCtx *rlctx, int policy, int *pms);
static inline int swapRatelimitNeeded(swapRatelimitCtx *rlctx, int policy, int *pms) {
  int pms0, pms1 = 0, pms2 = 0, maxmemory, persist;
  maxmemory = swapRatelimitMaxmemoryNeeded(rlctx,policy,&pms1);
  persist = swapRatelimitPersistNeeded(rlctx,policy,&pms2);
  pms0 = MAX(pms1, pms2);
  if (pms) *pms = pms0;
  return maxmemory || persist;
}
int swapRateLimitReject(swapRatelimitCtx *rlctx, client *c);
void swapRateLimitPause(swapRatelimitCtx *rlctx, client *c);
void trackSwapRateLimitInstantaneousMetrics(void);
void resetSwapRateLimitInstantaneousMetrics(void);
sds genSwapRateLimitInfoString(sds info);

/* Expire */
int submitExpireClientRequest(client *c, robj *key, int force);

/* Rocks */
#define ROCKS_DIR_MAX_LEN 512
#define ROCKS_DATA "data.rocks"
#define ROCKS_DISK_HEALTH_DETECT_FILE "disk_health_detect"

/* Rocksdb engine */
typedef struct rocks {
    int rocksdb_epoch;
    rocksdb_t *db;
    rocksdb_options_t *cf_opts[CF_COUNT];
    rocksdb_column_family_handle_t *cf_handles[CF_COUNT];
    rocksdb_options_t *db_opts;
    rocksdb_readoptions_t *ropts;
    rocksdb_writeoptions_t *wopts;
    rocksdb_readoptions_t *filter_meta_ropts;
    const rocksdb_snapshot_t *snapshot;
    pthread_rwlock_t rwlock[1];
} rocks;

rocks *serverRocksGetReadLock(void);
rocks *serverRocksGetTryReadLock(void);
rocks *serverRocksGetWriteLock(void);
void serverRocksUnlock(rocks *rocks);

static inline rocksdb_column_family_handle_t *swapGetCF(int cf) {
    serverAssert(cf < CF_COUNT);
    return server.rocks->cf_handles[cf];
}

static inline const char *swapGetCFName(int cf) {
    serverAssert(cf < CF_COUNT);
    return swap_cf_names[cf];
}

typedef struct rocksdbMemOverhead {
  size_t total;
  size_t block_cache;
  size_t index_and_filter;
  size_t memtable;
  size_t pinned_blocks;
} rocksdbMemOverhead;


#define SWAP_FORK_ROCKSDB_TYPE_CHECKPOINT 0
#define SWAP_FORK_ROCKSDB_TYPE_SNAPSHOT   1

struct swapForkRocksdbCtx;

typedef struct swapForkRocksdbType {
  void (*init)(struct swapForkRocksdbCtx *sfrctx);
  int (*beforeFork)(struct swapForkRocksdbCtx *sfrctx);
  int (*afterForkChild)(struct swapForkRocksdbCtx *sfrctx);
  int (*afterForkParent)(struct swapForkRocksdbCtx *sfrctx, int childpid);
  void (*deinit)(struct swapForkRocksdbCtx *sfrctx);
} swapForkRocksdbType;

typedef struct swapForkRocksdbCtx {
  swapForkRocksdbType *type;
  void *extend;
} swapForkRocksdbCtx;

swapForkRocksdbCtx *swapForkRocksdbCtxCreate(int mode);
void swapForkRocksdbCtxRelease(swapForkRocksdbCtx *sfrctx);

static inline int swapForkRocksdbBefore(swapForkRocksdbCtx *sfrctx) {
  return sfrctx->type->beforeFork(sfrctx);
}
static inline int swapForkRocksdbAfterChild(swapForkRocksdbCtx *sfrctx) {
  return sfrctx->type->afterForkChild(sfrctx);
}
static inline int swapForkRocksdbAfterParent(swapForkRocksdbCtx *sfrctx, int childpid) {
  return sfrctx->type->afterForkParent(sfrctx,childpid);
}

/* compaction filter */
typedef enum {
  FILTER_STATE_CLOSE = 0,
  FILTER_STATE_OPEN
} filterState;
int setFilterState(filterState state);
filterState getFilterState(void);
rocksdb_compactionfilterfactory_t* createDataCfCompactionFilterFactory(void);
rocksdb_compactionfilterfactory_t* createScoreCfCompactionFilterFactory(void);

int serverRocksInit(void);
int rocksFlushDB(int dbid);
void serverRocksCron(void);
int rocksCreateCheckpoint(rocks *rocks, sds checkpoint_dir);
void rocksReleaseCheckpoint(rocks *rocks);
void rocksReleaseSnapshot(rocks *rocks);
int rocksCreateSnapshot(rocks *rocks);
int rocksRestore(rocks *rocks, const char *checkpoint_dir);
struct rocksdbMemOverhead *rocksGetMemoryOverhead(rocks *rocks);
void rocksFreeMemoryOverhead(struct rocksdbMemOverhead *mh);
sds genRocksdbInfoString(sds info);
sds genRocksdbStatsString(sds section, sds info);
int rocksPropertyInt(rocks *rocks, const char *cfnames, const char *propname, uint64_t *out_val);
sds rocksPropertyValue(rocks *rocks, const char *cfnames, const char *propname);
char *rocksdbVersion(void);

/* rocksdb util task: flush */
typedef struct swapData4RocksdbFlush {
  sds cfnames;
  mstime_t start_time;
  int rocksdb_epoch;
} swapData4RocksdbFlush;

int swapShouldFlushMeta(void);
swapData4RocksdbFlush *rocksdbFlushTaskArgCreate(rocks *rocks, const char *cfnames);
void rocksdbFlushTaskArgRelease(swapData4RocksdbFlush *data);
void rocksdbFlushTaskDone(void *result, void *pd, int errcode);

/* rocksdb util task: getstats */
typedef struct rocksdbCFInternalStats {
  char* rocksdb_stats_cache;
  uint64_t num_entries_imm_mem_tables;
  uint64_t num_deletes_imm_mem_tables;
  uint64_t num_entries_active_mem_table;
  uint64_t num_deletes_active_mem_table;
} rocksdbCFInternalStats;

typedef struct rocksdbInternalStats {
  rocksdbCFInternalStats cfs[CF_COUNT];
}rocksdbInternalStats;

rocksdbInternalStats *rocksdbInternalStatsNew(void);
void rocksdbInternalStatsFree(rocksdbInternalStats *internal_stats);
void rocksdbGetStatsTaskDone(void *result, void *pd, int errcode);

/* rocksdb util task: checkpoint */
typedef struct rocksdbCreateCheckpointPayload {
    pid_t waiting_child;
    int checkpoint_dir_pipe_writing;
} rocksdbCreateCheckpointPayload;

typedef struct checkpointDirPipeWritePayload {
    sds data;
    ssize_t written;
    pid_t waiting_child;
} checkpointDirPipeWritePayload;

typedef struct rocksdbCreateCheckpointResult{
    rocksdb_checkpoint_t *checkpoint;
    sds checkpoint_dir;
} rocksdbCreateCheckpointResult;

/* rocksdb util task: collect cf meta */
typedef struct cfMetas {
  uint num;
  rocksdb_column_family_metadata_t** cf_meta;
} cfMetas;

cfMetas *cfMetasNew(uint cf_num);
void cfMetasFree(cfMetas *metas);

typedef struct cfIndexes {
  uint num;
  int *index;
} cfIndexes;

cfIndexes *cfIndexesNew(uint num);
void cfIndexesFree(cfIndexes *indexes);

/* rocksdb util task: compact */
typedef struct compactKeyRange {
  uint cf_index;
  char *start_key;
  char *end_key;
  size_t start_key_size;
  size_t end_key_size;
} compactKeyRange;

compactKeyRange *compactKeyRangeNew(uint cf_index, char *start_key, char *end_key, size_t start_key_size, size_t end_key_size);
void compactKeyRangeFree(compactKeyRange *range);

#define TYPE_TTL_COMPACT 0
#define TYPE_FULL_COMPACT 1
typedef struct compactTask {
  int compact_type;
  uint count;
  uint capacity;
  compactKeyRange **key_range;
} compactTask;

compactTask *compactTaskNew(int compact_type);
void compactTaskFree(compactTask *task);
void compactTaskAppend(compactTask *task, compactKeyRange *key_range);

void rocksdbCompactRangeTaskDone(void *result, void *pd, int errcode);
void genServerTtlCompactTask(void *result, void *pd, int errcode);

#define SWAP_TTL_COMPACT_INVALID_EXPIRE LLONG_MAX /* expire or pexpire is long long int. */
#define SWAP_TTL_COMPACT_DEFAULT_EXPIRE_WT_WINDOW 86400000 /* ms, 24h */

typedef struct swapExpireStatus {
    long long sst_age_limit; /* milliseconds, both in master and slave, sub-slave, master will propagate it to slave */
    wtdigest *expire_wt; /* only in master, save in milliseconds */
} swapExpireStatus;

typedef struct swapTtlCompactCtx {
    compactTask *task; /* move to utilctx during serverCron. */
    swapExpireStatus *expire_stats;
    redisAtomic unsigned long long stat_request_compact_times;
    redisAtomic unsigned long long stat_request_sst_count;
    redisAtomic unsigned long long stat_expired_sst_count;
    redisAtomic unsigned long long stat_compacted_data_size;
} swapTtlCompactCtx;

swapTtlCompactCtx *swapTtlCompactCtxNew();
void swapTtlCompactCtxFree(swapTtlCompactCtx *ctx);
void swapTtlCompactCtxReset(swapTtlCompactCtx *ctx);

swapExpireStatus *swapExpireStatusNew();
void swapExpireStatusFree(swapExpireStatus *stats);
void swapExpireStatusReset(swapExpireStatus *stats);

sds genSwapTtlCompactInfoString(sds info);

void ttlCompactRefreshSstAgeLimit(void);
void ttlCompactProduceTask(void);
void ttlCompactConsumeTask(void);

/* swap info cmd */
#define SWAP_INFO_SUPPORTED_YES 0
#define SWAP_INFO_SUPPORTED_NO 1
#define SWAP_INFO_SUPPORTED_AUTO 2

void swapInfoCommand(client *c);

/* swap info propagate mode */
#define SWAP_INFO_PROPAGATE_BY_PING 0
#define SWAP_INFO_PROPAGATE_BY_SWAP_INFO 1

void swapBuildSwapInfoSstAgeLimitCmd(robj *argv[3], long long sst_age_limit);
void swapDestorySwapInfoSstAgeLimitCmd(robj *argv[3]);

void swapPropagateSwapInfoCmd(int argc, robj **argv);

sds swapEncodeSwapInfo(int swap_info_argc, sds *swap_info_argv);
sds *swapDecodeSwapInfo(sds argv, int *swap_info_argc);
void swapApplySwapInfo(int swap_info_argc, sds *swap_info_argv);

/* Repl */
int submitReplClientRequests(client *c);
sds genSwapReplInfoString(sds info);

/* Swap */
void swapInit(void);
int dbSwap(client *c);
int clientSwap(client *c);
void continueProcessCommand(client *c);
int replClientSwap(client *c);
void replicationCacheSwapDrainingMaster(client *c);
void replicationHandleMasterDisconnectionWithoutReconnect(void);
void replClientDiscardSwappingState(client *c);
void submitDeferredClientKeyRequests(client *c, getKeyRequestsResult *result, clientKeyRequestFinished cb, void* ctx_pd);
void submitClientKeyRequests(client *c, getKeyRequestsResult *result, clientKeyRequestFinished cb, void* ctx_pd);
int submitNormalClientRequests(client *c);
void keyRequestBeforeCall(client *c, swapCtx *ctx);
void swapMutexopCommand(client *c);
int lockGlobalAndExec(clientKeyRequestFinished locked_op, uint64_t exclude_mark);
uint64_t dictEncObjHash(const void *key);
int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2);
void dictObjectDestructor(void *privdata, void *val);

/* Swap rate limit */
#define SWAP_RL_NO      0
#define SWAP_RL_SLOW    1
#define SWAP_RL_STOP    2

#define SWAP_STAT_METRIC_COUNT 0
#define SWAP_STAT_METRIC_BATCH 1
#define SWAP_STAT_METRIC_MEMORY 2
#define SWAP_STAT_METRIC_TIME 3
#define SWAP_STAT_METRIC_SIZE 4

#define SWAP_DEBUG_COUNT 0
#define SWAP_DEBUG_VALUE 1
#define SWAP_DEBUG_SIZE 2

#define COMPACTION_FILTER_METRIC_FILT 0
#define COMPACTION_FILTER_METRIC_SCAN 1
#define COMPACTION_FILTER_METRIC_RIO 2
#define COMPACTION_FILTER_METRIC_SIZE 3

#define SWAP_LOCK_METRIC_REQUEST 0
#define SWAP_LOCK_METRIC_CONFLICT 1
#define SWAP_LOCK_METRIC_WAIT_TIME 2
#define SWAP_LOCK_METRIC_PROCEED_COUNT 3
#define SWAP_LOCK_METRIC_SIZE 4

#define SWAP_BATCH_STATS_METRIC_SUBMIT_REQUEST 0
#define SWAP_BATCH_STATS_METRIC_SUBMIT_BATCH 1
#define SWAP_BATCH_STATS_METRIC_COUNT 2

#define SWAP_FILTER_STATS_METRIC_LOOKUP 0
#define SWAP_FILTER_STATS_METRIC_FALSE_POSITIVE 1
#define SWAP_FILTER_STATS_METRIC_COUNT 2

#define SWAP_RATELIMIT_STATS_METRIC_PAUSE_COUNT 0
#define SWAP_RATELIMIT_STATS_METRIC_PAUSE_MS 1
#define SWAP_RATELIMIT_STATS_METRIC_COUNT 2

#define SWAP_SWAP_STATS_METRIC_COUNT (SWAP_STAT_METRIC_SIZE*SWAP_TYPES)
#define SWAP_RIO_STATS_METRIC_COUNT (SWAP_STAT_METRIC_SIZE*ROCKS_TYPES)
#define SWAP_COMPACTION_FILTER_STATS_METRIC_COUNT (COMPACTION_FILTER_METRIC_SIZE*CF_COUNT)
#define SWAP_DEBUG_STATS_METRIC_COUNT (SWAP_DEBUG_SIZE*SWAP_DEBUG_INFO_TYPE)
#define SWAP_LOCK_STATS_METRIC_COUNT (SWAP_LOCK_METRIC_SIZE*REQUEST_LEVEL_TYPES)

/* stats metrics are ordered mem>swap>rio */
#define SWAP_SWAP_STATS_METRIC_OFFSET STATS_METRIC_COUNT_MEM
#define SWAP_RIO_STATS_METRIC_OFFSET (SWAP_SWAP_STATS_METRIC_OFFSET+SWAP_SWAP_STATS_METRIC_COUNT)
#define SWAP_COMPACTION_FILTER_STATS_METRIC_OFFSET (SWAP_RIO_STATS_METRIC_OFFSET+SWAP_RIO_STATS_METRIC_COUNT)
#define SWAP_DEBUG_STATS_METRIC_OFFSET (SWAP_COMPACTION_FILTER_STATS_METRIC_OFFSET+SWAP_COMPACTION_FILTER_STATS_METRIC_COUNT)
#define SWAP_LOCK_STATS_METRIC_OFFSET (SWAP_DEBUG_STATS_METRIC_OFFSET+SWAP_DEBUG_STATS_METRIC_COUNT)
#define SWAP_BATCH_STATS_METRIC_OFFSET (SWAP_LOCK_STATS_METRIC_OFFSET+SWAP_LOCK_STATS_METRIC_COUNT)
#define SWAP_FILTER_STATS_METRIC_OFFSET (SWAP_BATCH_STATS_METRIC_OFFSET+SWAP_BATCH_STATS_METRIC_COUNT)
#define SWAP_RATELIMIT_STATS_METRIC_OFFSET (SWAP_FILTER_STATS_METRIC_OFFSET+SWAP_FILTER_STATS_METRIC_COUNT)

#define SWAP_STATS_METRIC_COUNT (SWAP_SWAP_STATS_METRIC_COUNT+SWAP_RIO_STATS_METRIC_COUNT+SWAP_COMPACTION_FILTER_STATS_METRIC_COUNT+SWAP_DEBUG_STATS_METRIC_COUNT+SWAP_LOCK_STATS_METRIC_COUNT+SWAP_BATCH_STATS_METRIC_COUNT+SWAP_FILTER_STATS_METRIC_COUNT+SWAP_RATELIMIT_STATS_METRIC_COUNT)

typedef struct swapStat {
    const char *name;
    redisAtomic size_t count;
    redisAtomic size_t batch;
    redisAtomic size_t memory;
    redisAtomic size_t time;
    int stats_metric_idx_count;
    int stats_metric_idx_batch;
    int stats_metric_idx_memory;
    int stats_metric_idx_time;
} swapStat;

typedef struct compactionFilterStat {
    const char *name;
    redisAtomic long long filt_count;
    redisAtomic long long scan_count;
    redisAtomic long long rio_count;
    int stats_metric_idx_filt;
    int stats_metric_idx_scan;
    int stats_metric_idx_rio;
} compactionFilterStat;

typedef struct swapHitStat {
    redisAtomic long long stat_swapin_attempt_count;
    redisAtomic long long stat_swapin_not_found_coldfilter_cuckoofilter_filt_count;
    redisAtomic long long stat_swapin_not_found_coldfilter_absentcache_filt_count;
    redisAtomic long long stat_swapin_not_found_coldfilter_miss_count;
    redisAtomic long long stat_swapin_no_io_count;
    redisAtomic long long stat_swapin_data_not_found_count;
    redisAtomic long long stat_absent_subkey_query_count;
    redisAtomic long long stat_absent_subkey_filt_count;
} swapHitStat;

static inline int isSwapHitStatKeyRequest(keyRequest *kr) {
    return kr && kr->cmd_intention == SWAP_IN &&
        !isMetaScanRequest(kr->cmd_intention_flags);
}

void resetSwapHitStat(void);
sds genSwapHitInfoString(sds info);

typedef struct rorStat {
    struct swapStat *swap_stats; /* array of swap stats (one for each swap type). */
    struct swapStat *rio_stats; /* array of rio stats (one for each rio type). */
    struct compactionFilterStat *compaction_filter_stats; /* array of compaction filter stats (one for each column family). */
} rorStat;

void initStatsSwap(void);
void resetStatsSwap(void);

static inline void updateCompactionFiltSuccessCount(int cf) {
    atomicIncr(server.ror_stats->compaction_filter_stats[cf].filt_count, 1);
}
static inline void updateCompactionFiltScanCount(int cf) {
    atomicIncr(server.ror_stats->compaction_filter_stats[cf].scan_count, 1);
}
static inline void updateCompactionFiltRioCount(int cf) {
    atomicIncr(server.ror_stats->compaction_filter_stats[cf].rio_count, 1);
}

typedef
struct swapDebugInfo {
    const char *name;
    redisAtomic size_t count;
    redisAtomic size_t value;
    int metric_idx_count;
    int metric_idx_value;
} swapDebugInfo;
void metricDebugInfo(int type, long val);

void trackSwapInstantaneousMetrics(void);
sds genSwapInfoString(sds info);
sds genSwapStorageInfoString(sds info);
sds genSwapExecInfoString(sds info);
sds genSwapUnblockInfoString(sds info);

/* Rocks iter thread */
#define ITER_BUFFER_CAPACITY_DEFAULT 4096
#define ITER_NOTIFY_BATCH 32

typedef struct iterResult {
    int cf;
    sds rawkey;
    unsigned char rdbtype;
    sds rawval;
} iterResult;

typedef struct bufferedIterCompleteQueue {
    int buffer_capacity;
    iterResult *buffered;
    int iter_finished;
    int64_t buffered_count;
    int64_t processed_count;
    pthread_mutex_t buffer_lock;
    pthread_cond_t ready_cond;
    pthread_cond_t vacant_cond;
} bufferedIterCompleteQueue;

typedef struct rocksIter{
    redisDb *db;
    struct rocks *rocks;
    pthread_t io_thread;
    bufferedIterCompleteQueue *buffered_cq;
    rocksdb_column_family_handle_t *cf_handles[CF_COUNT];
    rocksdb_iterator_t *data_iter;
    rocksdb_iterator_t *meta_iter;
    rocksdb_t* checkpoint_db;
    sds data_endkey;
    sds meta_endkey;
} rocksIter;

rocksIter *rocksCreateIter(struct rocks *rocks, redisDb *db);
int rocksIterSeekToFirst(rocksIter *it);
int rocksIterNext(rocksIter *it);
void rocksIterCfKeyTypeValue(rocksIter *it, int *cf, sds *rawkey, unsigned char *type, sds *rawval);
void rocksReleaseIter(rocksIter *it);
void rocksIterGetError(rocksIter *it, char **error);

/* Rdb save */
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

typedef enum swapRdbSaveErrType {
    SAVE_ERR_NONE,
    SAVE_ERR_META_LEN_MISMATCH,
    SAVE_ERR_UNRECOVERABLE
} swapRdbSaveErrType;

void openSwapChildErrPipe(void);
void closeSwapChildErrPipe(void);
void sendSwapChildErr(swapRdbSaveErrType err_type, int dbid, sds key);
void receiveSwapChildErrs(void);
void swapLoadCommand(client *c);
int tryLoadKey(redisDb *db, robj *key, int oom_sensitive);

/* result that decoded from current rocksIter value */
typedef struct decodedResult {
  int cf;
  int dbid;
  sds key;
  void *reserved[6];
} decodedResult;

typedef struct decodedMeta {
  int cf;
  int dbid;
  sds key;
  uint64_t version;
  int swap_type;
  long long expire;
  sds extend;
} decodedMeta;

typedef struct decodedData {
  int cf;
  int dbid;
  sds key;
  uint64_t version;
  sds subkey;
  int rdbtype;
  sds rdbraw;  /* not include type in first byte. */
} decodedData;

typedef struct rocksIterDecodeStats {
    long long ok;
    long long err;
} rocksIterDecodeStats;

void decodedResultInit(decodedResult *decoded);
void decodedResultDeinit(decodedResult *decoded);

int rocksIterDecode(rocksIter *it, decodedResult *decoded, rocksIterDecodeStats *stats);
sds rocksIterDecodeStatsDump(rocksIterDecodeStats *stats);

struct rdbKeySaveData;
typedef struct rdbKeySaveType {
  int (*save_start)(struct rdbKeySaveData *keydata, rio *rdb);
  int (*save_hot_ext)(struct rdbKeySaveData *keydata, rio *rdb);
  int (*save)(struct rdbKeySaveData *keydata, rio *rdb, decodedData *decoded);
  int (*save_end)(struct rdbKeySaveData *keydata, rio *rdb, int save_result);
  void (*save_deinit)(struct rdbKeySaveData *keydata);
} rdbKeySaveType;

typedef struct rdbKeySaveData {
  rdbKeySaveType *type;
  objectMetaType *omtype;
  robj *key; /* own */
  robj *value; /* ref: incrRefcount will cause cow */
  objectMeta *object_meta; /* own */
  long long expire;
  int rdbtype; /* target rdb format, only used in bitmap saving. */
  int saved;
  void *iter; /* used by list (metaListIterator), bitmap (bitmapSaveIterator) */
} rdbKeySaveData;

typedef struct rdbSaveRocksStats {
    long long init_save_ok;
    long long init_save_skip;
    long long init_save_err;
    long long save_ok;
} rdbSaveRocksStats;

/* rdb save */
int rdbSaveKeyHeader(rio *rdb, robj *key, robj *o, unsigned char rdbtype, long long expiretime);
int rdbKeySaveHotExtensionInit(rdbKeySaveData *keydata, redisDb *db, sds keystr);
int rdbKeySaveWarmColdInit(rdbKeySaveData *keydata, redisDb *db, decodedResult *dr);
void rdbKeySaveDataDeinit(rdbKeySaveData *keydata);
int rdbKeySaveHotExtension(struct rdbKeySaveData *keydata, rio *rdb);
int rdbKeySaveStart(struct rdbKeySaveData *keydata, rio *rdb);
int rdbKeySave(struct rdbKeySaveData *keydata, rio *rdb, decodedData *d);
int rdbKeySaveEnd(struct rdbKeySaveData *keydata, rio *rdb, int save_result);
void wholeKeySaveInit(rdbKeySaveData *keydata);
int hashSaveInit(rdbKeySaveData *save, uint64_t version, const char *extend, size_t extlen);
int setSaveInit(rdbKeySaveData *save, uint64_t version, const char *extend, size_t extlen);
int listSaveInit(rdbKeySaveData *save, uint64_t version, const char *extend, size_t extlen);
int zsetSaveInit(rdbKeySaveData *save, uint64_t version, const char *extend, size_t extlen);
int bitmapSaveInit(rdbKeySaveData *save, uint64_t version, const char *extend, size_t extlen);

/* Rdb load */
/* RDB_LOAD_ERR_*: [1 +inf), SWAP_ERR_RDB_LOAD_*: (-inf -500] */
#define SWAP_ERR_RDB_LOAD_UNSUPPORTED -500

#define RDB_LOAD_BATCH_COUNT 50
#define RDB_LOAD_BATCH_CAPACITY  (4*1024*1024)

typedef struct swapRdbLoadObjectCtx {
  size_t errors;
  size_t idx;
  struct {
    size_t capacity;
    size_t memory;
    size_t count;
    size_t index;
    int *cfs;
    sds *rawkeys;
    sds *rawvals;
  } batch;
} swapRdbLoadObjectCtx;

void swapStartLoading(void);
void swapStopLoading(int success);

struct rdbKeyLoadData;

typedef struct rdbKeyLoadType {
  void (*load_start)(struct rdbKeyLoadData *keydata, rio *rdb, OUT int *cf, OUT sds *rawkey, OUT sds *rawval, OUT int *error);
  int (*load)(struct rdbKeyLoadData *keydata, rio *rdb, OUT int *cf, OUT sds *rawkey, OUT sds *rawval, OUT int *error);
  int (*load_end)(struct rdbKeyLoadData *keydata,rio *rdb);
  void (*load_deinit)(struct rdbKeyLoadData *keydata);
} rdbKeyLoadType;

typedef struct rdbKeyLoadData {
    rdbKeyLoadType *type;
    objectMetaType *omtype;
    redisDb *db;
    sds key; /* ref */
    long long expire;
    long long now;
    uint64_t version;
    int rdbtype;
    int swap_type;
    int nfeeds;
    int total_fields;
    int loaded_fields;
    robj *value;
    void *iter;
    void *load_info; /* only used in RDB_TYPE_BITMAP load process. */
} rdbKeyLoadData;

static inline sds rdbVerbatimNew(unsigned char rdbtype) {
    return sdsnewlen(&rdbtype,1);
}

int rdbLoadStringVerbatim(rio *rdb, sds *verbatim);
int rdbLoadHashFieldsVerbatim(rio *rdb, unsigned long long len, sds *verbatim);
int swapRdbLoadObject(int rdbtype, rio *rdb, redisDb *db, sds key, long long expire, long long now, rdbKeyLoadData *keydata);
robj *rdbKeyLoadGetObject(rdbKeyLoadData *keydata);
int rdbKeyLoadDataInit(rdbKeyLoadData *keydata, int rdbtype, redisDb *db, sds key, long long expire, long long now);
void rdbKeyLoadDataDeinit(rdbKeyLoadData *keydata);
void rdbKeyLoadDataSetup(rdbKeyLoadData *keydata, int rdbtype, redisDb *db, sds key, long long expire, long long now);
void rdbKeyLoadStart(struct rdbKeyLoadData *keydata, rio *rdb, int *cf, sds *rawkey, sds *rawval, int *error);
int rdbKeyLoad(struct rdbKeyLoadData *keydata, rio *rdb, int *cf, sds *rawkey, sds *rawval, int *error);
int rdbKeyLoadEnd(struct rdbKeyLoadData *keydata, rio *rdb);
int rdbKeyLoadDbAdd(struct rdbKeyLoadData *keydata, redisDb *db);
void rdbKeyLoadExpired(struct rdbKeyLoadData *keydata);

#define rdbLoadStartHT rdbLoadStartLenMeta
#define rdbLoadStartSet rdbLoadStartLenMeta
void rdbLoadStartLenMeta(struct rdbKeyLoadData *load, rio *rdb, int *cf, sds *rawkey, sds *rawval, int *error);

void wholeKeyLoadInit(rdbKeyLoadData *keydata);
void hashLoadInit(rdbKeyLoadData *load);
void setLoadInit(rdbKeyLoadData *load);
void listLoadInit(rdbKeyLoadData *load);

void zsetLoadInit(rdbKeyLoadData *load);
void bitmapLoadInit(rdbKeyLoadData *load);
int rdbLoadLenVerbatim(rio *rdb, sds *verbatim, int *isencoded, unsigned long long *lenptr);
void _rdbSaveBackground(client *c, swapCtx *ctx);

#define SWAP_SAVE_NOP 0
#define SWAP_SAVE_SUCC 1
#define SWAP_SAVE_FAIL -1

typedef struct swapRdbSaveCtx {
    long key_count;
    size_t processed;
    long long info_updated_time;
    list *hot_keys_extension;
    redisDb *rehash_paused_db;
    int rdbflags;
    int rordb;
} swapRdbSaveCtx;

void rdbSaveInfoSetRordb(rdbSaveInfo *rsiptr, int rordb);
int rdbSaveInfoSetSfrctx(rdbSaveInfo *rsiptr, swapForkRocksdbCtx *sfrctx);

void swapRdbSaveCtxInit(swapRdbSaveCtx *ctx, int rdbflags, int rordb);
void swapRdbSaveCtxDeinit(swapRdbSaveCtx *ctx);
int swapRdbSaveStart(rio *rdb, swapRdbSaveCtx *ctx);
void swapRdbSaveBeginDb(rio *rdb, redisDb *db, swapRdbSaveCtx *ctx);
int swapRdbSaveKeyValuePair(rio *rdb, redisDb *db, robj *key, robj *o, swapRdbSaveCtx *ctx);
void swapRdbSaveProgress(rio *rdb, swapRdbSaveCtx *ctx);
int swapRdbSaveDb(rio *rdb, int *error, redisDb *db, swapRdbSaveCtx *ctx);
void swapRdbSaveEndDb(rio *rdb, redisDb *db, swapRdbSaveCtx *ctx);
int swapRdbSaveStop(rio *rdb, swapRdbSaveCtx *ctx);
int swapRdbSaveHotExtension(rio *rdb, int *error, redisDb *db, swapRdbSaveCtx *ctx);
int swapRdbSaveRocks(rio *rdb, int *error, redisDb *db, swapRdbSaveCtx *ctx);

#define SWAP_LOAD_UNRECOGNIZED 0
#define SWAP_LOAD_SUCC 1
#define SWAP_LOAD_FAIL -1

extern robj *SWAP_RDB_LOAD_MOCK_VALUE;

typedef struct swapRdbLoadCtx {
  int reopen_filter;
  int rordb;
  int rordb_sstloaded;
  long long rordb_object_flags;
} swapRdbLoadCtx;

void swapRdbLoadCtxInit(swapRdbLoadCtx *ctx);
void swapRdbLoadBegin(swapRdbLoadCtx *ctx);
void swapRdbLoadEnd(swapRdbLoadCtx *ctx, rio *rdb, int eoferr);
int swapRdbLoadAuxField(swapRdbLoadCtx *ctx, rio *rdb, robj *auxkey, robj *auxval);
int swapRdbLoadNoKV(swapRdbLoadCtx *ctx, rio *rdb, redisDb *db, int type);
int swapRdbLoadKVBegin(swapRdbLoadCtx *ctx, rio *rdb);
robj *swapRdbLoadKVLoadValue(swapRdbLoadCtx *ctx, rio *rdb, int *perror, redisDb *db, int type, sds key, long long expiretime, long long now);
int swapRdbLoadKVLoaded(swapRdbLoadCtx *ctx, redisDb *db, sds key, robj *val);
void swapRdbLoadKVEnd(swapRdbLoadCtx *ctx);
static inline int swapRdbLoadDontExpireKeyOnLoad(swapRdbLoadCtx *ctx) {
  return ctx->rordb;
}
int rdbLoadDoubleValue(rio *rdb, double *val);

/* Note that rdbLoadObjectGetSkipEmptyCheck is not thread safe.
 * In our case, skip empty check flag is set only by main thread,
 * and only matters when read in main thread, so it's safe for our use case.
 * Although swap thread may skip empty check when not suppose to, but it
 * will not harm anyway. */
extern int RDB_LOAD_OBJECT_SKIP_EMPTY_CHECK;

static inline void rdbLoadObjectSetSkipEmptyCheckFlag(int skip) {
    RDB_LOAD_OBJECT_SKIP_EMPTY_CHECK = skip;
}
static inline int rdbLoadObjectGetSkipEmptyCheckFlag(void) {
    return RDB_LOAD_OBJECT_SKIP_EMPTY_CHECK;
}

/* persist load fix */
typedef struct keyLoadFixData {
  redisDb *db;
  int swap_type;
  robj *key;
  long long expire;
  uint64_t version;
  objectMeta *cold_meta;
  objectMeta *rebuild_meta;
  long long feed_err;
  long long feed_ok;
  sds errstr;
} keyLoadFixData;

typedef struct loadFixStats {
  long long init_ok;
  long long init_skip;
  long long init_err;
  long long fix_none;
  long long fix_update;
  long long fix_delete;
  long long fix_err;
} loadFixStats;

sds loadFixStatsDump(loadFixStats *stats);

/* absent cache */
typedef struct absentKeyMapEntry {
  dict *subkeys;
  listNode *ln;
} absentKeyMapEntry;

typedef struct absentListEntry {
  sds key; /* ref */
  sds subkey; /* ref */
} absentListEntry;

typedef struct absentCache {
  size_t capacity;
  dict *map;
  list *list;
} absentCache;

absentCache *absentCacheNew(size_t capacity);
void absentCacheFree(absentCache *absent);
int absentCacheDelete(absentCache *absent, sds key);
int absentCachePutKey(absentCache *absent, sds key);
int absentCachePutSubkey(absentCache *absent, sds key, sds subkey);
int absentCacheGetKey(absentCache *absent, sds key);
int absentCacheGetSubkey(absentCache *absent, sds key, sds subkey);
void absentCacheSetCapacity(absentCache *absent, size_t capacity);


/* cold keys filter */
#define COLDFILTER_FILT_BY_CUCKOO_FILTER 1
#define COLDFILTER_FILT_BY_ABSENT_CACHE 2

typedef struct swapCuckooFilterStat {
  long long lookup_count;
  long long false_positive_count;
} swapCuckooFilterStat;

void swapCuckooFilterStatInit(swapCuckooFilterStat *stat);
void swapCuckooFilterStatDeinit(swapCuckooFilterStat *stat);
void trackSwapCuckooFilterInstantaneousMetrics(void);
void resetSwapCukooFilterInstantaneousMetrics(void);
sds genSwapCuckooFilterInfoString(sds info);

typedef struct coldFilter {
  absentCache *absents;
  cuckooFilter *filter;
  swapCuckooFilterStat filter_stat;
} coldFilter;

coldFilter *coldFilterCreate(void);
void coldFilterDestroy(coldFilter *filter);
void coldFilterInit(coldFilter *filter);
void coldFilterDeinit(coldFilter *filter);
void coldFilterReset(coldFilter *filter);
void coldFilterAddKey(coldFilter *filter, sds key);
void coldFilterDeleteKey(coldFilter *filter, sds key);
void coldFilterKeyNotFound(coldFilter *filter, sds key);
int coldFilterMayContainKey(coldFilter *filter, sds key, int *filt_by);

void coldFilterSubkeyAdded(coldFilter *filter, sds key);
void coldFilterSubkeyNotFound(coldFilter *filter, sds key, sds subkey);
int coldFilterMayContainSubkey(coldFilter *filter, sds key, sds subkey);

/* Util */

#define ROCKS_KEY_FLAG_NONE 0x0
#define ROCKS_KEY_FLAG_SUBKEY 0x1
#define ROCKS_KEY_FLAG_DELETE 0xff

sds encodeMetaKey(int dbid, const char* key, size_t key_len);
sds rocksEncodeMetaKey(redisDb *db, sds key);
int rocksDecodeMetaKey(const char *raw, size_t rawlen, int *dbid, const char **key, size_t *keylen);
sds rocksEncodeDataKey(redisDb *db, sds key, uint64_t version, sds subkey);
sds rocksEncodeDataRangeStartKey(redisDb *db, sds key, uint64_t version);
sds rocksEncodeDataRangeEndKey(redisDb *db, sds key, uint64_t version);
#define rocksEncodeDataScanPrefix(db,key,version) rocksEncodeDataRangeStartKey(db,key,version)
int rocksDecodeDataKey(const char *raw, size_t rawlen, int *dbid, const char **key, size_t *keylen, uint64_t *version, const char **subkey, size_t *subkeylen);
sds rocksEncodeMetaVal(int swap_type, long long expire, uint64_t version, sds extend);
int rocksDecodeMetaVal(const char* raw, size_t rawlen, int *swap_type, long long *expire, uint64_t *version, const char **extend, size_t *extend_len);
sds rocksEncodeValRdb(robj *value);
robj *rocksDecodeValRdb(sds raw);
sds rocksEncodeObjectMetaLen(unsigned long len);
long rocksDecodeObjectMetaLen(const char *raw, size_t rawlen);
sds encodeMetaScanKey(unsigned long cursor, int limit, sds seek);
int decodeMetaScanKey(sds meta_scan_key, unsigned long *cursor, int *limit, const char **seek, size_t *seeklen);
sds rocksEncodeDbRangeStartKey(int dbid);
sds rocksEncodeDbRangeEndKey(int dbid);

#define sizeOfDouble (BYTE_ORDER == BIG_ENDIAN? sizeof(double):8)
int encodeDouble(char* buf, double value);
int decodeDouble(const char* val, double* score);
int decodeScoreKey(const char* raw, int rawlen, int* dbid, const char** key, size_t* keylen, uint64_t *version, double* score, const char** subkey, size_t* subkeylen);
sds encodeScoreKey(redisDb* db ,sds key, uint64_t version, double score, sds subkey);
sds encodeIntervalSds(int ex, MOVE IN sds data);
int decodeIntervalSds(sds data, int* ex, char** raw, size_t* rawlen);
sds encodeScoreRangeStart(redisDb* db, sds key, uint64_t version);
sds encodeScoreRangeEnd(redisDb* db, sds key, uint64_t version);

extern struct swapSharedObjectsStruct swap_shared;
struct swapSharedObjectsStruct {
    robj *outofdiskerr, *rocksdbdiskerr, *emptystring, *swap_info, *sst_age_limit;
};
void createSwapSharedObjects(void);
/* accept ignore */
void ctrip_ignoreAcceptEvent();
void ctrip_resetAcceptIgnore();
void acceptMonitorHandler(aeEventLoop *el, int fd, void *privdata, int mask);

robj *unshareStringValue(robj *value);
size_t objectEstimateSize(robj *o);
size_t keyEstimateSize(redisDb *db, robj *key);
void swapCommand(client *c);
void swapDebugCommand(client *c);
void swapExpiredCommand(client *c);
const char *strObjectType(int type);
int timestampIsExpired(mstime_t expire);
size_t swap_dbSize(redisDb *db);
long get_dir_size(char *dirname);
robj *swapRandomKey(redisDb *db, metaScanResult *metas);
void dbPauseRehash(redisDb *db);
void dbResumeRehash(redisDb *db);
int debugGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
void commandProcessed(client *c);
ssize_t rdbWriteRaw(rio *rdb, void *p, size_t len);
void swapInitServerConfig(void);
void swapInitServer(void);
void freeClientsInDeferedQueue(void);
void swap_replicationStartPendingFork(void);
void debugSwapOutCommand(client *c);
size_t swapobjectComputeSize(robj *val, int samples, objectMeta *object_meta);

void notifyKeyspaceEventDirty(int type, char *event, robj *key, int dbid, ...);
void notifyKeyspaceEventDirtyKey(int type, char *event, robj *key, int dbid);
void notifyKeyspaceEventDirtyMeta(int type, char *event, robj *key, int dbid, robj *o);
void notifyKeyspaceEventDirtySubkeys(int type, char *event, robj *key, int dbid, robj *o, int count, sds *subkeys, size_t *sublens);

robj *dirtySubkeysNew(void);
void dirtySubkeysFree(robj *dss);
unsigned long dirtySubkeysLength(robj *dss);
int dirtySubkeysAdd(robj *dss, sds subkey, size_t sublen);
int dirtySubkeysRemove(robj *dss, sds subkey);

typedef struct dirtySubkeysIterator {
  hashTypeIterator *iter;
}dirtySubkeysIterator;

void dirtySubkeysIteratorInit(dirtySubkeysIterator *it, robj *dss);
void dirtySubkeysIteratorDeinit(dirtySubkeysIterator *it);
robj* dirtySubkeysIteratorNext(dirtySubkeysIterator *it, size_t *vlen);

extern dictType dbDirtySubkeysDictType;

void dbAddDirtySubkeys(redisDb *db, robj* key, robj *dss);
int dbDeleteDirtySubkeys(redisDb *db, robj* key);
robj *lookupDirtySubkeys(redisDb *db, robj* key);


uint64_t SwapCommandDataTypeFlagByName(const char *name);

static inline void clientSwapError(client *c, int swap_errcode) {
  if (c && swap_errcode) {
    if (swap_errcode != SWAP_ERR_DATA_WRONG_TYPE_ERROR &&
        swap_errcode != SWAP_ERR_METASCAN_UNSUPPORTED_IN_MULTI) {
      atomicIncr(server.swap_error_count,1);
    }
    c->swap_errcode = swap_errcode;
  }
}

#define ROCKSDB_COMPACT_RANGE_TASK 0
#define ROCKSDB_GET_STATS_TASK 1
#define ROCKSDB_FLUSH_TASK 2
#define ROCKSDB_EXCLUSIVE_TASK_COUNT 3
#define ROCKSDB_CREATE_CHECKPOINT 3
#define ROCKSDB_COLLECT_CF_META_TASK 4

typedef void (*rocksdbUtilTaskCallback)(void *result, void *pd, int errcode);

typedef struct rocksdbUtilTaskManager{
    struct {
        int stat;
    } stats[ROCKSDB_EXCLUSIVE_TASK_COUNT];
} rocksdbUtilTaskManager;

rocksdbUtilTaskManager* createRocksdbUtilTaskManager(void);

typedef struct rocksdbUtilTaskCtx {
    int type;
    void *argument; /* input argument for util task (ref, owned by caller)*/
    void *result;   /* output result for util task(moved to finish_cb when finished) */
    rocksdbUtilTaskCallback finish_cb;
    void *finish_pd;
} rocksdbUtilTaskCtx;

int submitUtilTask(int type, void *arg, rocksdbUtilTaskCallback cb, void* pd, sds* error);

/* swap trace */
#define SLOWLOG_ENTRY_MAX_TRACE 16
void swapSlowlogCommand(client *c);

struct swapTrace {
    int intention;
    monotime swap_lock_time;
    monotime swap_dispatch_time;
    monotime swap_process_time;
    monotime swap_notify_time;
    monotime swap_callback_time;
};

struct swapCmdTrace {
    int swap_cnt;
    int finished_swap_cnt;
    monotime swap_submitted_time;
    monotime swap_finished_time;
    swapTrace *swap_traces;
};

swapCmdTrace *createSwapCmdTrace(void);
void initSwapTraces(swapCmdTrace *swap_cmd, int swap_cnt);
void swapCmdTraceFree(swapCmdTrace *trace);
void swapCmdSwapSubmitted(swapCmdTrace *swap_cmd);
void swapTraceLock(swapTrace *trace);
void swapTraceDispatch(swapTrace *trace);
void swapTraceProcess(swapTrace *trace);
void swapTraceNotify(swapTrace *trace, int intention);
void swapTraceCallback(swapTrace *trace);
void swapCmdSwapFinished(swapCmdTrace *swap_cmd);
void attachSwapTracesToSlowlog(void *ptr, swapCmdTrace *swap_cmd);

/* swap block*/
typedef struct swapUnblockCtx  {
  long long version;
  client** mock_clients;
  /* status */
  long long swap_total_count;
  long long swapping_count;
  long long swap_retry_count;
  long long swap_err_count;
} swapUnblockCtx ;

swapUnblockCtx* createSwapUnblockCtx(void);
void releaseSwapUnblockCtx(swapUnblockCtx* block);
void swapServeClientsBlockedOnListKey(robj *o, readyList *rl);
int getKeyRequestsSwapBlockedLmove(int dbid, int intention, int intention_flags, uint64_t cmd_flags,
            robj *key, struct getKeyRequestsResult *result, int arg_rewrite0,
            int arg_rewrite1, int num_ranges, ...);
int serveClientBlockedOnList(client *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int wherefrom, int whereto,list* swap_wrong_type_error_keys);
void incrSwapUnBlockCtxVersion();
void incrSwapUnBlockCtxVersion(void);

#ifndef __APPLE__
typedef struct swapThreadCpuUsage{
    /* CPU usage Cacluation */
    double main_thread_cpu_usage;
    double swap_threads_cpu_usage;
    double other_threads_cpu_usage;

    double main_thread_ticks_save;
    double *swap_thread_ticks_save;
    double process_cpu_ticks_save;

    int main_tid[1];
    int *swap_tids;

    pid_t pid;
    double hertz;
    double uptime_save;
}swapThreadCpuUsage;

void swapThreadCpuUsageUpdate(swapThreadCpuUsage *cpu_usage);
void swapThreadCpuUsageFree(swapThreadCpuUsage *cpu_usage);
struct swapThreadCpuUsage *swapThreadCpuUsageNew(void);
sds genRedisThreadCpuUsageInfoString(sds info, swapThreadCpuUsage *cpu_usage);
#endif

#ifdef REDIS_TEST
int initTestRedisDb(void);
int initTestRedisServer(void);
void initServerConfig4Test(void);
int clearTestRedisDb(void);
int clearTestRedisServer(void);
swapData *createWholeKeySwapDataWithExpire(redisDb *db, robj *key, robj *value, long long expire, void **datactx);
swapData *createWholeKeySwapData(redisDb *db, robj *key, robj *value, void **datactx);

int swapDataWholeKeyTest(int argc, char **argv, int accurate);
int swapDataHashTest(int argc, char **argv, int accurate);
robj **mockSubKeys(int num,...);
int swapDataSetTest(int argc, char **argv, int accurate);
int swapDataZsetTest(int argc, char **argv, int accurate);
int swapDataTest(int argc, char *argv[], int accurate);
int swapLockTest(int argc, char **argv, int accurate);
int swapLockReentrantTest(int argc, char **argv, int accurate);
int swapLockProceedTest(int argc, char **argv, int accurate);
int swapCmdTest(int argc, char **argv, int accurate);
int swapExecTest(int argc, char **argv, int accurate);
int swapRdbTest(int argc, char **argv, int accurate);
int swapObjectTest(int argc, char *argv[], int accurate);
int swapIterTest(int argc, char *argv[], int accurate);
int metaScanTest(int argc, char *argv[], int accurate);
int swapExpireTest(int argc, char *argv[], int accurate);
int swapUtilTest(int argc, char **argv, int accurate);
int swapFilterTest(int argc, char **argv, int accurate);
int swapListMetaTest(int argc, char *argv[], int accurate);
int swapListDataTest(int argc, char *argv[], int accurate);
int swapListUtilsTest(int argc, char *argv[], int accurate);
int swapAbsentTest(int argc, char *argv[], int accurate);
int lruCacheTest(int argc, char *argv[], int accurate);
int swapRIOTest(int argc, char *argv[], int accurate);
int swapBatchTest(int argc, char *argv[], int accurate);
int cuckooFilterTest(int argc, char *argv[], int accurate);
int swapPersistTest(int argc, char *argv[], int accurate);
int swapRordbTest(int argc, char *argv[], int accurate);
int roaringBitmapTest(int argc, char *argv[], int accurate);
int swapDataBitmapTest(int argc, char **argv, int accurate);
int wtdigestTest(int argc, char **argv, int accurate);
int swapReplTest(int argc, char **argv, int accurate);

int swapTest(int argc, char **argv, int accurate);

#endif

#endif
