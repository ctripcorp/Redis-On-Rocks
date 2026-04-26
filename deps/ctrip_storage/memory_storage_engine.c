/* memory_storage_engine.c - 内存存储引擎实现
 *
 * 基于跳表实现与 RocksDB 接口兼容的内存存储引擎。
 * Key 按字节序排列，每个 CF（Column Family）使用独立跳表隔离数据。
 * rawkey 直接存储（无前缀），跳表内按字节序排序。
 */

#include <stdlib.h>
#include <string.h>
/* 先 include server.h + rio，确保完整 RIO 类型在 StorageEngine 函数指针之前可见 */
#include "server.h"
#include "ctrip_storage_rio.h"
#include "ctrip_storage_utils.h"
#include "ctrip_storage.h"
#include "memory_storage_engine.h"
#include "ctrip_storage_objects.h"
#include "ctrip_storage_filter.h"
#include "ctrip_storage_request_utils.h"

/* ==================== 跳表实现 ==================== */

/* 生成随机层数：以 MSE_SKIPLIST_P 概率升层，上限 MAXLEVEL */
static int mseSkipListRandomLevel() {
    int level = 1;
    while ((random() & 0xFFFF) < (MSE_SKIPLIST_P * 0xFFFF))
        level++;
    return (level < MSE_SKIPLIST_MAXLEVEL) ? level : MSE_SKIPLIST_MAXLEVEL;
}

/* 创建跳表节点
 * 输入：level-层数, key-编码key(转移所有权), val-值(转移所有权，可为NULL)
 * 输出：新节点指针 */
static mseNode* mseNodeCreate(int level, sds key, sds val) {
    mseNode *node = zmalloc(sizeof(mseNode) + level * sizeof(mseNode*));
    node->key = key;
    node->val = val;
    node->forward = (mseNode**)(node + 1);
    for (int i = 0; i < level; i++) node->forward[i] = NULL;
    return node;
}

/* 释放节点（同时释放 key/val 的 sds 内存） */
static void mseNodeFree(mseNode *node) {
    if (node->key) sdsfree(node->key);
    if (node->val) sdsfree(node->val);
    zfree(node);
}

/* 创建跳表 */
static mseSkipList* mseSkipListCreate() {
    mseSkipList *sl = zmalloc(sizeof(mseSkipList));
    sl->level = 1;
    sl->length = 0;
    /* 哨兵头节点，key/val 为 NULL */
    sl->header = mseNodeCreate(MSE_SKIPLIST_MAXLEVEL, NULL, NULL);
    return sl;
}

/* 销毁跳表，释放所有节点 */
static void mseSkipListFree(mseSkipList *sl) {
    mseNode *cur = sl->header->forward[0];
    /* 释放哨兵头节点（key/val 为 NULL，不会 double free） */
    zfree(sl->header);
    while (cur) {
        mseNode *next = cur->forward[0];
        mseNodeFree(cur);
        cur = next;
    }
    zfree(sl);
}

/* 比较两个 sds key 的字节序大小
 * 输出：<0 / 0 / >0 */
static int mseKeyCmp(const sds a, const sds b) {
    size_t la = sdslen(a), lb = sdslen(b);
    size_t minlen = la < lb ? la : lb;
    int cmp = memcmp(a, b, minlen);
    if (cmp != 0) return cmp;
    if (la < lb) return -1;
    if (la > lb) return 1;
    return 0;
}

/* 在跳表中查找 key，返回节点指针，找不到返回 NULL
 * 输入：sl-跳表, key-已编码的 key */
static mseNode* mseSkipListFind(mseSkipList *sl, const sds key) {
    mseNode *cur = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (cur->forward[i] && mseKeyCmp(cur->forward[i]->key, key) < 0)
            cur = cur->forward[i];
    }
    mseNode *candidate = cur->forward[0];
    if (candidate && mseKeyCmp(candidate->key, key) == 0)
        return candidate;
    return NULL;
}

/* 在跳表中插入或更新 key-val
 * 若 key 已存在则更新 val；否则插入新节点。
 * 输入：sl-跳表, key-已编码key(转移所有权), val-值(转移所有权) */
static void mseSkipListInsert(mseSkipList *sl, sds key, sds val) {
    /* update[i] 记录每层需要更新的前驱节点 */
    mseNode *update[MSE_SKIPLIST_MAXLEVEL];
    mseNode *cur = sl->header;

    for (int i = sl->level - 1; i >= 0; i--) {
        while (cur->forward[i] && mseKeyCmp(cur->forward[i]->key, key) < 0)
            cur = cur->forward[i];
        update[i] = cur;
    }

    mseNode *existing = cur->forward[0];
    if (existing && mseKeyCmp(existing->key, key) == 0) {
        /* key 已存在：更新 val，释放旧的 key（传入的 key 不再需要） */
        sdsfree(key);
        if (existing->val) sdsfree(existing->val);
        existing->val = val;
        return;
    }

    /* 新节点 */
    int level = mseSkipListRandomLevel();
    if (level > sl->level) {
        /* 新层的前驱为哨兵头节点 */
        for (int i = sl->level; i < level; i++)
            update[i] = sl->header;
        sl->level = level;
    }

    mseNode *node = mseNodeCreate(level, key, val);
    for (int i = 0; i < level; i++) {
        node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = node;
    }
    sl->length++;
}

/* 在跳表中删除 key
 * 输入：sl-跳表, key-已编码key
 * 输出：1 表示删除成功，0 表示 key 不存在 */
static int mseSkipListDelete(mseSkipList *sl, const sds key) {
    mseNode *update[MSE_SKIPLIST_MAXLEVEL];
    mseNode *cur = sl->header;

    for (int i = sl->level - 1; i >= 0; i--) {
        while (cur->forward[i] && mseKeyCmp(cur->forward[i]->key, key) < 0)
            cur = cur->forward[i];
        update[i] = cur;
    }

    mseNode *target = cur->forward[0];
    if (!target || mseKeyCmp(target->key, key) != 0)
        return 0; /* 不存在 */

    /* 从每层摘除 target */
    for (int i = 0; i < sl->level; i++) {
        if (update[i]->forward[i] != target) break;
        update[i]->forward[i] = target->forward[i];
    }

    /* 收缩层数 */
    while (sl->level > 1 && sl->header->forward[sl->level - 1] == NULL)
        sl->level--;

    mseNodeFree(target);
    sl->length--;
    return 1;
}

/* 找到第一个 key >= seek 的节点
 * 输入：sl-跳表, seek-已编码的起始 key（可为 NULL 表示从头开始）
 * 输出：节点指针，若无则返回 NULL */
static mseNode* mseSkipListSeekGE(mseSkipList *sl, const sds seek) {
    if (seek == NULL) return sl->header->forward[0];

    mseNode *cur = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (cur->forward[i] && mseKeyCmp(cur->forward[i]->key, seek) < 0)
            cur = cur->forward[i];
    }
    return cur->forward[0];
}

/* ==================== StorageEngine 回调实现 ==================== */

/* put 操作：将 numkeys 个 (cf, rawkey, rawval) 写入跳表
 * RIO.put.cfs[i], rawkeys[i], rawvals[i] → 插入跳表
 */
int mseEnginePut(void *context, struct RIO *rio) {
    memoryStorageEngineCtx *engine = (memoryStorageEngineCtx*)context;
    int numkeys = rio->put.numkeys;

    for (int i = 0; i < numkeys; i++) {
        int cf = rio->put.cfs[i];
        sds rawkey = rio->put.rawkeys[i];
        sds rawval = rio->put.rawvals[i];

        mseSkipListInsert(engine->sl[cf], sdsdup(rawkey), sdsdup(rawval));
    }
    return 0;
}

/* get 操作：按 (cf, rawkey) 查找值，结果写回 rawvals[i]
 * 若 key 不存在则设置 rio->get.notfound = 1
 * 注意：rawvals 由调用方分配数组，每个 rawvals[i] 的 sds 由本函数分配并返回
 */
int mseEngineGet(void *context, struct RIO *rio) {
    serverLog(LL_WARNING, "mseEngineGet action=%d start", rio->action);
    memoryStorageEngineCtx *engine = (memoryStorageEngineCtx*)context;
    int numkeys = rio->get.numkeys;

    rio->get.notfound = 0;
    if(rio->get.rawvals == NULL) rio->get.rawvals = zmalloc(rio->get.numkeys*sizeof(sds));
    for (int i = 0; i < numkeys; i++) {
        int cf = rio->get.cfs[i];
        sds rawkey = rio->get.rawkeys[i];
        serverLog(LL_WARNING, "mseEngineGetCF cf=%d start", cf);
        mseNode *node = mseSkipListFind(engine->sl[cf], rawkey);
        serverLog(LL_WARNING, "mseEngineGetCF cf=%d end", cf);

        if (node && node->val) {
            /* 返回值的副本，调用方负责释放 */
            rio->get.rawvals[i] = sdsdup(node->val);
        } else {
            rio->get.rawvals[i] = NULL;
            rio->get.notfound++;
        }
    }
    serverLog(LL_WARNING, "mseEngineGet action=%d end", rio->action);
    return 0;
}

/* del 操作：删除 (cf, rawkey) 对应的条目
 * 不存在的 key 静默忽略（与 RocksDB 行为一致）
 */
int mseEngineDel(void *context, struct RIO *rio) {
    memoryStorageEngineCtx *engine = (memoryStorageEngineCtx*)context;
    int numkeys = rio->del.numkeys;

    for (int i = 0; i < numkeys; i++) {
        int cf = rio->del.cfs[i];
        sds rawkey = rio->del.rawkeys[i];

        mseSkipListDelete(engine->sl[cf], rawkey);
    }
    return 0;
}

/* iterate 操作：范围扫描 [start, end)，最多 limit 条，结果存入 rawkeys/rawvals
 * rio->iterate.start  - 起始 key（含），NULL 表示从头
 * rio->iterate.end    - 结束 key（不含），NULL 表示到尾
 * rio->iterate.limit  - 最多返回条数，0 表示不限
 * rio->iterate.numkeys - 输出：实际返回条数
 * rio->iterate.nextseek - 输出：下一次迭代的起始 key（若还有更多数据）
 *
 * 注意：rawkeys/rawvals 数组由本函数分配，调用方负责释放
 */
int mseEngineIterate(void *context, struct RIO *rio) {
    memoryStorageEngineCtx *engine = (memoryStorageEngineCtx*)context;
    int cf = rio->iterate.cf;
    sds start = rio->iterate.start;
    sds end   = rio->iterate.end;
    size_t limit = rio->iterate.limit;

    /* 预分配结果数组：limit 为 0 时先分配 64 槽，按需扩展 */
    size_t capacity = (limit > 0) ? limit : 64;
    sds *rawkeys = zmalloc(capacity * sizeof(sds));
    sds *rawvals = zmalloc(capacity * sizeof(sds));
    size_t count = 0;

    /* 每个 CF 独立跳表，rawkey 直接存储（无前缀），直接用 start 查找 */
    mseNode *cur = mseSkipListSeekGE(engine->sl[cf], start);

    while (cur != NULL) {
        sds nodekey = cur->key;

        /* 超过 end 则停止 */
        if (end && mseKeyCmp(nodekey, end) >= 0) break;

        /* 达到 limit 则记录 nextseek（下一个未返回节点的 rawkey）并停止 */
        if (limit > 0 && count >= limit) {
            rio->iterate.nextseek = sdsdup(nodekey);
            break;
        }

        /* 动态扩展（仅在 limit == 0 时可能触发） */
        if (count >= capacity) {
            capacity *= 2;
            rawkeys = zrealloc(rawkeys, capacity * sizeof(sds));
            rawvals = zrealloc(rawvals, capacity * sizeof(sds));
        }

        rawkeys[count] = sdsdup(nodekey);
        rawvals[count] = cur->val ? sdsdup(cur->val) : NULL;
        count++;

        cur = cur->forward[0];
    }

    rio->iterate.rawkeys = rawkeys;
    rio->iterate.rawvals = rawvals;
    rio->iterate.numkeys = (int)count;

    return 0;
}

/* ==================== 引擎初始化 ==================== */
memoryStorageEngineCtx* createMemoryStorageEngineCtx() {
    serverLog(LL_WARNING, "Initializing memoryStorageEngineCtx...");
    memoryStorageEngineCtx* engine = zmalloc(sizeof(memoryStorageEngineCtx));
    engine->sl = zmalloc(sizeof(mseSkipList*) * CF_COUNT);
    for (size_t i = 0; i < CF_COUNT; i++)
    {
        engine->sl[i] = mseSkipListCreate();
    }
    return engine;
}

/* 销毁内存存储引擎上下文 */
static void freeMemoryStorageEngineCtx(memoryStorageEngineCtx *engine) {
    if (engine == NULL) return;
    for (size_t i = 0; i < CF_COUNT; i++) {
        mseSkipListFree(engine->sl[i]);
    }
    zfree(engine->sl);
    zfree(engine);
}

int mseEngineBatchGet(void *context, struct RIOBatch *rios) {
    serverLog(LL_WARNING, "mseEngineBatchGet action=%d start", rios->action);
    /* batch get 暂未实现，返回错误 */
    for (size_t i = 0; i < rios->count; i++) {
        RIO *rio = rios->rios + i;
        mseEngineGet(context, rio);
    }
    serverLog(LL_WARNING, "mseEngineBatchGet action=%d end", rios->action);
    return -1;
}

int mseEngineBatchPut(void *context, struct RIOBatch *rios) {
    serverLog(LL_WARNING, "mseEngineBatchPut action=%d start", rios->action);
    /* batch put 暂未实现，返回错误 */
    for (size_t i = 0; i < rios->count; i++) {
        RIO *rio = rios->rios + i;
        mseEnginePut(context, rio);
    }
    serverLog(LL_WARNING, "mseEngineBatchPut action=%d end", rios->action);
    return -1;
}

int mseEngineBatchDEL(void *context, struct RIOBatch *rios) {
    serverLog(LL_WARNING, "mseEngineBatchDEL action=%d start", rios->action);
    /* batch del 暂未实现，返回错误 */
    for (size_t i = 0; i < rios->count; i++) {
        RIO *rio = rios->rios + i;
        mseEngineDel(context, rio);
    }
    serverLog(LL_WARNING, "mseEngineBatchDEL action=%d end", rios->action);
    return -1;
}

int msfInit(StorageForkCtx* ctx) {
    return 0;
}

int msfDeinit(StorageForkCtx* ctx) {
    return 0;
}

int msfBeforeFork(StorageForkCtx* ctx) {
    return 0;
}
int msfAfterForkChild(StorageForkCtx* ctx) {
    return 0;
}
int msfAfterForkParent(StorageForkCtx* ctx, int childpid) {
    UNUSED(childpid);
    return 0;
}

StorageForkCtxType memory_storage_fork_type = {
    .init = msfInit,
    .deinit = msfDeinit,
    .beforeFork = msfBeforeFork,
    .afterForkChild = msfAfterForkChild,
    .afterForkParent = msfAfterForkParent,
};
int mseSetForkCtxType(struct StorageForkCtx *ctx, struct rdbSaveInfo *rsiptr, int mincapa) {
    ctx->type = &memory_storage_fork_type;
    return 1;
}

typedef struct MemoryStorageRdbSaveCtx {
    int rdbflags;
    long key_count;
    rio* rdb;
    list* hot_keys_extension;
    redisDb* rehash_paused_db;
} MemoryStorageRdbSaveCtx;
int freeMemoryStorageSaveRdbCtx(RdbSaveCtx* ctx, struct rio* rdb) {
    zfree(ctx->ctx);
}

void dbPauseRehash(redisDb *db) {
    //LATTE_TO_DO
    // dictPauseRehashing(db->dict);
    // dictPauseRehashing(db->expires);
    dictPauseRehashing(db->storage.meta);
    dictPauseRehashing(db->storage.dirty_subkeys);
}


int memoryStorageSaveDbInit(RdbSaveCtx* ctx, struct rio* rdb, struct redisDb* db) {
    MemoryStorageRdbSaveCtx* memory_ctx = ctx->ctx;
    serverAssert(memory_ctx->rdb == rdb);
    memory_ctx->hot_keys_extension = listCreate();
    memory_ctx->rehash_paused_db = db;
    dbPauseRehash(db);
    return 1;
}

void dbResumeRehash(redisDb *db) {
    //LATTE_TO_DO
    // dictResumeRehashing(db->dict);
    // dictResumeRehashing(db->expires);
    dictResumeRehashing(db->storage.meta);
    dictResumeRehashing(db->storage.dirty_subkeys);
}


int memoryStorageSaveDbDeInit(RdbSaveCtx* ctx,struct rio* rdb, redisDb* db) {
    MemoryStorageRdbSaveCtx* memory_ctx = ctx->ctx;
    serverAssert(memory_ctx->rdb == rdb);
    serverAssert(memory_ctx->rehash_paused_db == db);
    dbResumeRehash(db);
    memory_ctx->rehash_paused_db = NULL;
    serverAssert(memory_ctx->hot_keys_extension != NULL);
    listRelease(memory_ctx->hot_keys_extension);
    memory_ctx->hot_keys_extension = NULL;
    return 1;
}





int storageShouldSaveByMemory(objectMeta* meta, robj* o) {
    return !keyIsHot(meta, o) || (meta != NULL && meta->swap_type == SWAP_TYPE_BITMAP);
}

int storageShouldByMemoryAsHotExtention(objectMeta *meta, robj *o) {
    return meta != NULL && meta->swap_type == SWAP_TYPE_BITMAP && keyIsHot(meta, o);
}

SAVE_RESULT_ENUM memoryStorageSaveHotKey(RdbSaveCtx* ctx, struct rio* rdb, redisDb* db, robj* key, robj* val) {
    MemoryStorageRdbSaveCtx* memory_ctx = ctx->ctx;
    objectMeta* meta = lookupMeta(db, key);
    if (storageShouldSaveByMemory(meta, val)) {
        if (storageShouldByMemoryAsHotExtention(meta,val))
            listAddNodeTail(memory_ctx->hot_keys_extension, key->ptr);
        return SAVE_SUCC;
    } else {
        return SAVE_NOP;
    }
}


typedef struct decodedData {
  int cf;
  int dbid;
  sds key;
  uint64_t version;
  sds subkey;
  int rdbtype;
  sds rdbraw;  /* not include type in first byte. */
} decodedData;
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
  redisDb* db;    /* hash hsashTypeExists used ,UNINIT*/
} rdbKeySaveData;

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


int rdbKeySaveDataInitHot(rdbKeySaveData *save, redisDb *db, robj *key, robj *value) {
    
    objectMeta *object_meta = lookupMeta(db,key);
    serverAssert(object_meta->swap_type == SWAP_TYPE_BITMAP);

    long long expire = getExpire(db,key->ptr,NULL);

    rdbKeySaveDataInitCommon(save,key,value,expire,object_meta);
    //TODO
    // serverAssert(bitmapSaveInit(save,SWAP_VERSION_ZERO,NULL,0) == 0);
    return INIT_SAVE_OK;
}

int rdbKeySaveHotExtensionInit(rdbKeySaveData *save, redisDb *db, sds keystr) {

    robj *value, *key;
    key = createStringObject(keystr, sdslen(keystr));
    value = lookupKeyReadWithFlags(db,key,LOOKUP_NOTOUCH);

    objectMeta *object_meta = lookupMeta(db,key);

    serverAssert(keyIsHot(object_meta,value));

    return rdbKeySaveDataInitHot(save,db,key,value);
}

int rdbKeySaveHotExtension(struct rdbKeySaveData *save, rio *rdb) {
    if (save->type->save_hot_ext)
        return save->type->save_hot_ext(save,rdb);
    else
        return 0;
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

int stroageRdbSaveHotExtension(rio* rdb, redisDb* db, MemoryStorageRdbSaveCtx* ctx) {
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

        // if (server.swap_debug_rdb_key_save_delay_micro)
        //     debugDelay(server.swap_debug_rdb_key_save_delay_micro);

        save_ok++;

        rdbKeySaveDataDeinit(save);
        // swapRdbSaveProgress(rdb,ctx);
    }
    serverLog(LL_NOTICE,
            "Save hot key extension to rdb finished: save.ok=%lld",save_ok);
    return C_OK;
err:
    // if (error && *error == 0) *error = errno;
    serverLog(LL_WARNING, "Save hot key extension to rdb failed: %s", errstr);

    if (errstr) sdsfree(errstr);
    return C_ERR;
}

int storageRdbSaveMemory(rio* rdb, redisDb* db, MemoryStorageRdbSaveCtx* ctx) {
    sds errstr = NULL;
    int recoverable_err = 0;
    StorageEngine *engine = server.storage.engine;
    memoryStorageEngineCtx *mse_ctx = (memoryStorageEngineCtx*)engine->context;

    /* Iterate DATA_CF to get all key-value pairs stored in memory storage */
    mseSkipList *sl = mse_ctx->sl[DATA_CF];
    mseNode *cur = sl->header->forward[0];

    while (cur != NULL) {
        sds rawkey = cur->key;
        sds rawval = cur->val;

        /* Decode rawkey to get dbid, key, version */
        int dbid;
        const char *keystr;
        size_t keylen;
        uint64_t version;
        const char *subkey;
        size_t subkeylen;

        if (rocksDecodeDataKey(rawkey, sdslen(rawkey), &dbid, &keystr, &keylen,
                               &version, &subkey, &subkeylen) != 0) {
            serverLog(LL_WARNING, "Failed to decode rawkey in storageRdbSaveMemory");
            cur = cur->forward[0];
            continue;
        }

        /* Skip keys from other databases */
        if (dbid != db->id) {
            cur = cur->forward[0];
            continue;
        }

        /* Only handle whole keys (no subkey) */
        if (subkey != NULL) {
            cur = cur->forward[0];
            continue;
        }

        /* Create key object - properly initialized */
        robj *keyobj = createStringObject(keystr, keylen);

        /* 跳过主字典中已存在的热key，这些key已由热路径保存，
         * 避免RDB中出现重复key导致加载时crash */
        if (lookupKeyReadWithFlags(db, keyobj, LOOKUP_NOTOUCH) != NULL) {
            decrRefCount(keyobj);
            cur = cur->forward[0];
            continue;
        }

        /* Decode rawval to get value object */
        robj *valobj = rocksDecodeValRdb(rawval);
        if (valobj == NULL) {
            serverLog(LL_WARNING, "Failed to decode rawval in storageRdbSaveMemory for key: %s", keystr);
            decrRefCount(keyobj);
            cur = cur->forward[0];
            continue;
        }

        serverLog(LL_NOTICE, "Decoded key %s: type=%d, encoding=%d, refcount=%d",
                  keystr, valobj->type, valobj->encoding, valobj->refcount);

        /* Validate the object */
        if (valobj->type != OBJ_STRING && valobj->type != OBJ_LIST &&
            valobj->type != OBJ_SET && valobj->type != OBJ_ZSET &&
            valobj->type != OBJ_HASH && valobj->type != OBJ_MODULE) {
            serverLog(LL_WARNING, "Invalid object type %d for key: %s", valobj->type, keystr);
            decrRefCount(valobj);
            decrRefCount(keyobj);
            cur = cur->forward[0];
            continue;
        }

        /* Get expire time */
        long long expire = getExpire(db, keyobj->ptr, NULL);

        /* Save key-value pair to RDB */
        int save_result = rdbSaveKeyValuePair(rdb, keyobj, valobj, expire, dbid);
        if (save_result == -1) {
            errstr = sdscatfmt(sdsempty(), "Failed to save key to RDB: %s", keystr);
            recoverable_err = 1;
        } else {
            ctx->key_count++;
        }

        /* Clean up */
        decrRefCount(keyobj);
        decrRefCount(valobj);

        if (recoverable_err) {
            serverLog(LL_WARNING, "storageRdbSaveMemory error: %s", errstr);
            sdsfree(errstr);
            return C_ERR;
        }

        cur = cur->forward[0];
    }

    serverLog(LL_NOTICE, "storageRdbSaveMemory completed, saved %ld keys from db %d",
              ctx->key_count, db->id);
    return C_OK;
}

enum SAVE_RESULT_ENUM memoryStorageSaveColdKeys(RdbSaveCtx* ctx, struct rio* rdb, redisDb* db) {
    MemoryStorageRdbSaveCtx* memory_ctx = ctx->ctx;
    serverLog(LL_NOTICE, "memoryStorageSaveColdKeys starting for db %d", db->id);
    int hot_ext_result = stroageRdbSaveHotExtension(rdb, db, memory_ctx);
    if (hot_ext_result) {
        serverLog(LL_WARNING, "stroageRdbSaveHotExtension failed with result %d", hot_ext_result);
        return SAVE_FAIL;
    }
    int memory_result = storageRdbSaveMemory(rdb, db, memory_ctx);
    if (memory_result) {
        serverLog(LL_WARNING, "storageRdbSaveMemory failed with result %d", memory_result);
        return SAVE_FAIL;
    }
    serverLog(LL_NOTICE, "memoryStorageSaveColdKeys completed successfully for db %d", db->id);
    return SAVE_SUCC;
}

RdbSaveCtxType memory_storage_rdb_save_ctx_type = {
    .free_rdb_save_ctx = freeMemoryStorageSaveRdbCtx,
    .save_db_init = memoryStorageSaveDbInit,
    .save_db_deinit = memoryStorageSaveDbDeInit,
    .save_hot_key = memoryStorageSaveHotKey,
    .save_cold_keys = memoryStorageSaveColdKeys,
};



int mseSetRdbSaveCtxType(struct RdbSaveCtx* ctx, struct rio* rdb, int rdbflags) {
    ctx->type = &memory_storage_rdb_save_ctx_type;
    MemoryStorageRdbSaveCtx* memory_ctx = zmalloc(sizeof(MemoryStorageRdbSaveCtx));
    memory_ctx->rdbflags = rdbflags;
    memory_ctx->key_count = 0;
    memory_ctx->rdb = rdb;
    ctx->ctx = memory_ctx;
    return 1;
}

/* ==================== RDB Load Context 实现 ==================== */

/* memoryRdbLoadCtx - 内存引擎 RDB 加载上下文
 * 记录加载进度和 rio 指针 */
typedef struct memoryRdbLoadCtx {
    long key_count;    /* 已加载 key 计数 */
    struct rio* rdb;   /* rio 流指针 */
} memoryRdbLoadCtx;

/* memoryRdbLoadFreeCtx - 释放 load ctx 资源 */
static int memoryRdbLoadFreeCtx(RdbLoadCtx* ctx) {
    if (ctx->ctx) {
        zfree(ctx->ctx);
        ctx->ctx = NULL;
    }
    return 1;
}

/* memoryRdbLoadDbInit - 切换 DB 时初始化（当前为 no-op）
 * 输入：ctx - load ctx, db - 目标数据库
 * 输出：1 */
static int memoryRdbLoadDbInit(RdbLoadCtx* ctx, redisDb* db) {
    UNUSED(db);
    return 1;
}

/* memoryRdbLoadDbDeinit - 离开 DB 时清理（当前为 no-op）
 * 输入：ctx - load ctx, db - 当前数据库
 * 输出：1 */
static int memoryRdbLoadDbDeinit(RdbLoadCtx* ctx, redisDb* db) {
    UNUSED(db);
    return 1;
}

/* memoryStorageLoadKeyValue - 将 RDB 加载的 key-value 直接写入内存存储跳表
 *
 * 核心逻辑：
 * 1. DATA_CF：rocksEncodeDataKey 编码 key + rocksEncodeValRdb 编码 value，写入跳表
 * 2. META_CF：rocksEncodeMetaKey 编码 key + rocksEncodeMetaVal 编码元数据，写入跳表
 *    （SWAP_IN 流程查询 META_CF 判断 key 是否在存储层，缺少 META_CF 条目会导致 GET 找不到 key）
 * 3. coldFilterAddKey 标记冷键（GET 时触发 SWAP_IN）
 * 4. 更新 cold_keys 计数
 *
 * 输入：ctx - load ctx, db - 数据库, key - sds key, val - 值对象指针
 *       expiretime/lfu_freq/lru_idle - 元数据（expire 暂不处理：setExpire 需要 key 在主字典中）
 * 输出：RDB_LOAD_SUCC(成功写入存储), RDB_LOAD_FAIL(编码/写入失败) */
static RDB_LOAD_RESULT memoryStorageLoadKeyValue(RdbLoadCtx* ctx, redisDb* db,
    sds key, robj** val, long long expiretime,
    long long lfu_freq, long long lru_idle) {
    UNUSED(lfu_freq);
    UNUSED(lru_idle);
    UNUSED(expiretime);  /* setExpire 需要 key 在主字典中，冷key暂不处理expire */

    memoryRdbLoadCtx* mctx = (memoryRdbLoadCtx*)ctx->ctx;
    StorageEngine *engine = server.storage.engine;
    memoryStorageEngineCtx *mse_ctx = (memoryStorageEngineCtx*)engine->context;

    /* 1. 编码 data key（与 SWAP_OUT 格式兼容：dbid|key|version|subkey） */
    sds rawkey = rocksEncodeDataKey(db, key, SWAP_VERSION_ZERO, NULL);
    if (rawkey == NULL) {
        return RDB_LOAD_FAIL;
    }

    /* 2. 编码 value 为 RDB 二进制格式（与 SWAP_OUT 兼容） */
    sds rawval = rocksEncodeValRdb(*val);
    if (rawval == NULL) {
        sdsfree(rawkey);
        return RDB_LOAD_FAIL;
    }

    /* 3. 写入 DATA_CF 跳表 */
    mseSkipListInsert(mse_ctx->sl[DATA_CF], rawkey, rawval);

    /* 4. 写入 META_CF 跳表（SWAP_IN 流程查询 META_CF 判断 key 是否存在） */
    sds metakey = rocksEncodeMetaKey(db, key);
    sds metaval = rocksEncodeMetaVal((*val)->type, expiretime, SWAP_VERSION_ZERO, NULL);
    mseSkipListInsert(mse_ctx->sl[META_CF], metakey, metaval);

    /* 5. 标记为冷键（GET 时触发 SWAP_IN 从存储层加载） */
    coldFilterAddKey(db->storage.cold_filter, key);

    /* 6. 更新冷键计数 */
    db->storage.cold_keys++;

    mctx->key_count++;
    return RDB_LOAD_SUCC;
}

/* memory_storage_rdb_load_ctx_type - 内存引擎 RDB 加载虚函数表 */
static RdbLoadCtxType memory_storage_rdb_load_ctx_type = {
    .free_rdb_load_ctx = memoryRdbLoadFreeCtx,
    .load_db_init = memoryRdbLoadDbInit,
    .load_db_deinit = memoryRdbLoadDbDeinit,
    .load_key_value = memoryStorageLoadKeyValue,
};

/* mseSetRdbLoadCtxType - 设置 load ctx 虚函数表并分配引擎上下文
 * 输入：ctx - RdbLoadCtx 指针
 * 输出：成功返回 1 */
int mseSetRdbLoadCtxType(struct RdbLoadCtx* ctx) {
    ctx->type = &memory_storage_rdb_load_ctx_type;
    memoryRdbLoadCtx* mctx = zmalloc(sizeof(memoryRdbLoadCtx));
    mctx->key_count = 0;
    mctx->rdb = NULL;
    ctx->ctx = mctx;
    return 1;
}
/* creatememoryStorageEngineCtx - 框架入口，返回 void* (memoryStorageEngineCtx*)
 * 同时填充 StorageEngine 函数指针表并注册到 server.storage.engine
 */
StorageEngineType memory_storage_type = {
    .batch_del = mseEngineBatchDEL,
    .batch_get = mseEngineBatchGet,
    .batch_put = mseEngineBatchPut,
    .put          = mseEnginePut,
    .get          = mseEngineGet,
    .del          = mseEngineDel,
    .iterate      = mseEngineIterate,
    .set_forkctx_type = mseSetForkCtxType,
    .set_rdb_save_ctx_type = mseSetRdbSaveCtxType,
    .set_rdb_load_ctx_type = mseSetRdbLoadCtxType,
};
void* createMemoryStorageEngine() {
    serverLog(LL_WARNING, "Initializing memoryStorageEngineCtx...");
    memoryStorageEngineCtx* engine = createMemoryStorageEngineCtx();

    StorageEngine *se = zmalloc(sizeof(StorageEngine));
    se->context      = engine;
    se->type = &memory_storage_type;

    return se;
}

/* freeMemoryStorageEngine - 释放内存存储引擎 */
void freeMemoryStorageEngine(void *engine) {
    if (engine == NULL) return;
    StorageEngine *se = (StorageEngine*)engine;
    freeMemoryStorageEngineCtx(se->context);
    zfree(se);
}

/* ==================== 单元测试（REDIS_TEST 模式）==================== */
#ifdef REDIS_TEST
#include "ctrip_storage_testhelp.h"

/* 构造单键 put RIO，使用栈上数组 */
#define MSE_TEST_PUT(eng, _cf, _key, _val) do { \
    int _cfs[1] = {(_cf)}; \
    sds _rk[1] = {sdsnew(_key)}; \
    sds _rv[1] = {sdsnew(_val)}; \
    struct RIO _rio = {0}; \
    _rio.put.numkeys = 1; \
    _rio.put.cfs = _cfs; \
    _rio.put.rawkeys = _rk; \
    _rio.put.rawvals = _rv; \
    mseEnginePut(eng, &_rio); \
    sdsfree(_rk[0]); sdsfree(_rv[0]); \
} while(0)

/* 查询单键，结果通过 _out_val(sds) 和 _out_nf(int) 返回，调用方 sdsfree(_out_val) */
#define MSE_TEST_GET(eng, _cf, _key, _out_val, _out_nf) do { \
    int _cfs[1] = {(_cf)}; \
    sds _rk[1] = {sdsnew(_key)}; \
    sds _rv[1] = {NULL}; \
    struct RIO _rio = {0}; \
    _rio.get.numkeys = 1; \
    _rio.get.cfs = _cfs; \
    _rio.get.rawkeys = _rk; \
    _rio.get.rawvals = _rv; \
    mseEngineGet(eng, &_rio); \
    (_out_val) = _rio.get.rawvals[0]; \
    (_out_nf)  = _rio.get.notfound; \
    sdsfree(_rk[0]); \
} while(0)

/* 删除单键 */
#define MSE_TEST_DEL(eng, _cf, _key) do { \
    int _cfs[1] = {(_cf)}; \
    sds _rk[1] = {sdsnew(_key)}; \
    struct RIO _rio = {0}; \
    _rio.del.numkeys = 1; \
    _rio.del.cfs = _cfs; \
    _rio.del.rawkeys = _rk; \
    mseEngineDel(eng, &_rio); \
    sdsfree(_rk[0]); \
} while(0)

/* 测试 put + get 基本功能：写入后能正确读取 */
static int mseTestPutGetBasic() {
    memoryStorageEngineCtx *eng = creatememoryStorageEngineCtx();
    MSE_TEST_PUT(eng, 0, "hello", "world");

    sds val; int nf;
    MSE_TEST_GET(eng, 0, "hello", val, nf);
    test_cond("put+get: value correct", val && strcmp(val, "world") == 0);
    test_cond("put+get: notfound=0", nf == 0);
    if (val) sdsfree(val);

    /* 测试不存在的 key */
    MSE_TEST_GET(eng, 0, "nokey", val, nf);
    test_cond("get nonexistent: notfound=1", nf == 1 && val == NULL);
    if (val) sdsfree(val);

    return 0;
}

/* 测试 put 覆盖：写入相同 key 后读取新值 */
static int mseTestPutOverwrite() {
    memoryStorageEngineCtx *eng = creatememoryStorageEngineCtx();
    MSE_TEST_PUT(eng, 0, "k", "v1");
    MSE_TEST_PUT(eng, 0, "k", "v2");

    sds val; int nf;
    MSE_TEST_GET(eng, 0, "k", val, nf);
    test_cond("put overwrite: value updated", val && strcmp(val, "v2") == 0);
    if (val) sdsfree(val);

    return 0;
}

/* 测试 del：删除后 get 返回 notfound */
static int mseTestDel() {
    memoryStorageEngineCtx *eng = creatememoryStorageEngineCtx();
    MSE_TEST_PUT(eng, 0, "key", "val");
    MSE_TEST_DEL(eng, 0, "key");

    sds val; int nf;
    MSE_TEST_GET(eng, 0, "key", val, nf);
    test_cond("del: notfound after del", nf == 1 && val == NULL);
    if (val) sdsfree(val);

    /* 删除不存在的 key 应静默成功 */
    MSE_TEST_DEL(eng, 0, "ghost");
    test_cond("del nonexistent: no crash", 1);

    return 0;
}

/* 测试 iterate：范围扫描返回字典序有序结果
 * 插入 c/a/b/d，iterate [a, c) 应返回 a, b
 */
static int mseTestIterate() {
    memoryStorageEngineCtx *eng = creatememoryStorageEngineCtx();
    const char *keys[] = {"c", "a", "b", "d"};
    const char *vals[] = {"vc", "va", "vb", "vd"};
    for (int i = 0; i < 4; i++) MSE_TEST_PUT(eng, 0, keys[i], vals[i]);

    struct RIO rio = {0};
    rio.iterate.cf    = 0;
    rio.iterate.start = sdsnew("a");
    rio.iterate.end   = sdsnew("c");
    rio.iterate.limit = 0;
    mseEngineIterate(eng, &rio);

    test_cond("iterate range: count=2", rio.iterate.numkeys == 2);
    test_cond("iterate range: key[0]=a", rio.iterate.rawkeys[0] && strcmp(rio.iterate.rawkeys[0], "a") == 0);
    test_cond("iterate range: key[1]=b", rio.iterate.rawkeys[1] && strcmp(rio.iterate.rawkeys[1], "b") == 0);
    test_cond("iterate range: val[0]=va", rio.iterate.rawvals[0] && strcmp(rio.iterate.rawvals[0], "va") == 0);

    sdsfree(rio.iterate.start);
    sdsfree(rio.iterate.end);
    for (int i = 0; i < rio.iterate.numkeys; i++) {
        sdsfree(rio.iterate.rawkeys[i]);
        if (rio.iterate.rawvals[i]) sdsfree(rio.iterate.rawvals[i]);
    }
    zfree(rio.iterate.rawkeys);
    zfree(rio.iterate.rawvals);

    return 0;
}

/* 测试 iterate limit：limit=2 时只返回前 2 条，nextseek 指向第 3 条 */
static int mseTestIterateLimit() {
    memoryStorageEngineCtx *eng = creatememoryStorageEngineCtx();
    const char *ks[] = {"a", "b", "c", "d", "e"};
    for (int i = 0; i < 5; i++) MSE_TEST_PUT(eng, 0, ks[i], ks[i]);

    struct RIO rio = {0};
    rio.iterate.cf    = 0;
    rio.iterate.limit = 2;
    mseEngineIterate(eng, &rio);

    test_cond("iterate limit: count=2", rio.iterate.numkeys == 2);
    test_cond("iterate limit: nextseek=c",
              rio.iterate.nextseek && strcmp(rio.iterate.nextseek, "c") == 0);

    if (rio.iterate.nextseek) sdsfree(rio.iterate.nextseek);
    for (int i = 0; i < rio.iterate.numkeys; i++) {
        sdsfree(rio.iterate.rawkeys[i]);
        if (rio.iterate.rawvals[i]) sdsfree(rio.iterate.rawvals[i]);
    }
    zfree(rio.iterate.rawkeys);
    zfree(rio.iterate.rawvals);

    return 0;
}

/* 测试 CF 隔离：不同 CF 的同名 key 互不干扰，iterate 只返回指定 CF 的数据 */
static int mseTestCFIsolation() {
    memoryStorageEngineCtx *eng = createMemoryStorageEngineCtx();
    MSE_TEST_PUT(eng, 0, "key", "cf0_val");
    MSE_TEST_PUT(eng, 1, "key", "cf1_val");

    sds v0, v1; int nf;
    MSE_TEST_GET(eng, 0, "key", v0, nf);
    MSE_TEST_GET(eng, 1, "key", v1, nf);
    test_cond("CF isolation: CF0 value", v0 && strcmp(v0, "cf0_val") == 0);
    test_cond("CF isolation: CF1 value", v1 && strcmp(v1, "cf1_val") == 0);
    if (v0) sdsfree(v0);
    if (v1) sdsfree(v1);

    /* iterate CF0 只返回 1 个 key */
    struct RIO rio = {0};
    rio.iterate.cf    = 0;
    rio.iterate.limit = 0;
    mseEngineIterate(eng, &rio);
    test_cond("CF isolation: iterate CF0 count=1", rio.iterate.numkeys == 1);
    for (int i = 0; i < rio.iterate.numkeys; i++) {
        sdsfree(rio.iterate.rawkeys[i]);
        if (rio.iterate.rawvals[i]) sdsfree(rio.iterate.rawvals[i]);
    }
    zfree(rio.iterate.rawkeys);
    zfree(rio.iterate.rawvals);

    return 0;
}

/* 测试字典序：乱序插入后 iterate 按字节序升序返回 */
static int mseTestLexOrder() {
    memoryStorageEngineCtx *eng = creatememoryStorageEngineCtx();
    const char *inserts[] = {"z", "aa", "ab", "b", "a"};
    const char *expected[] = {"a", "aa", "ab", "b", "z"};
    for (int i = 0; i < 5; i++) MSE_TEST_PUT(eng, 0, inserts[i], inserts[i]);

    struct RIO rio = {0};
    rio.iterate.cf    = 0;
    rio.iterate.limit = 0;
    mseEngineIterate(eng, &rio);

    test_cond("lex order: count=5", rio.iterate.numkeys == 5);
    int ordered = 1;
    for (int i = 0; i < rio.iterate.numkeys && i < 5; i++) {
        if (strcmp(rio.iterate.rawkeys[i], expected[i]) != 0) { ordered = 0; break; }
    }
    test_cond("lex order: keys in lexicographic order", ordered);

    for (int i = 0; i < rio.iterate.numkeys; i++) {
        sdsfree(rio.iterate.rawkeys[i]);
        if (rio.iterate.rawvals[i]) sdsfree(rio.iterate.rawvals[i]);
    }
    zfree(rio.iterate.rawkeys);
    zfree(rio.iterate.rawvals);

    return 0;
}

/* 内存引擎测试入口，供 ctripStorageTest 调用 */
int memoryStorageEngineCtxTest(int argc, char **argv, int accurate) {
    UNUSED(argc); UNUSED(argv); UNUSED(accurate);
    mseTestPutGetBasic();
    mseTestPutOverwrite();
    mseTestDel();
    mseTestIterate();
    mseTestIterateLimit();
    mseTestCFIsolation();
    mseTestLexOrder();
    return 0;
}

#endif /* REDIS_TEST */
