

#include "ctrip_storage_expire.h"
#include "ctrip_storage.h"
#include "ctrip_storage_request.h"
#include "ctrip_storage_commands.h"
#include "ctrip_storage_lock.h"
#include "object.h"
#include "versions/functions.h"

/* src/expire.c 中定义，跟踪 slave 上带过期时间的 key（dbid bitmap）*/
extern dict *slaveKeysWithExpire;
/* src/expire.c 中定义，尝试过期单个 key（swap模式下会调回本文件的 storageActiveExpireTryExpire）*/
extern int activeExpireCycleTryExpire(redisDb *db, kvobj *kv, long long now);

/* ==========================================================================
 * slave key 两阶段异步过期机制
 *
 * 问题：expireSlaveKeys（内存模式）假设 key 一定在内存中，直接查 expires dict 即可。
 *       swap 模式下 key 可能是冷key（仅在 rocksdb），无法通过内存路径判断是否过期。
 *
 * 方案：
 *   热key（lookupKeyReadWithFlags 能找到）→ 走 activeExpireCycleTryExpire，
 *         通过 swap_expire_clients 提交异步 SWAP_IN_DEL 请求（delete-on-swap-in）
 *   冷key（不在内存）→ 提交 swap_ttl_clients 请求加载 meta，
 *         回调 slaveExpireClientKeyRequestFinished 判断 expire < now，
 *         若确认过期则写入 slave_expiring_keys 队列，下一轮 expireSlaveKeysSwapMode 排空
 * ========================================================================== */

/* 记录一个已确认需要过期的冷key（元数据已加载，expire < now） */
typedef struct slaveExpiringKey {
    robj *key;      /* key对象（已加 refcount），由 expireSlaveKeysSwapMode 负责 decrRefCount */
    redisDb *db;    /* 所在数据库 */
} slaveExpiringKey;

/* 确认要过期的冷key队列，跨周期传递
 * 由 slaveExpireClientKeyRequestFinished 写入，由 expireSlaveKeysSwapMode Phase1 排空 */
static list *slave_expiring_keys = NULL;

/* 第1处：swap_expire_clients 的回调，expire 请求完成后的清理
 * 输入: c - swap_expire_clients[dbid], ctx - swap上下文 */
static void expireClientKeyRequestFinished(client *c, swapCtx *ctx) {
    robj *key = ctx->key_request->key;
    if (ctx->errcode) clientSwapError(c, ctx->errcode);
    incrRefCount(key);
    c->deferred_cmd->keyrequests_count--;
    clientReleaseLocks(c, ctx);
    decrRefCount(key);
}

/* 第2处（提交函数）：对热key提交异步 expire 请求（SWAP_IN_DEL，swap-in时自动删除rocksdb数据）
 * 输入: c - swap_expire_clients[dbid], key - 需要过期的key, force - 是否强制（slave场景传1）
 * 输出: 1=已提交, 0=有前序请求跳过 */
static int submitExpireClientRequest(client *c, robj *key, int force) {
    uint32_t cmd_intention_flags = force ? SWAP_EXPIRE_FORCE : 0;
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    serverLog(LL_WARNING, "expireClientKeyRequestFinished step1");
    getKeyRequestsPrepareResult(&result, 1);
    serverLog(LL_WARNING, "expireClientKeyRequestFinished step1.1 %d", key->refcount);
    incrRefCount(key);
    serverLog(LL_WARNING, "expireClientKeyRequestFinished step2: %p ", c->cmd);
    swapCommand *swapCmd = lookupSwapCommand(c->cmd->fullname);
    serverLog(LL_WARNING, "expireClientKeyRequestFinished: %s %p", c->cmd->fullname, swapCmd);
    getKeyRequestsAppendSubkeyResult(&result, REQUEST_LEVEL_KEY, key, 0, NULL,
            swapCmd->intention, cmd_intention_flags | swapCmd->intention_flags,
            c->cmd->flags, c->db->id);
    c->deferred_cmd->keyrequests_count++;
    submitClientKeyRequests(c, &result, expireClientKeyRequestFinished, NULL);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
    serverLog(LL_WARNING, "expireClientKeyRequestFinished step2");
    
    return 1;
}

/* 第3处（A）：swap_ttl_clients 的回调，冷key meta 加载完成后判断是否过期
 * 输入: c - swap_ttl_clients[dbid], ctx - swap上下文（含key和dbid）
 * 逻辑: expire < now → 放入 slave_expiring_keys 队列；同时清除 slaveKeysWithExpire bitmap 位 */
static void slaveExpireClientKeyRequestFinished(client *c, swapCtx *ctx) {
    redisDb *db = c->db;
    robj *key = ctx->key_request->key;
    int dbid = ctx->key_request->dbid;
    long long expire = getExpire(db, key->ptr, NULL);
    dictEntry *dbids_entry = dictFind(slaveKeysWithExpire, key->ptr);

    if (expire >= 0 && expire < mstime()) {
        /* 确认已过期：放入队列，等 expireSlaveKeysSwapMode Phase1 提交强制expire */
        if (slave_expiring_keys == NULL)
            slave_expiring_keys = listCreate();
        slaveExpiringKey *sek = zmalloc(sizeof(slaveExpiringKey));
        incrRefCount(key);
        sek->key = key;
        sek->db = db;
        listAddNodeTail(slave_expiring_keys, sek);
    }

    /* 无论是否过期，清除该dbid在bitmap中的位；bitmap归零则删除整条记录 */
    if (dbids_entry) {
        uint64_t dbids = dictGetUnsignedIntegerVal(dbids_entry);
        dbids &= ~(1ULL << dbid);
        if (dbids)
            dictSetUnsignedIntegerVal(dbids_entry, dbids);
        else
            dictDelete(slaveKeysWithExpire, key->ptr);
    }

    incrRefCount(key);
    if (ctx->errcode) clientSwapError(c, ctx->errcode);
    c->deferred_cmd->keyrequests_count--;
    clientReleaseLocks(c, ctx);
    decrRefCount(key);
}

/* 第3处（B）：对冷key提交 ttl 请求以加载 meta，回调为 slaveExpireClientKeyRequestFinished
 * 输入: c - swap_ttl_clients[dbid], key - 冷key对象 */
static int submitSlaveExpireClientRequest(client *c, robj *key) {
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    getKeyRequestsPrepareResult(&result, 1);
    incrRefCount(key);
    swapCommand *swapCmd = lookupSwapCommand(c->cmd->fullname);
    getKeyRequestsAppendSubkeyResult(&result, REQUEST_LEVEL_KEY, key, 0, NULL,
            swapCmd->intention, swapCmd->intention_flags, c->cmd->flags, c->db->id);
    c->deferred_cmd->keyrequests_count++;
    submitClientKeyRequests(c, &result, slaveExpireClientKeyRequestFinished, NULL);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
    return 1;
}

/* activeExpireCycleTryExpire 的 swap 分支封装（被 src/expire.c 调用）
 * 输入: db - 数据库, keyobj - 需要过期的key（调用者持有引用，本函数不释放）
 * 输出: 1=已提交expire请求, 0=跳过（key上有前序请求） */
int storageActiveExpireTryExpire(redisDb *db, robj *keyobj) {
    /* 若key上已有 in-progress 请求，跳过以避免重复提交 expire */
    if (lockWouldBlock(server.storage.swap_txid++, db, keyobj))
        return 0;
    client *c = server.storage.swap_expire_clients[db->id];
    /* slave 产生的过期请求需要 force=1，跳过角色检查直接执行 */
    int force = server.masterhost ? SWAP_EXPIRE_FORCE : 0;
    return submitExpireClientRequest(c, keyobj, force);
}

/* Phase1 辅助：排空上一轮 ttl_client 回调确认要过期的冷key队列
 * 由 expireSlaveKeys 在 swap 模式下每轮开头调用 */
void expireSlaveKeysPendingQueue(void) {
    if (!slave_expiring_keys || !listLength(slave_expiring_keys)) return;
    listNode *ln;
    while ((ln = listFirst(slave_expiring_keys))) {
        slaveExpiringKey *sek = listNodeValue(ln);
        client *c = server.storage.swap_expire_clients[sek->db->id];
        submitExpireClientRequest(c, sek->key, 1);  /* force=1 */
        decrRefCount(sek->key);
        zfree(sek);
        listDelNode(slave_expiring_keys, ln);
    }
}

/* 对冷key（不在内存）提交 ttl_client 请求以加载 meta，回调决定是否过期
 * 由 expireSlaveKeys 在 swap 模式下遇到冷key时调用
 * 输入: c - swap_ttl_clients[dbid], key - 冷key对象 */
void storageSlaveExpireColdKey(client *c, robj *key) {
    submitSlaveExpireClientRequest(c, key);
}

// /* expireSlaveKeys 辅助：检测并处理冷key（swap模式）
//  * 若key不在内存（冷key），提交 ttl_client 异步加载 meta，并将该dbid写入 new_dbids
//  * 输入: db/dbid - 数据库及其id, keyname - key名, new_dbids - 输出参数，追加该dbid位
//  * 输出: 1=是冷key已处理（调用者应 continue），0=非swap模式或热key（走正常过期路径）*/
// int storageSlaveExpireCheckColdKey(redisDb *db, int dbid, sds keyname, uint64_t *new_dbids) {
//     if (!isStorageSPIEnabled()) return 0;
//     robj *keyobj = createStringObject(keyname, sdslen(keyname));
//     int cold = (lookupKeyReadWithFlags(db, keyobj, LOOKUP_NOTOUCH) == NULL);
//     if (cold) {
//         storageSlaveExpireColdKey(server.storage.swap_ttl_clients[dbid], keyobj);
//         *new_dbids |= (uint64_t)1 << dbid;
//     }
//     decrRefCount(keyobj);
//     return cold;
// }

expireCandidates *expireCandidatesCreate(size_t capacity) {
    expireCandidates *ecs;
    robj *zobj = createZsetObject();
    serverAssert(capacity > 0);
    ecs = zmalloc(sizeof(expireCandidates));
    ecs->zobj = zobj;
    ecs->capacity = capacity;
    return ecs;
}

/* Scan Expire */
scanExpire *scanExpireCreate(void) {
    scanExpire *scan_expire = zcalloc(sizeof(scanExpire));
    scan_expire->nextseek = NULL;
    scan_expire->limit = EXPIRESCAN_DEFAULT_LIMIT;
    scan_expire->candidates = expireCandidatesCreate(EXPIRESCAN_DEFAULT_CANDIDATES);
    return scan_expire;
}

void scanExpireCycleTryExpire(sds key, long long expire, redisDb *db,
        long long now) {
    serverLog(LL_WARNING, "expireScanCycleTryExpire setp1: %s", key);
    robj *keyobj = createStringObject(key,sdslen(key));
    client *c = server.storage.swap_expire_clients[db->id];
    serverAssert(expire <= now);
    serverLog(LL_WARNING, "expireScanCycleTryExpire setp2");
    submitExpireClientRequest(c, keyobj, 0);
    decrRefCount(keyobj);
    serverLog(LL_WARNING, "expireScanCycleTryExpire setp3");
}


#define SCAN_EXPIRE_CYCLE_KEYS_PER_LOOP 20
#define SCAN_EXPIRE_CYCLE_KEYS_BASE 16
#define SCAN_EXPIRE_CYCLE_KEYS_MAX 256
#define SWAP_TTL_COMPACT_INVALID_EXPIRE LLONG_MAX /* expire or pexpire is long long int. */

void metaScan4ScanExpireRequestFinished(client *c, swapCtx *ctx) {
    UNUSED(ctx);
    robj *key = ctx->key_request->key;
    if (ctx->errcode) clientSwapError(c,ctx->errcode);
    incrRefCount(key);
    c->deferred_cmd->keyrequests_count--;
    clientReleaseLocks(c,ctx);
    decrRefCount(key);
}

/* NOTE: expire-scan is designed not to run in-parallel. */
void startMetaScan4ScanExpire(client *c) {
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    const char *expire_scan_key = "____expire_scan____";
    robj *key = createStringObject(expire_scan_key,strlen(expire_scan_key));
    getKeyRequestsPrepareResult(&result,1);
    getKeyRequestsAppendSubkeyResult(&result,REQUEST_LEVEL_KEY,key,0,NULL,
            SWAP_IN,SWAP_METASCAN_EXPIRE,c->cmd->flags,c->db->id);
    c->deferred_cmd->keyrequests_count++;
    submitDeferredClientKeyRequests(c,&result,metaScan4ScanExpireRequestFinished,NULL);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
}
/* expire */
typedef void (*expiredHandler)(sds key, long long expire, redisDb *db, long long now);

/* Seek zslDeleteRangeByScore for details */
unsigned long zslDeleteRangeByScoreWithLimitHandler(zskiplist *zsl,
        zrangespec *range, dict *dict, unsigned long limit,
        expiredHandler handler, redisDb *db, long long now) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;

    x = zsl->header;
    for (int i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score, range))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;
    
    /* Delete nodes while in range. */
    while (x && zslValueLteMax(x->score, range) && removed < limit) {
        zskiplistNode *next = x->level[0].forward;
        sds ele = zslGetNodeElement(x);
        if (handler) handler(ele,x->score,db,now);
        zslUnlinkNode(zsl,x,update);
        dictDelete(dict,ele);
        zslFreeNode(zsl,x); /* Here is where x->ele is actually released. */
        removed++;
        x = next;
    }

    return removed;
}
size_t expireCandidatesRemoveExpired(expireCandidates *ecs, long long now,
        size_t limit, expiredHandler handler, redisDb *db) {
    zset *zs = ecs->zobj->ptr;
    zskiplist *zsl = zs->zsl;
    dict *dict = zs->dict;
    zrangespec _expired_range, *expired_range = &_expired_range;
    expired_range->min = 0;
    expired_range->minex = 0;
    expired_range->max = now;
    expired_range->maxex = 0;
    return zslDeleteRangeByScoreWithLimitHandler(zsl,expired_range,dict,
            limit,handler,db,now);
}

int expireCandidatesAdd(expireCandidates *ecs, long long expire, sds key) {
    robj *zobj = ecs->zobj;
    int out_flags;
    serverAssert(zsetLength(zobj) <= ecs->capacity);
    if (zsetLength(zobj) == ecs->capacity) {
        double max_expire;
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zln = zsl->tail;
        max_expire = zln->score;
        if (expire < max_expire) {
            zsetDel(zobj,zslGetNodeElement(zln));
            zsetAdd(zobj,expire,key,ZADD_IN_NONE,&out_flags,NULL);
        } else {
            out_flags = ZADD_OUT_NOP;
        }
    } else {
        zsetAdd(zobj,expire,key,ZADD_IN_NONE,&out_flags,NULL);
    }
    return out_flags & ZADD_OUT_ADDED;
}

int ctripStorageScanExpireDbCycle(redisDb* db, int type, long long timelimit) {
    if(timelimit <= 0) return 1;
    scanExpire *scan_expire = db->storage.scan_expire;
    client *c = server.storage.swap_scan_expire_clients[db->id];
    int timelimit_exit = 0;
    static long long stat_last_update_time = 0,  
                stat_scan_keys = 0,               
                stat_scan_expired_keys = 0;
    long long start = ustime(), scan_time, expire_time, elapsed;      
    if (db->storage.cold_keys <= 0) goto update_stats;
    unsigned long
        effort = server.active_expire_effort-1,
        expire_keys_per_loop = SCAN_EXPIRE_CYCLE_KEYS_PER_LOOP +
                           SCAN_EXPIRE_CYCLE_KEYS_PER_LOOP/4*effort,
        scan_expire_base = SCAN_EXPIRE_CYCLE_KEYS_BASE +
            SCAN_EXPIRE_CYCLE_KEYS_BASE/4*effort;
    scan_expire->limit = scan_expire_base +
            (SCAN_EXPIRE_CYCLE_KEYS_MAX - scan_expire_base) *
            scan_expire->stale_percent;
    /* step 1: add candidates */
    if (type != ACTIVE_EXPIRE_CYCLE_FAST && c->deferred_cmd->swap_metas != NULL) {
        metaScanResult *metas = c->deferred_cmd->swap_metas;
        serverAssert(scan_expire->inprogress);
        for (int i = 0; i < metas->num; i++) {
            scanMeta *meta = metas->metas + i;

            long long nowtime = server.mstime;
            long long expire_add;

            if (meta->expire != -1) {
                expireCandidatesAdd(scan_expire->candidates,
                        meta->expire,meta->key);
                expire_add = meta->expire - nowtime;
            } else {
                expire_add = SWAP_TTL_COMPACT_INVALID_EXPIRE;
            }
            // compact 
            // if (server.storage.swap_ttl_compact_enabled) {
            //     int res = wtdigestAdd(server.storage.swap_ttl_compact_ctx->expire_stats->expire_wt, (double)expire_add, 1);
            //     serverAssert(res == 0);
            // }
        }

        stat_scan_keys += metas->num;
        freeScanMetaResult(metas);
        c->deferred_cmd->swap_metas = NULL;
        scan_expire->inprogress = 0;
    }
    if (type != ACTIVE_EXPIRE_CYCLE_FAST && !scan_expire->inprogress) {
        serverAssert(c->deferred_cmd->swap_metas == NULL);
        scan_expire->inprogress = 1;
        startMetaScan4ScanExpire(c);
    }

    scan_time = ustime() - start;

    size_t total_removed = 0;
    int drained = 0, iteration = 0; 
    do {
        size_t removed = expireCandidatesRemoveExpired(
                scan_expire->candidates,start/1000,expire_keys_per_loop,
                scanExpireCycleTryExpire,db);

        total_removed += removed;
        iteration++;

        drained = removed < expire_keys_per_loop;

        if ((iteration & 0xf) == 0) {
            elapsed = ustime()-start;
            if (elapsed > timelimit) {
                timelimit_exit = 1;
                server.stat_expired_time_cap_reached_count++;
            }
        }
    } while (!drained && !timelimit_exit); 

    elapsed = ustime() - start;
    expire_time = elapsed - scan_time;

    scan_expire->stat_scan_time_used += scan_time;
    scan_expire->stat_expire_time_used += expire_time;
    stat_scan_expired_keys += total_removed;
update_stats:
    if (start/1000000 > stat_last_update_time) {
        stat_last_update_time = start/1000000;
        scan_expire->stat_scan_per_sec = stat_scan_keys;
        scan_expire->stat_expired_per_sec = stat_scan_expired_keys;
        if (stat_scan_keys > 0)
            scan_expire->stale_percent = (double)stat_scan_expired_keys/stat_scan_keys;
        else
            scan_expire->stale_percent = 0;
        stat_scan_keys = 0;
        stat_scan_expired_keys = 0;
    }

    if (scan_expire->stat_scan_per_sec) {
        scan_expire->stat_estimated_cycle_seconds =
            db->storage.cold_keys/scan_expire->stat_scan_per_sec;
    }

    return timelimit_exit;
}