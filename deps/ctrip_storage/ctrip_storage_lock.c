#include "ctrip_storage_lock.h"
#include "buffered_allocator.h"
#include "ctrip_storage_metric.h"

#define LINK_TO_LOCK(link_ptr) ((struct lock*)((char*)(link_ptr)-offsetof(struct lock,link)))

static inline void *lock_malloc(size_t size) {
    void *ptr = zmalloc(size);
#ifdef LOCK_PRECISE_MMEORY_USED
#ifdef HAVE_MALLOC_SIZE
    if (ptr) lock_memory_used += zmalloc_size(ptr);
#endif
#endif
    return ptr;
}

static inline void *lock_realloc(void *oldptr, size_t size) {
#ifdef LOCK_PRECISE_MMEORY_USED
#ifdef HAVE_MALLOC_SIZE
    if (oldptr) lock_memory_used -= zmalloc_size(oldptr);
#endif
#endif
    void *ptr = zrealloc(oldptr,size);
#ifdef LOCK_PRECISE_MMEORY_USED
#ifdef HAVE_MALLOC_SIZE
    if (ptr) lock_memory_used += zmalloc_size(ptr);
#endif
#endif
    return ptr;
}

static inline void lock_free(void *ptr) {
#ifdef LOCK_PRECISE_MMEORY_USED
#ifdef HAVE_MALLOC_SIZE
    if (ptr) lock_memory_used -= zmalloc_size(ptr);
#endif
#endif
    zfree(ptr);
}

static inline const char *requestLevelName(int level) {
  const char *name = "?";
  const char *levels[] = {"SVR","DB","KEY"};
  if (level >= 0 && level < REQUEST_LEVEL_TYPES)
    name = levels[level];
  return name;
}


static inline int lockLinkTargetReady(lockLinkTarget *target) {
    serverAssert(target->signaled <= target->linked);
    return target->signaled == target->linked;
}

static void lockStatUpdateLocked(lock *lock) {
    int level = lock->locks->level;
    lockStat *stat = server.storage.swap_lock->stat;
    lockInstantaneouStat *inst_stat = stat->instant+level;
    lockCumulativeStat *cumu_stat = &stat->cumulative;

    cumu_stat->request_count++;
    inst_stat->request_count++;
    if (lock->conflict) {
        cumu_stat->conflict_count++;
        inst_stat->conflict_count++;
    }
}




static inline void lockStartLatencyTraceIfNeeded(lock *lock) {
    if (lock->conflict && server.storage.swap_debug_trace_latency) {
        elapsedStart(&lock->lock_timer);
    } else {
        lock->lock_timer = 0;
    }
}

static inline int lockShouldFlushAfterProceed(lock *lock) {
    return lock->link.links.count > 0;
}

static inline void lockEndLatencyTraceIfNeeded(lock *lock) {
    if (lock->lock_timer) {
        metricDebugInfo(SWAP_DEBUG_LOCK_WAIT, elapsedUs(lock->lock_timer));
    }
}

static void lockUpdateWaitTime(lock *lock) {
    long long wait_time = ustime() - lock->start_time;
    int level;
    if (lock->key != NULL) {
        level = REQUEST_LEVEL_KEY;
    } else if (lock->db != NULL) {
        level = REQUEST_LEVEL_DB;
    } else {
        level = REQUEST_LEVEL_SVR;
    }
    lockInstantaneouStat* stat = server.storage.swap_lock->stat->instant+level;
    atomicIncr(stat->wait_time, wait_time);
    atomicIncr(stat->proceed_count, 1);
    if (stat->wait_time_maxs[stat->wait_time_max_index] < wait_time) {
        stat->wait_time_maxs[stat->wait_time_max_index] = wait_time;
    }

}
static void lockProceed(lock *lock) {
    int flush = lockShouldFlushAfterProceed(lock);
    serverAssert(lockLinkTargetReady(&lock->link.target));
    lockEndLatencyTraceIfNeeded(lock);
    if (lock->start_time != 0) lockUpdateWaitTime(lock);
    lock->proceed(lock,flush,lock->db,lock->key,lock->c,lock->pd);
}

static inline int lockProceedIfReady(lock *lock) {
    lock->conflict = !lockLinkTargetReady(&lock->link.target);
    lockStatUpdateLocked(lock);
    lockStartLatencyTraceIfNeeded(lock);
    if (!lock->conflict) {
        lockProceed(lock);
        return 1;
    } else {
        return 0;
    }
}

static inline void lockAttachToLocks(lock *lock, locks *locks) {
    listAddNodeTail(locks->lock_list,lock);
    lock->locks = locks;
    lock->locks_ln = listLast(locks->lock_list);
}
static inline lock *locksLastLock(locks *locks) {
    if (locks == NULL) return NULL;
    listNode *ln = listLast(locks->lock_list);
    return ln ? listNodeValue(ln) : NULL;
}
static inline void lockLinkTargetSignaled(lockLinkTarget *target) {
    target->signaled++;
    serverAssert(target->signaled <= target->linked);
}




#define LOCK_LINKS_LINER_SIZE 4096
static void lockLinksMakeRoomFor(lockLinks *links, int count) {
    if (count <= links->capacity) return;

    while (links->capacity < count &&
            links->capacity < LOCK_LINKS_LINER_SIZE) {
        links->capacity *= 2;
    }
    while (links->capacity < count &&
            links->capacity >= LOCK_LINKS_LINER_SIZE) {
        links->capacity += LOCK_LINKS_LINER_SIZE;
    }
    serverAssert(links->capacity >= count);

    if (links->links == links->buf) {
        links->links = lock_malloc(sizeof(lock*)*links->capacity);
        memcpy(links->links,links->buf,sizeof(lock*)*LOCK_LINKS_BUF_SIZE);
    } else {
        links->links = lock_realloc(links->links,sizeof(lock*)*links->capacity);
    }
}

static inline void lockLinksPush(lockLinks *links, void *target) {
    lockLinksMakeRoomFor(links,links->count+1);
    links->links[links->count++] = target;
}

static inline void lockLinkTargetLinked(lockLinkTarget *target) {
    serverAssert(target->signaled <= target->linked);
    target->linked++;
}

void lockLinkLink(lockLink *from, lockLink *to, int *test_would_block) {
    serverAssert(from->txid <= to->txid);
    int wont_block = (from->links.proceeded && from->txid == to->txid) ||
            from->links.unlocked;

    if (test_would_block) {
       if (!wont_block) {
           *test_would_block = 1;
       }
       return;
    }

    lockLinksPush(&from->links,to);
    lockLinkTargetLinked(&to->target);
    if (wont_block) {
        lockLinkTargetSignaled(&to->target);
    }
}

/* create children link (with higher target level) of left to lock */
void lockMigrateChildrenLinks(lock *left, lock *lock, int *would_block) {
    int level = left->locks->level;
    for (int i = 0; i < left->link.links.count; i++) {
        struct lock *from = LINK_TO_LOCK(left->link.links.links[i]);
        if (from->locks == NULL || from->locks->level <= level) {
            /* skip lower level or current (locks is NULL) lock */
            continue;
        }
        lockLinkLink(&from->link,&lock->link,would_block);
        if (would_block && *would_block) break;
    }
}

/* create link with upper or current level lock (if exits). */
static inline void locksLinkLock(locks *locks, lock* lock, int *would_block) {
    struct lock *last;
    if ((last = locksLastLock(locks))) {
        lockLinkLink(&last->link,&lock->link,would_block);
    }
}

static void dbLocksChildrenLinkLock(locks *locks, lock* lock, int *would_block) {
    dictEntry *de;
    dictIterator *di = dictGetIterator(locks->db.keys);
    serverAssert(locks->level == REQUEST_LEVEL_DB);
    while ((de = dictNext(di)) != NULL) {
        struct locks *keylocks = dictGetVal(de);
        locksLinkLock(keylocks,lock,would_block);
        if (would_block && *would_block) break;
    }
    dictReleaseIterator(di);
}
static void svrLocksChildrenLinkLock(locks *locks, lock* lock, int *would_block) {
    serverAssert(locks->level == REQUEST_LEVEL_SVR);
    for (int i = 0; i < locks->svr.dbnum; i++) {
        struct locks *dblocks = locks->svr.dbs[i];
        locksLinkLock(dblocks,lock,would_block);
        if (would_block && *would_block) break;
        dbLocksChildrenLinkLock(dblocks,lock,would_block);
        if (would_block && *would_block) break;
    }
}


/* create link with all children of current level locks. */
void locksChildrenLinksLock(locks *locks, lock* lock, int *would_block) {
    switch (locks->level) {
    case REQUEST_LEVEL_SVR:
        svrLocksChildrenLinkLock(locks,lock,would_block);
        break;
    case REQUEST_LEVEL_DB:
        dbLocksChildrenLinkLock(locks,lock,would_block);
        break;
    case REQUEST_LEVEL_KEY:
        break;
    default:
        serverPanic("unexpected locks level");
        break;
    }
}

static inline void locksChildrenLinkLock(locks* locks, lock *lock,
        int *would_block) {
    struct lock *last = locksLastLock(locks);
    if (last == NULL) {
        if (locks) {
            locksChildrenLinksLock(locks,lock,would_block);
        } else {
            /* Note locks could be NULL if test would block */
            serverAssert(would_block != NULL);
        }
    } else {
        /* last->links is the 'right' part of last children, but it's
         * equvilant to 'all' of last children, 'left' part of children
         * can be represented by last itself. */
        lockMigrateChildrenLinks(last,lock,would_block);
    }
}




#define BUFFERED_ALLOCATOR_CAPACITY_LOCK 4096
#define BUFFERED_ALLOCATOR_CAPACITY_KEYLOCKS 4096

struct bufferedAllocator *buffered_allocator_lock;
struct bufferedAllocator *buffered_allocator_keylocks;

static inline void lockLinkTargetInit(lockLinkTarget *target) {
    target->linked = 0;
    target->signaled = 0;
}

static void lockLinksInit(lockLinks *links) {
    memset(links->buf,0,sizeof(lock*)*LOCK_LINKS_BUF_SIZE);
    links->links = links->buf;
    links->capacity = LOCK_LINKS_BUF_SIZE;
    links->count = 0;
    links->proceeded = 0;
    links->unlocked = 0;
    links->reserved = 0;
}

static void lockLinksDeinit(lockLinks *links) {
    if (links->links && links->links != links->buf)
        lock_free(links->links);
    lockLinksInit(links);
}

void lockLinkDeinit(lockLink *link) {
    link->txid = 0;
    lockLinksDeinit(&link->links);
    lockLinkTargetInit(&link->target);
}

void lockFree(lock *lock) {
    serverAssert(lockLinkTargetReady(&lock->link.target));
    serverAssert(lock->locks_ln == NULL);
    serverAssert(lock->locks == NULL);

    lockLinkDeinit(&lock->link);
    if (lock->key) {
        decrRefCount(lock->key);
        lock->key = NULL;
    }
    if (lock->pdfree) {
        lock->pdfree(lock->pd);
    }
    lock->pd = NULL;
    lock->pdfree = NULL;

    bufferedAllocatorFree(buffered_allocator_lock,lock);
}
uint64_t dictObjHash(const void *key);
int dictObjKeyCompare(struct dictCmpCache *privdata, const void *key1, const void *key2);

dictType keyLevelLockDictType = {
    dictObjHash,                    /* hash function */
    NULL,                           /* key dup */
    NULL,                           /* val dup */
    dictObjKeyCompare,              /* key compare */
    NULL,                           /* key destructor */
    NULL,                           /* val destructor */
    NULL                            /* allow to expand */
};

static inline void locksSetLevelParent(locks *locks, int level,
        struct locks *parent) {
    locks->level = level;
    locks->parent = parent;
}

locks *locksCreate(int level, redisDb *db, robj *key, locks *parent) {
    locks *locks = NULL;

    switch (level) {
    case REQUEST_LEVEL_SVR:
        serverAssert(parent == NULL);
        locks = lock_malloc(sizeof(struct locks));
        locksSetLevelParent(locks,level,parent);
        locks->lock_list = listCreate();
        locks->svr.dbnum = server.dbnum;
        locks->svr.dbs = lock_malloc(locks->svr.dbnum*sizeof(struct locks));
        break;
    case REQUEST_LEVEL_DB:
        serverAssert(parent->level == REQUEST_LEVEL_SVR);
        serverAssert(db);
        locks = lock_malloc(sizeof(struct locks));
        locksSetLevelParent(locks,level,parent);
        locks->lock_list = listCreate();
        locks->db.db = db;
        locks->db.keys = dictCreate(&keyLevelLockDictType);
        break;
    case REQUEST_LEVEL_KEY:
        serverAssert(parent->level == REQUEST_LEVEL_DB);
        serverAssert(db && key);
        locks = bufferedAllocatorAlloc(buffered_allocator_keylocks);
        locksSetLevelParent(locks,level,parent);
        incrRefCount(key);
        locks->key.key = key;
        dictAdd(parent->db.keys,key/*ref*/,locks);
        break;
    default:
        serverPanic("unexpected lock level");
        break;
    }

    return locks;
}




void lockLinkInit(lockLink *link, int64_t txid) {
    link->txid = txid;
    lockLinksInit(&link->links);
    lockLinkTargetInit(&link->target);
}

lock *lockNew(int64_t txid, redisDb *db, robj *key, client *c,
        lockProceedCallback proceed, void *pd, freefunc pdfree,
        void *msgs) {
    lock *lock = bufferedAllocatorAlloc(buffered_allocator_lock);

    lockLinkInit(&lock->link,txid);

    lock->locks = NULL;
    lock->locks_ln = NULL;
    lock->db = db;
    if (key) incrRefCount(key);
    lock->key = key;
    lock->c = c;
    lock->proceed = proceed;
    lock->pd = pd;
    lock->pdfree = pdfree;
    lock->lock_timer = 0;
    lock->conflict = 0;
    if (server.storage.swap_debug_trace_latency) {
        lock->start_time = ustime();
    } else {
        lock->start_time = 0;
    }

    UNUSED(msgs);
#ifdef SWAP_DEBUG
    lock->msgs = msgs;
#endif

    return lock;
}

static int _lockLock(int *would_block,
        int64_t txid, redisDb *db, robj *key, lockProceedCallback cb,
        client *c, void *pd, freefunc pdfree, void *msgs) {
    lock *lock = lockNew(txid,db,key,c,cb,pd,pdfree,msgs);
    locks *svrlocks = server.storage.swap_lock->svrlocks, *dblocks, *keylocks, *locks;

    locksLinkLock(svrlocks,lock,would_block);
    if (db == NULL) {
        locks = svrlocks;
        goto end;
    }

    dblocks = svrlocks->svr.dbs[db->id];
    locksLinkLock(dblocks,lock,would_block);
    if (key == NULL) {
        locks = dblocks;
        goto end;
    }

    keylocks = dictFetchValue(dblocks->db.keys,key);
    if (keylocks == NULL) {
        if (would_block == NULL) {
            keylocks = locksCreate(REQUEST_LEVEL_KEY,db,key,dblocks);
        } else {
            /* keylocks will remain NULL if testing would block. */
        }
    } else {
        serverAssert(locksLastLock(keylocks)!= NULL);
    }
    locksLinkLock(keylocks,lock,would_block);
    locks = keylocks;

end:
    locksChildrenLinkLock(locks,lock,would_block);

    if (would_block == NULL) {
        lockAttachToLocks(lock, locks);
#ifdef SWAP_DEBUG
        sds dump = locksDump(locks);
        int conflict = !lockLinkTargetReady(&lock->link.target);
        DEBUG_MSGS_APPEND(msgs,"lock","locks = %s, conflict=%d",dump,conflict);
        sdsfree(dump);
#endif
        return lockProceedIfReady(lock);
    } else {
        lockFree(lock);
        return 0;
    }
}
int lockLock(int64_t txid, redisDb *db, robj *key, lockProceedCallback cb, client *c, void *pd, freefunc pdfree, void *msgs) {
    return _lockLock(NULL, txid, db, key, cb, c, pd, pdfree, msgs);
}

void keylocksNewAux(void *_locks) {
    locks *locks = _locks;
    locks->lock_list = listCreate();
}

void keylocksFreeAux(void *_locks) {
    locks *locks = _locks;
    serverAssert(locks->level == REQUEST_LEVEL_KEY);
    serverAssert(listLength(locks->lock_list) == 0);
    listRelease(locks->lock_list);
    locks->lock_list = NULL;
}

static void lockStatInitCumulative(lockCumulativeStat *cumu_stat) {
    cumu_stat->request_count = 0;
    cumu_stat->conflict_count = 0;
}
static void lockStatFreeInstantaneou(lockInstantaneouStat *stat) {
    lock_free(stat);
}

static lockInstantaneouStat *lockStatCreateInstantaneou() {
    int i, metric_offset, j;
    lockInstantaneouStat *inst_stats = lock_malloc(REQUEST_LEVEL_TYPES*sizeof(lockInstantaneouStat));
    for (i = 0; i < REQUEST_LEVEL_TYPES; i++) {
        /* TODO 后续使用的话 需要对stats_metric_idx_request，stats_metric_idx_conflict，stats_metric_idx_wait_time，stats_metric_idx_proceed_count 进行矫正*/
        metric_offset = i * SWAP_LOCK_METRIC_SIZE;
        inst_stats[i].name = requestLevelName(i);
        inst_stats[i].request_count = 0;
        inst_stats[i].conflict_count = 0;
        inst_stats[i].proceed_count = 0;
        inst_stats[i].wait_time = 0;
        inst_stats[i].wait_time_max_index = 0;
        for (j = 0;j < STATS_METRIC_SAMPLES;j++) {
            inst_stats[i].wait_time_maxs[j] = 0;
        }
        inst_stats[i].stats_metric_idx_request = metric_offset+SWAP_LOCK_METRIC_REQUEST;
        inst_stats[i].stats_metric_idx_conflict = metric_offset+SWAP_LOCK_METRIC_CONFLICT;
        inst_stats[i].stats_metric_idx_wait_time = metric_offset+SWAP_LOCK_METRIC_WAIT_TIME;
        inst_stats[i].stats_metric_idx_proceed_count= metric_offset+SWAP_LOCK_METRIC_PROCEED_COUNT;
    }
    return inst_stats;
}

void lockStatInit(lockStat *stat) {
    lockStatInitCumulative(&stat->cumulative);
    stat->instant = lockStatCreateInstantaneou();
}

void swapLockCreate() {
    int i;

    buffered_allocator_lock = bufferedAllocatorCreate(
            BUFFERED_ALLOCATOR_CAPACITY_LOCK,sizeof(struct lock),NULL,NULL);
    buffered_allocator_keylocks = bufferedAllocatorCreate(
            BUFFERED_ALLOCATOR_CAPACITY_KEYLOCKS,sizeof(struct locks),
            keylocksNewAux,keylocksFreeAux);

    locks *svrlocks = locksCreate(REQUEST_LEVEL_SVR,NULL,NULL,NULL);
    for (i = 0; i < svrlocks->svr.dbnum; i++) {
        redisDb *db = server.db + i;
        svrlocks->svr.dbs[i] = locksCreate(REQUEST_LEVEL_DB,db,NULL,svrlocks);
    }

    lockStat *stat = lock_malloc(sizeof(lockStat));
    lockStatInit(stat);

    server.storage.swap_lock = lock_malloc(sizeof(struct swapLock));
    server.storage.swap_lock->svrlocks = svrlocks;
    server.storage.swap_lock->stat = stat;
}

void lockProceedByLink(lockLink *link, void *pd) {
    lock *lock = LINK_TO_LOCK(link);
    UNUSED(pd);
    lockProceed(lock);
}

static inline void lockDetachFromLocks(lock *lock) {
    locks *locks = lock->locks;
    lock->locks = NULL;
    listDelNode(locks->lock_list,lock->locks_ln);
    lock->locks_ln = NULL;
}

static void locksRelease(locks *locks) {
    if (!locks) return;

    serverAssert(listLength(locks->lock_list) == 0);

    switch (locks->level) {
    case REQUEST_LEVEL_SVR:
        lock_free(locks->svr.dbs);
        listRelease(locks->lock_list), locks->lock_list = NULL;
        lock_free(locks);
        break;
    case REQUEST_LEVEL_DB:
        dictRelease(locks->db.keys);
        listRelease(locks->lock_list), locks->lock_list = NULL;
        lock_free(locks);
        break;
    case REQUEST_LEVEL_KEY:
        serverAssert(locks->parent->level == REQUEST_LEVEL_DB);
        dictDelete(locks->parent->db.keys,locks->key.key);
        decrRefCount(locks->key.key);
        bufferedAllocatorFree(buffered_allocator_keylocks,locks);
        break;
    default:
        serverPanic("unexpected lock level");
        break;
    }
}

static inline void locksFreeIfEmptyKeyLevel(locks *locks) {
    if (locks->level == REQUEST_LEVEL_KEY &&
            listLength(locks->lock_list) == 0) {
        locksRelease(locks);
    }
}


static void lockStatUpdateUnlocked(lock *lock) {
    lockCumulativeStat *cumu_stat = &server.storage.swap_lock->stat->cumulative;
    cumu_stat->request_count--;
    if (lock->conflict) cumu_stat->conflict_count--;
}
/* callback when link target is ready. */
typedef void (*linkProceed)(struct lockLink *link, void *pd);
#define LINK_SIGNAL_PROCEEDED 0
#define LINK_SIGNAL_UNLOCK 1

static void lockLinkSignal(lockLink *link, int type, linkProceed cb,
        void *pd) {
    if (type == LINK_SIGNAL_PROCEEDED) {
        serverAssert(!link->links.proceeded && !link->links.unlocked);
        link->links.proceeded = 1;
    } else {
        serverAssert(type == LINK_SIGNAL_UNLOCK);
        serverAssert(link->links.proceeded);
        link->links.unlocked = 1;
    }

    for (int i = 0; i < link->links.count; i++) {
        lockLink *to = link->links.links[i];
        serverAssert(link->txid <= to->txid);
        if ((type == LINK_SIGNAL_PROCEEDED && link->txid == to->txid) ||
                (type == LINK_SIGNAL_UNLOCK && link->txid < to->txid)) {
            lockLinkTargetSignaled(&to->target);
            if (lockLinkTargetReady(&to->target)) {
                cb(to,pd);
            }
        }
    }
}

void lockLinkUnlock(lockLink *link, linkProceed cb, void *pd) {
    lockLinkSignal(link,LINK_SIGNAL_UNLOCK,cb,pd);
}
void lockUnlock(void *lock_) {
    lock *lock = lock_;
    locks *locks = lock->locks;
    lockDetachFromLocks(lock);
    locksFreeIfEmptyKeyLevel(locks);
    lockLinkUnlock(&lock->link,lockProceedByLink,NULL);
    lockStatUpdateUnlocked(lock);
    lockFree(lock);
}


/* lockProceeded */
void lockLinkProceeded(lockLink *link, linkProceed cb, void *pd) {
    lockLinkSignal(link,LINK_SIGNAL_PROCEEDED,cb,pd);
}

void lockProceeded(void *lock_) {
    lock *lock = lock_;
    lockLinkProceeded(&lock->link,lockProceedByLink,NULL);
}


int lockWouldBlock(int64_t txid, redisDb *db, robj *key) {
    int would_block = 0;
    _lockLock(&would_block,txid,db,key,NULL,NULL,NULL,NULL,NULL);
    return would_block;
}