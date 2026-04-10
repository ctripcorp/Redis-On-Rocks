/*
 * ctrip_storage_persist.h - 持久化相关结构体和宏（从 objects.h 迁移）
 *
 * 包含：persistingKeyEntry, swapPersistStat, persistingKeys, swapPersistCtx
 *      以及持久化相关的对象属性宏（setObjectDirty, schedulePersistIfNeeded 等）
 */

#ifndef __CTRIP_STORAGE_PERSIST_H__
#define __CTRIP_STORAGE_PERSIST_H__

#include "server.h"

/* ==================== 持久化条目 ==================== */

/* 持久化键条目，记录正在持久化的键信息 */
typedef struct persistingKeyEntry {
    listNode *ln;       /* 在 persistingKeys 链表中的节点 */
    uint64_t version;   /* 版本号 */
    mstime_t mstime;    /* 持久化时间戳 */
    int state;          /* 持久化状态 */
} persistingKeyEntry;
persistingKeyEntry *persistingKeyEntryNew(listNode *ln, uint64_t version, mstime_t mstime);

/* 持久化统计信息 */
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

/* 持久化键集合，分为 todo/doing 两个队列 + 映射表 */
typedef struct persistingKeys {
    list *todo;     /* 待持久化队列 */
    list *doing;    /* 正在持久化队列 */
    dict *map;      /* 键到条目的映射 */
} persistingKeys;

/* 持久化上下文，管理整个持久化流程 */
typedef struct swapPersistCtx {
  int keep;
  uint64_t version;
  persistingKeys **keys; /* one for each db */
  long long inprogress_count; /* current inprogress persist count */
  long long inprogress_limit; /* current inprogress limit */
  swapPersistStat stat;
} swapPersistCtx;
void swapPersistCtxAddKey(swapPersistCtx *ctx, redisDb *db, robj *key);

/* ==================== 对象持久化属性宏 ==================== */

/* 获取/设置对象的 persist_keep 标志 */
#define getObjectPersistKeep(o) ((o) ? o->storage.persist_keep : 0)
#define setObjectPersistKeep(o) do { \
    if (o) o->storage.persist_keep = 1; \
} while(0)

/* 设置对象的脏标志 */
#define setObjectMetaDirty(o) do { \
    if (o) o->storage.dirty_meta = 1; \
} while(0)

#define setObjectDataDirty(o) do { \
    if (o) o->storage.dirty_data = 1; \
} while(0)
#define setObjectDirty(o) do { \
  setObjectMetaDirty(o); \
  setObjectDataDirty(o); \
} while(0)

/* 按需调度持久化 */
#define schedulePersistIfNeeded(dbid,key) do { \
  if (server.storage.swap_persist_enabled) swapPersistCtxAddKey(server.storage.swap_persist_ctx,server.db+dbid,key); \
} while (0)

#define setObjectDirtyPersist(dbid,key,o) do { \
  setObjectDirty(o); \
  schedulePersistIfNeeded(dbid,key); \
} while (0)
static inline void dbSetDirty(redisDb *db, robj *key) {
    kvobj *o = lookupKeyRead(db,key);
    if (o) setObjectDirtyPersist(db->id,key,o);
}

static inline void dbSetMetaDirty(redisDb *db, robj *key) {
    kvobj *o = lookupKeyRead(db,key);
    if (o) setObjectMetaDirty(o);
}

#endif /* __CTRIP_STORAGE_PERSIST_H__ */
