

#ifndef __CTRIP_STORAGE_LOCK_H
#define __CTRIP_STORAGE_LOCK_H 
#include "ctrip_storage_request.h"

#define REQUEST_LEVEL_SVR  0
#define REQUEST_LEVEL_DB   1
#define REQUEST_LEVEL_KEY  2
#define REQUEST_LEVEL_TYPES  3


/* SWAP_LOCK_METRIC */
#define SWAP_LOCK_METRIC_REQUEST 0
#define SWAP_LOCK_METRIC_CONFLICT 1
#define SWAP_LOCK_METRIC_WAIT_TIME 2
#define SWAP_LOCK_METRIC_PROCEED_COUNT 3
#define SWAP_LOCK_METRIC_SIZE 4




typedef void (*freefunc)(void *);
typedef void (*lockProceedCallback)(void *lock, int flush, redisDb *db, robj *key, client *c, void *pd);


typedef struct lockLinkTarget {
  int signaled;
  int linked;
} lockLinkTarget;

#define LOCK_LINKS_BUF_SIZE 2
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

int lockLock(int64_t txid, redisDb *db, robj *key, lockProceedCallback cb, client *c, void *pd, freefunc pdfree, void *msgs);
void lockUnlock(void *lock_);
void lockProceeded(void *lock_);
int lockWouldBlock(int64_t txid, redisDb *db, robj *key);

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

#endif