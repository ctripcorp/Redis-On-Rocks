
#ifndef __CTRIP_STORAGE_CLIENT_H__
#define __CTRIP_STORAGE_CLIENT_H__
#include "server.h"
#include "ctrip_storage_data.h"
#define SWAP_LOCK_UNIQUE  0  /* Client will submit/lock/proceed/unlock one tx at a time, typically used by normal and repl worker client. */
#define SWAP_LOCK_SHARED  1  /* Client may submit another tx event if previous tx is in flight, typicall used by evict client. */

/* ==================== Deferred Command（从 types.h 迁移） ==================== */

/* 延迟命令状态枚举 */
typedef enum {
    DEFERRED_STATE_NONE,
    DEFERRED_STATE_PENDING,
    DEFERRED_STATE_READY
} deferredState;

/* 客户端延迟标志位 */
#define CLIENT_DEFERRED_SWAPPING (1ULL<<1)  /* The client is swapping. */
#define CLIENT_DEFERRED_UNLOCKING (1ULL<<2) /* Client is releasing swap lock. */
#define CLIENT_DEFERRED_REWINDING (1ULL<<3) /* The client is waiting rewind. */
#define CLIENT_DEFERRED_DISCARD_CACHED_MASTER (1ULL<<4)
#define CLIENT_DEFERRED_SHIFT_REPL_ID (1ULL<<5)
#define CLIENT_DEFERRED_DONT_RECONNECT_MASTER (1ULL<<6)
/* swap 多命令状态（MULTI/EXEC 中每条命令的 swap 状态） */
struct swapMstate {
    struct redisCommand *cmd;
    struct swapCmdTrace *swap_cmd;
};

/* 延迟命令结构体，管理命令的 swap 生命周期 */
struct deferredCommand {
    deferredState state;        /* 当前命令的状态 */
    long long flags;            /* 标志位，记录命令的属性 */
    int keyrequests_count;
    struct swapCmdTrace *swap_cmd;
    long swap_duration;         /* microseconds used in swap */
    int swap_result;
    int swap_lock_mode;
    int CLIENT_DEFERED_CLOSING;
    int CLIENT_REPL_SWAPPING;
    long long swap_cmd_reploff;
    struct client *swap_repl_client;
    list* swap_locks;
    struct metaScanResult *swap_metas;
    int swap_errcode;
    struct argRewrites *swap_arg_rewrites;
    int rate_limit_event_id;
    swapMstate *mstates;
};

/* swap 阻塞上下文，管理 swap 依赖阻塞 */
struct swapUnblockCtx {
  long long version;
  struct client** mock_clients;
  /* status */
  long long swap_total_count;
  long long swapping_count;
  long long swap_retry_count;
  long long swap_err_count;
};

/* ==================== ArgRewrite ==================== */

/** argRewrite */
typedef struct argRewrite {
  argRewriteRequest arg_req;
  robj *orig_arg; /* own */
} argRewrite;

#define ARG_REWRITES_MAX 2
typedef struct argRewrites {
  int num;
  argRewrite rewrites[ARG_REWRITES_MAX];
} argRewrites;
// void initDeferredCommand(client* c);
// void resetDeferredCommand(client *c);

#endif /* __CTRIP_STORAGE_CLIENT_H__ */
