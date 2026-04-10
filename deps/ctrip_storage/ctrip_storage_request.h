

#ifndef __CTRIP_STORAGE_REQUEST_H__
#define __CTRIP_STORAGE_REQUEST_H__
#include <inttypes.h>
#include "server.h"
#include "ctrip_storage_trace.h"
#include "ctrip_storage_object_meta.h"
#include "buffered_allocator.h"
#include "ctrip_storage_objects.h"
#include "ctrip_storage_error.h"
#include "ctrip_storage_utils.h"

/* ==================== 异步/同步请求队列（从 types.h 迁移） ==================== */

/* 异步完成队列，IO 线程通过 eventfd 通知主线程处理完成的请求 */
typedef struct asyncCompleteQueue {
    int eventfd;                /* eventfd 文件描述符，用于线程间通知 */
    pthread_mutex_t lock;       /* 互斥锁，保护 complete_queue */
    list *complete_queue;       /* 已完成请求的链表 */
} asyncCompleteQueue;

/* 并行同步模式上下文 */
typedef struct parallelSync {
    list *entries;              /* swapEntry 链表 */
    int parallel;               /* 并行度 */
    int mode;                   /* 模式 */
} parallelSync;

#define SWAP_ANA_THD_MAIN 0
#define SWAP_ANA_THD_SWAP 1

/* Cmd */
#define REQUEST_LEVEL_SVR  0
#define REQUEST_LEVEL_DB   1
#define REQUEST_LEVEL_KEY  2
#define REQUEST_LEVEL_TYPES  3

/* REQUEST TYPE */
#define KEYREQUEST_TYPE_KEY    0
#define KEYREQUEST_TYPE_SUBKEY 1
#define KEYREQUEST_TYPE_RANGE  2
#define KEYREQUEST_TYPE_SCORE  3
#define KEYREQUEST_TYPE_SAMPLE 4
#define KEYREQUEST_TYPE_BTIMAP_OFFSET  5
#define KEYREQUEST_TYPE_BTIMAP_RANGE  6




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
void clientGotLock(client *c, swapCtx *ctx, void *lock);
void clientReleaseLocks(client *c, swapCtx *ctx);
void swapCtxSetSwapData(swapCtx *ctx, MOVE swapData *data, MOVE void *datactx);

/* Exec */
#define SWAP_MODE_ASYNC 0
#define SWAP_MODE_PARALLEL_SYNC 1

struct swapRequestBatch;

typedef void (*swapRequestFinishedCallback)(swapData *data, void *pd, int errcode);

#define SWAP_REQUEST_TYPE_DATA 0
#define SWAP_REQUEST_TYPE_META 0

/* swapRequest 定义 */
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
        uint32_t intention_flags, swapCtx *ctx, swapData *data,
        void *datactx,swapTrace *trace,
        swapRequestFinishedCallback cb, void *pd, void *msgs);
void swapRequestFree(swapRequest *req);

/* swapRequestBatch 接口定义 */
struct swapRequestBatch;
typedef void (*swapRequestBatchNotifyCallback)(struct swapRequestBatch *req, void *pd);



/* 请求批量 */
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
swapRequestBatch *swapRequestBatchNew();
void swapRequestBatchFree(swapRequestBatch *reqs);
void swapRequestBatchAppend(swapRequestBatch *reqs, swapRequest *req);
void swapRequestBatchExecute(swapRequestBatch *reqs);
void swapRequestBatchProcess(swapRequestBatch *reqs);
void swapRequestBatchCallback(swapRequestBatch *reqs);
void swapRequestBatchDispatched(swapRequestBatch *reqs);
void swapRequestBatchStart(swapRequestBatch *reqs);
void swapRequestBatchEnd(swapRequestBatch *reqs);
static inline int swapIntentionInOutDel(int intention) {
    return intention == SWAP_IN || intention == SWAP_OUT ||
            intention == SWAP_DEL;
}

#define MAX_KEYREQUESTS_BUFFER 8
#define GET_KEYREQUESTS_RESULT_INIT { {{0}}, NULL, NULL, 0, MAX_KEYREQUESTS_BUFFER}
typedef struct getKeyRequestsResult {
	keyRequest buffer[MAX_KEYREQUESTS_BUFFER];
	keyRequest *key_requests;
	swapCmdTrace *swap_cmd;
	int num;
	int size;
} getKeyRequestsResult;

void getKeyRequests(client *c, struct getKeyRequestsResult *result);
void releaseKeyRequests(getKeyRequestsResult *result);
int submitNormalClientRequests(client *c);
int submitReplClientRequests(client *c);
void submitClientKeyRequests(client *c, getKeyRequestsResult *result,
                             clientKeyRequestFinished cb, void* ctx_pd);
void keyRequestBeforeCall(client *c, swapCtx *ctx);
typedef void (*freefunc)(void *);


void swapCmdSwapSubmitted(swapCmdTrace *swap_cmd);

/* swapBatchCtxStat, swapBatchCtx, swapExecBatch 已迁移到 ctrip_storage_batch.h */


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

/* swap type */
static inline const char *swapIntentionName(int intention) {
  const char *name = "?";
  const char *intentions[] = {"NOP", "IN", "OUT", "DEL", "UTILS"};
  if (intention >= 0 && intention < SWAP_TYPES)
    name = intentions[intention];
  return name;
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



/*meta scan*/
int swapDataSetupMetaScan(swapData *data, uint32_t intention_flags,
        client *c, void **pdatactx);

extern bufferedAllocator *buffered_allocator_swapctx;
extern bufferedAllocator *buffered_allocator_swapdata;
void initSwapRequest() ;


/* callback */
void swapRequestBatchCallback(swapRequestBatch *reqs);
/* async request */
int asyncCompleteQueueProcess(asyncCompleteQueue *cq) ;
int asyncCompleteQueueInit(void);
void asyncCompleteQueueDeinit(asyncCompleteQueue *cq);
int asyncCompleteQueueDrain(mstime_t time_limit);

void asyncCompleteQueueAppend(asyncCompleteQueue *cq, swapRequestBatch *reqs);
void asyncSwapRequestBatchSubmit(swapRequestBatch *reqs, int idx);
/* sync request */
/* Parallel sync */
typedef struct {
    int inprogress;         /* swap entry in progress? */
    int pipe_read_fd;       /* read end to wait rio swap finish. */
    int pipe_write_fd;      /* write end to notify swap finish by rio. */
    swapRequestBatch *reqs;
} swapEntry;

int parallelSyncSwapRequestBatchSubmit(swapRequestBatch *reqs, int idx);



/* keyRequest */
static inline int isMetaScanRequest(uint32_t intention_flag) {
    return (intention_flag & SWAP_METASCAN_SCAN) ||
           (intention_flag & SWAP_METASCAN_RANDOMKEY) ||
           (intention_flag & SWAP_METASCAN_EXPIRE);
}
static inline int isSwapHitStatKeyRequest(keyRequest *kr) {
    return kr && kr->cmd_intention == SWAP_IN &&
        !isMetaScanRequest(kr->cmd_intention_flags);
}

void getKeyRequestsFreeResult(getKeyRequestsResult *result);
#endif /* __CTRIP_STORAGE_REQUEST_H__ */