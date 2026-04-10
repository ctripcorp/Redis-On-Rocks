
#ifndef __CTRIP_STORAGE_DEBUG_H__
#define __CTRIP_STORAGE_DEBUG_H__

/* swapExecBatch 完整定义在 ctrip_storage_batch.h，此处仅需前向声明（用于函数参数指针） */
typedef struct swapExecBatch swapExecBatch;

#define SWAP_DEBUG_LOCK_WAIT            0
#define SWAP_DEBUG_SWAP_QUEUE_WAIT      1
#define SWAP_DEBUG_NOTIFY_QUEUE_WAIT    2
#define SWAP_DEBUG_NOTIFY_QUEUE_HANDLES 3
#define SWAP_DEBUG_NOTIFY_QUEUE_HANDLE_TIME 4
#define SWAP_DEBUG_INFO_TYPE            5


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

#endif /* __CTRIP_STORAGE_DEBUG_H__ */