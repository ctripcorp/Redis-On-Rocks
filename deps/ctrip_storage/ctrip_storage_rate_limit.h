
#ifndef __CTRIP_STORAGE_RATE_LIMIT_H__
#define __CTRIP_STORAGE_RATE_LIMIT_H__

#include "ctrip_storage.h"
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
void swapRatelimitStart(swapRatelimitCtx *rlctx, client *c);
int swapRateLimitReject(swapRatelimitCtx *rlctx, client *c);
void swapRateLimitPause(swapRatelimitCtx *rlctx, client *c);
mstime_t swapPersistCtxLag(swapPersistCtx *ctx);

#endif /* __CTRIP_STORAGE_RATE_LIMIT_H__ */