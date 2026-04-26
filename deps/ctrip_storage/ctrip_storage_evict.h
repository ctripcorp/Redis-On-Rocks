
#ifndef __CTRIP_STORAGE_EVICT_H__
#define __CTRIP_STORAGE_EVICT_H__

#include <stdint.h>
#include <stdlib.h>

/* Evict */
#define EVICT_SUCC_SWAPPED      0
#define EVICT_SUCC_FREED        1
#define EVICT_FAIL_ABSENT       2
#define EVICT_FAIL_EVICTED      3
#define EVICT_FAIL_SWAPPING     4
#define EVICT_FAIL_UNSUPPORTED  5
#define EVICT_RESULT_TYPES      6
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

#define EVICT_ASAP_OK 0
#define EVICT_ASAP_AGAIN 1

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

int swapEvictGetInprogressLimit(size_t mem_tofree);
int swapEvictionReachedInprogressLimit(); 


swapEvictionCtx *swapEvictionCtxCreate();

/* Wait for async operations to complete */
void ctripStorageWaitCommand(struct client *c);

#endif /* __CTRIP_STORAGE_EVICT_H__ */