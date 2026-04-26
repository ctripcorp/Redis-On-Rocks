
#ifndef __CTRIP_STORAGE_EXPIRE_H__
#define __CTRIP_STORAGE_EXPIRE_H__
#include "server.h"

#define EXPIRESCAN_DEFAULT_LIMIT 32
#define EXPIRESCAN_DEFAULT_CANDIDATES (16*1024)

typedef struct expireCandidates {
    robj *zobj;
    size_t capacity;
} expireCandidates;
typedef struct scanExpire {
    expireCandidates *candidates;
    int inprogress;
    sds nextseek;
    int limit;
    double stale_percent;
    long long stat_estimated_cycle_seconds;
    size_t stat_scan_per_sec;
    size_t stat_expired_per_sec;
    long long stat_scan_time_used;
    long long stat_expire_time_used;
} scanExpire;
scanExpire *scanExpireCreate(void);



#endif /* __CTRIP_STORAGE_EXPIRE_H__ */