

#include "ctrip_storage_expire.h"
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
