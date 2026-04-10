#ifndef __CTRIP_STORAGE_SCAN_H__
#define __CTRIP_STORAGE_SCAN_H__
#include "server.h"
#include "ctrip_storage_expire.h"
typedef struct swapScanSession {
    time_t last_active;
    unsigned long session_id; /* inner */
    sds nextseek;
    unsigned long nextcursor; /* inner cursor */
    int binded;
} swapScanSession;

typedef struct swapScanSessionsStat {
    uint64_t assigned_failed;
    uint64_t assigned_succeded;
    uint64_t bind_failed;
    uint64_t bind_succeded;
} swapScanSessionsStat;

typedef struct swapScanSessions {
    rax *assigned;
    list *free;
    swapScanSession *array;
    swapScanSessionsStat stat;
} swapScanSessions;


#define cursorIsHot(outer_cursor) ((outer_cursor & 0x1UL) == 0)
#define cursorOuterToInternal(outer_cursor) (outer_cursor >> 1)
#define cursorInternalToOuter(outer_cursor, cursor) (cursor << 1 | (outer_cursor & 0x1UL))

static inline unsigned long cursorGetSessionId(unsigned long outer_cursor) {
    return cursorOuterToInternal(outer_cursor) & ((1<<server.storage.swap_scan_session_bits)-1);
}

static inline unsigned long cursorGetSessionSeq(unsigned long outer_cursor) {
    return cursorOuterToInternal(outer_cursor) >> server.storage.swap_scan_session_bits;
}
static inline uint64_t sessionId2RaxKey(unsigned long session_id) {
    return htonu64(session_id);
}

static inline unsigned long swapScanSessionGetNextCursor(swapScanSession *session) {
    return session->nextcursor;
}

static inline void swapScanSessionIncrNextCursor(swapScanSession *session) {
    session->nextcursor += 1 << server.storage.swap_scan_session_bits;
}

static inline void swapScanSessionZeroNextCursor(swapScanSession *session) {
    session->nextcursor = session->session_id;
}

static inline int swapScanSessionFinished(swapScanSession *session) {
    return session->nextseek == NULL;
}

/* ==================== MetaScan 结构体（从 object_meta.h 迁移） ==================== */

/* metaScan 扫描元数据 */
#define DEFAULT_SCANMETA_BUFFER 16

typedef struct scanMeta {
  sds key;
  long long expire;
  int swap_type;
} scanMeta;
void scanMetaInit(scanMeta *meta, int swap_type, sds key, long long expire);

/* metaScan 结果集 */
typedef struct metaScanResult {
  scanMeta buffer[DEFAULT_SCANMETA_BUFFER];
  scanMeta *metas;
  int num;
  int size;
  sds nextseek;
} metaScanResult;
metaScanResult *metaScanResultCreate();

/* metaScan 数据上下文类型接口（虚函数表） */
struct metaScanDataCtx;

typedef struct metaScanDataCtxType {
    void (*swapAna)(struct metaScanDataCtx *datactx, int *intention, uint32_t *intention_flags);
    void (*swapIn)(struct metaScanDataCtx *datactx, metaScanResult *result);
    void (*freeExtend)(struct metaScanDataCtx *datactx);
} metaScanDataCtxType;

/* metaScan 数据上下文 */
typedef struct metaScanDataCtx {
    metaScanDataCtxType *type;
    client *c;
    int limit;
    sds seek;
    void *extend;
} metaScanDataCtx;

/* metaScan 数据上下文 - 过期扫描 */
typedef struct metaScanDataCtxScanExpire {
    scanExpire *scan_expire;
} metaScanDataCtxScanExpire;

/* metaScan 数据上下文 - 随机键 */
typedef struct metaScanDataCtxRandomkey {
  redisDb *db;
} metaScanDataCtxRandomkey;

/* metaScan 数据上下文 - Scan 命令 */
typedef struct metaScanDataCtxScan {
    swapScanSession *session;
} metaScanDataCtxScan;

#endif /* __CTRIP_STORAGE_SCAN_H__ */