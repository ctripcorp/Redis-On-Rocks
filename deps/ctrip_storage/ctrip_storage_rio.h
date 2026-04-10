

#ifndef __CTRIP_STORAGE_RIO_H__
#define __CTRIP_STORAGE_RIO_H__
#include "server.h"
#include "ctrip_storage_utils.h"

typedef struct RIO {
	int action;
	union {
		struct {
			int numkeys;
      int *cfs;
			sds *rawkeys;
			sds *rawvals;
      int notfound;
		} get, put, del, generic;

    struct {
        int cf;
        uint32_t flags;
        sds start;
        sds end;
        size_t limit;
        int numkeys;
        sds *rawkeys;
        sds *rawvals;
        sds nextseek; /* own */
    } iterate;
	};
  sds err;
  int errcode;
  int oom_check;
  void *privdata;
} RIO;
void RIOInitGet(RIO *rio, int numkeys, int *cfs, sds *rawkeys);
void RIOInitPut(RIO *rio, int numkeys, int *cfs, sds *rawkeys, sds *rawvals);
void RIOInitDel(RIO *rio, int numkeys, int *cfs, sds *rawkeys);
void RIOInitIterate(RIO *rio, int cf, uint32_t flags, sds start, sds end, size_t limit);
void RIODeinit(RIO *rio);
void RIODo(RIO *rio);
static inline int RIOGetError(RIO *rio) {
  return rio->errcode;
}
static inline void RIOSetError(RIO *rio, int errcode, MOVE sds err) {
  serverAssert(rio->errcode == 0 && rio->err == NULL);
  rio->errcode = errcode;
  rio->err = err;
}

/* rios with same type */
typedef struct RIOBatch {
   RIO rio_buf[SWAP_BATCH_DEFAULT_SIZE];
   RIO *rios;
   size_t capacity;
   size_t count;
   int action;
} RIOBatch;

void RIOBatchInit(RIOBatch *rios, int action);
void RIOBatchDeinit(RIOBatch *rios);
RIO *RIOBatchAlloc(RIOBatch *rios);
void RIOBatchDo(RIOBatch *rios);
void RIOBatchUpdateStatsDo(RIOBatch *rios, long duration);
void RIOBatchUpdateStatsDataNotFound(RIOBatch *rios);
size_t RIOEstimatePayloadSize(RIO *rio) ;

#endif /* __CTRIP_STORAGE_RIO_H__ */