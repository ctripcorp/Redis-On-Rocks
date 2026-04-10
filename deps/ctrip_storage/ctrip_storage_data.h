#ifndef ___CTRIP_STORAGE_DATA_H_
#define ___CTRIP_STORAGE_DATA_H_

#include "server.h"
#include "ctrip_storage_object_meta.h"
#include "ctrip_storage_trace.h"
#define SWAP_UNSET -1
#define SWAP_NOP    0
#define SWAP_IN     1
#define SWAP_OUT    2
#define SWAP_DEL    3
#define SWAP_UTILS  4
#define SWAP_TYPES  5

typedef struct swapDataAbsentSubkey {
  size_t count;
  size_t capacity;
  sds *subkeys;
} swapDataAbsentSubkey;
/* argRewriteRequest */
typedef struct argRewriteRequest {
  int mstate_idx; /* >=0 if current command is a exec, means index in mstate; -1 means req not in multi/exec */
  int arg_idx; /* index of argument to use for rewrite func */
} argRewriteRequest;
static inline void argRewriteRequestInit(argRewriteRequest *arg_req) {
  arg_req->mstate_idx = -1;
  arg_req->arg_idx = -1;
}

/* Both start and end are inclusive, see addListRangeReply for details. */
typedef struct range {
  long long start;
  long long end;
  int reverse; /* LTRIM command specifies range to keep, so swap in the reverse range */
} range;

typedef struct keyRequest{
  int dbid;
  int level;
  int cmd_intention;
  int cmd_intention_flags;
  uint64_t cmd_flags;
  int type; /* request type */
  int deferred;
  robj *key;
  union {
    struct {
      int num_subkeys;
      robj **subkeys;
    } b; /* subkey: hash, set */
    struct {
      int num_ranges;
      range *ranges;
    } l; /* range: list */
    struct {
      zrangespec* rangespec;
      int reverse;
      int limit;
    } zs; /* zset score*/
    struct {
      int count;
    } sp; /* sample */
    struct {
      long long offset;
    } bo; /* bitmap offset*/
    struct {
      long long start;
      long long end;
    } br; /* bitmap range*/
  };
  argRewriteRequest arg_rewrite[2];
  swapCmdTrace *swap_cmd;
  swapTrace *trace;
} keyRequest;


/* SwapData represents key state when swap start. It is stable during
 * key swapping, misc dynamic data are save in dataCtx. */
typedef struct swapData {
  struct swapDataType *type;
  struct objectMetaType *omtype;
  redisDb *db;
  robj *key; /*own*/
  robj *value; /*own*/
  long long expire;
  objectMeta *object_meta; /* ref */
  objectMeta *cold_meta; /* own, moved from exec */
  objectMeta *new_meta; /* own */
  int swap_type;
  unsigned propagate_expire:1;
  unsigned set_dirty:1;
  unsigned set_dirty_meta:1;
  unsigned persistence_deleted:1;
  unsigned set_persist_keep:1;
  unsigned reserved:27;
  sds nextseek; /* own, moved from exec */
  swapDataAbsentSubkey *absent;
  robj *dirty_subkeys;
  void *extends[2];
} swapData;
static inline objectMeta *swapDataObjectMeta(swapData *d) {
    serverAssert(
        !(d->object_meta && d->new_meta) ||
        !(d->object_meta && d->cold_meta) ||
        !(d->new_meta && d->cold_meta));

    if (d->object_meta) return d->object_meta;
    if (d->cold_meta) return d->cold_meta;
    return d->new_meta;
}

swapData *createSwapData(redisDb *db, robj *key, robj *value, robj* dirty_subkeys);
int swapDataSetupMeta(swapData *d, int swap_type, long long expire, OUT void **datactx);
static inline int swapDataAlreadySetup(swapData *data) {
    return data->type != NULL;
}
void swapDataMarkPropagateExpire(swapData *data);
int swapDataAna(swapData *d, int thd, struct keyRequest *key_request, int *intention, uint32_t *intention_flag, void *datactx);
int swapDataSwapAnaAction(swapData *data, int intention, void *datactx_, int *action);
sds swapDataEncodeMetaKey(swapData *d);
sds swapDataEncodeMetaVal(swapData *d, void *datactx);
int swapDataEncodeKeys(swapData *d, int intention, void *datactx, int *num, int **cfs, sds **rawkeys);
int swapDataEncodeData(swapData *d, int intention, void *datactx, int *num, int **cfs, sds **rawkeys, sds **rawvals);
int swapDataEncodeRange(struct swapData *data, int intention, void *datactx_, int *limit, uint32_t *flags, int *pcf, sds *start, sds *end);
int swapDataDecodeAndSetupMeta(swapData *d, sds rawval, OUT void **datactx);
int swapDataDecodeData(swapData *d, int num, int *cfs, sds *rawkeys, sds *rawvals, void **decoded);
int swapDataSwapIn(swapData *d, void **result, void *datactx);
int swapDataSwapOut(swapData *d, void *datactx, int keep_data, OUT int *totally_out);
int swapDataSwapDel(swapData *d, void *datactx, int async);
void *swapDataCreateOrMergeObject(swapData *d, MOVE void *decoded, void *datactx);
int swapDataCleanObject(swapData *d, void *datactx, int keep_data);
int swapDataBeforeCall(swapData *d, keyRequest *key_request, client *c, void *datactx);
int swapDataKeyRequestFinished(swapData *data);
char swapDataGetObjectAbbrev(robj *value);
void swapDataFree(swapData *data, void *datactx);
int swapDataMergedIsHot(swapData *d, void *result, void *datactx);
void swapDataRetainAbsentSubkeys(swapData *data, int num, int *cfs, sds *rawkeys, sds *rawvals);
void swapDataMergeAbsentSubkey(swapData *data);
int swapDataMayContainSubkey(swapData *data, int thd, robj *subkey);
void *swapDataGetObjectMetaAux(swapData *data, void *datactx);
static inline int swapDataIsCold(swapData *data) {
  return data->value == NULL;
}
void swapDataTurnWarmOrHot(swapData *data);
void swapDataTurnCold(swapData *data);
void swapDataTurnDeleted(swapData *data,int del_skip);


typedef struct swapDataType {
  char* name;
  uint64_t cmd_swap_flags;
  int (*swapAna)(struct swapData *data, int thd, struct keyRequest *key_request, OUT int *intention, OUT uint32_t *intention_flags, void *datactx);
  int (*swapAnaAction)(struct swapData *data, int intention, void *datactx, OUT int *action);
  int (*encodeKeys)(struct swapData *data, int intention, void *datactx, OUT int *num, OUT int **cfs, OUT sds **rawkeys);
  int (*encodeRange)(struct swapData *data, int intention, void *datactx, OUT int *limit, OUT uint32_t *flags, OUT int *cf, OUT sds *start, OUT sds *end);
  int (*encodeData)(struct swapData *data, int intention, void *datactx, OUT int *num, OUT int **cfs, OUT sds **rawkeys, OUT sds **rawvals);
  int (*decodeData)(struct swapData *data, int num, int *cfs, sds *rawkeys, sds *rawvals, OUT void **decoded);
  int (*swapIn)(struct swapData *data, MOVE void **result, void *datactx);
  int (*swapOut)(struct swapData *data, void *datactx, int keep_data, OUT int *totally_out);
  int (*swapDel)(struct swapData *data, void *datactx, int async);
  void *(*createOrMergeObject)(struct swapData *data, MOVE void *decoded, void *datactx);
  int (*cleanObject)(struct swapData *data, void *datactx, int keep_data);
  int (*beforeCall)(struct swapData *data, keyRequest *key_request, client *c, void *datactx);
  void (*free)(struct swapData *data, void *datactx);
  int (*rocksDel)(struct swapData *data_,  void *datactx_, int inaction, int num, int* cfs, sds *rawkeys, sds *rawvals, OUT int *outaction, OUT int *outnum, OUT int** outcfs,OUT sds **outrawkeys);
  int (*mergedIsHot)(struct swapData *data, MOVE void *result, void *datactx);
  void* (*getObjectMetaAux)(struct swapData *data, void *datactx);
} swapDataType;
#endif /* ___CTRIP_STORAGE_DATA_H_ */