

#ifndef __CTRIP_STORAGE_OBJECTS_H__
#define __CTRIP_STORAGE_OBJECTS_H__

#include "ctrip_storage_data.h"
#include "roaring_bitmap.h"
#include "ctrip_storage_persist.h"
#include "ctrip_storage_commands.h"

/* Object meta */
#define SWAP_VERSION_ZERO 0
#define SWAP_VERSION_MAX  UINT64_MAX

#define BIG_DATA_CTX_FLAG_NONE 0
#define BIG_DATA_CTX_FLAG_MOCK_VALUE (1U<<0)

#define BASE_SWAP_CTX_TYPE_SUBKEY 0
#define BASE_SWAP_CTX_TYPE_SAMPLE 1

#define ZSET_SWAP_CTX_TYPE_NONE 0
#define ZSET_SWAP_CTX_TYPE_ZS 1





/*big data*/
typedef struct baseBigDataCtx {
    int type;
    union {
      struct {
        int num;
        robj **subkeys;
      } sub;
      struct {
        int count;
      } spl;
    };
    int ctx_flag;
} baseBigDataCtx;


/*bitmap*/
/* Meta bitmap */
/* meta != NULL, bitmap with hole, which means cold subkey, it is not entire bitmap in memory.
 * meta == NULL,  no hole in bitmap, it is entire bitmap in memory. */
typedef struct bitmapMeta {
    size_t subkey_size;
    size_t size;
    int pure_cold_subkeys_num;
    roaringBitmap *subkeys_status;  /* status set to 1 if subkey hot. */
} bitmapMeta;
typedef struct metaBitmap {
    struct bitmapMeta *meta;
    robj *bitmap;
} metaBitmap;
typedef struct bitmapDataCtx {
    int ctx_flag;
    unsigned long subkeys_total_size;  /* only used in swap out */
    unsigned int subkeys_num;
    uint32_t *subkeys_logic_idx;
    metaBitmap new_meta_bitmap; /* own, only used in swap out */
    argRewriteRequest arg_reqs[2];
} bitmapDataCtx;
int swapDataSetupBitmap(swapData *d, void **pdatactx);
objectMeta *createBitmapObjectMarker();
void bitmapMetaTransToMarkerIfNeeded(objectMeta *object_meta);
int bitmapSetObjectMarkerIfNotExist(redisDb *db, robj *key);

/*list*/
typedef struct listDataCtx {
  struct listMeta *swap_meta;
  argRewriteRequest arg_reqs[2];
  int ctx_flag;
} listDataCtx;
int swapDataSetupList(swapData *d, void **pdatactx);

/* zset */
typedef struct zsetDataCtx {
	baseBigDataCtx bdc;
  int type;
  union {
    struct {
      zrangespec* rangespec;
      int reverse;
      int limit;
    } zs;
  };

} zsetDataCtx;
int swapDataSetupZSet(swapData *d, void **pdatactx);

/*set */
typedef struct setDataCtx {
    baseBigDataCtx ctx;
} setDataCtx;
int swapDataSetupSet(swapData *d, OUT void **datactx);

/* hash */
typedef struct hashDataCtx {
    baseBigDataCtx ctx;
} hashDataCtx;

int swapDataSetupHash(swapData *d, OUT void **datactx);
/* whole key*/
typedef struct wholeKeySwapData {
  swapData d;
} wholeKeySwapData;
int swapDataSetupWholeKey(swapData *d, OUT void **pdatactx);



/* utils function */
/* Util */

int swapDataObjectMergedIsHot(swapData *d, void *result, void *datactx);
static inline uint64_t swapGetAndIncrVersion(void) { return server.storage.swap_key_version++; }
objectMeta *lookupMeta(redisDb *db, robj *key);
void dbAddMeta(redisDb *db, robj *key, objectMeta *m);
sds rocksEncodeDataKey(redisDb *db, sds key, uint64_t version, sds subkey);
robj *rocksDecodeValRdb(sds raw);
sds rocksEncodeValRdb(robj *value);

#define ROCKS_KEY_FLAG_NONE 0x0
#define ROCKS_KEY_FLAG_SUBKEY 0x1
#define ROCKS_KEY_FLAG_DELETE 0xff

/* persist request keeps value in memory when maxmemory not reached or
 * data originally not cold (no need to swap in). */
static inline int swapDataPersistKeepData(swapData *d, uint32_t cmd_intention_flags, int may_keep_data) {
  int keep_data = (cmd_intention_flags & SWAP_OUT_PERSIST) &&
         (getObjectPersistKeep(d->value) || cmd_intention_flags&SWAP_OUT_KEEP_DATA) &&
         may_keep_data;
  if (server.storage.swap_persist_enabled && server.storage.swap_persist_ctx) {
    swapPersistStat *stat = &server.storage.swap_persist_ctx->stat;
    if (keep_data)
      stat->keep_data++;
    else
      stat->dont_keep++;
  }
  return keep_data;
}

static inline uint64_t swapDataObjectVersion(swapData *d) {
    objectMeta *object_meta = swapDataObjectMeta(d);
    return object_meta ? object_meta->version : 0;
}
int rocksDecodeDataKey(const char *raw, size_t rawlen, int *dbid,
        const char **key, size_t *keylen, uint64_t *version,
        const char **subkey, size_t *subkeylen);

static inline void swapDataSetColdObjectMeta(swapData *d, MOVE objectMeta *cold_meta) {
    d->cold_meta = cold_meta;
}
#endif /* __CTRIP_STORAGE_OBJECTS_H__ */