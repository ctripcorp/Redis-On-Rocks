

#ifndef __CTRIP_STORAGE_OBJECT_META_H__
#define __CTRIP_STORAGE_OBJECT_META_H__

#include "ctrip_storage_utils.h"
#include "ctrip_storage_scan.h"
#include <stdint.h>

struct objectMeta;

typedef struct objectMetaType {
  sds (*encodeObjectMeta) (struct objectMeta *object_meta, void *aux, int meta_enc_mode);
  int (*decodeObjectMeta) (struct objectMeta *object_meta, const char* extend, size_t extlen);
  int (*objectIsHot)(struct objectMeta *object_meta, robj *value);
  void (*free)(struct objectMeta *object_meta);
  void (*duplicate)(struct objectMeta *dup_meta, struct objectMeta *object_meta);
  int (*equal)(struct objectMeta *oma, struct objectMeta *omb);
  int (*rebuildFeed)(struct objectMeta *rebuild_meta, uint64_t version, const char *subkey, size_t sublen, robj *subval);
} objectMetaType;
extern objectMetaType lenObjectMetaType;
extern objectMetaType listObjectMetaType;
extern objectMetaType bitmapObjectMetaType;

typedef struct objectMeta {
  uint64_t version;
  unsigned swap_type:4;
  union {
    long long len:60;
    unsigned long long ptr:60;
  };
} objectMeta;

objectMeta *createObjectMeta(int swap_type, uint64_t version);
int buildObjectMeta(int swap_type, uint64_t version, const char *extend, size_t extlen, OUT objectMeta **pobject_meta);
objectMeta *dupObjectMeta(objectMeta *object_meta);
void freeObjectMeta(objectMeta *object_meta);
static inline void *objectMetaGetPtr(objectMeta *object_meta) {
  return (void*)(unsigned long long)object_meta->ptr;
}
static inline void objectMetaSetPtr(objectMeta *object_meta, void *ptr) {
  object_meta->ptr = (unsigned long long)ptr;
}

/* swap object meta */
typedef struct swapObjectMeta {
  objectMetaType *omtype;
  objectMeta *object_meta;
  objectMeta *cold_meta;
  robj *value;
} swapObjectMeta;
#define initStaticSwapObjectMeta(_som,_omtype,_object_meta,_value) do { \
    _som.omtype = _omtype; \
    _som.object_meta = _object_meta; \
    _som.value = _value; \
} while(0)

static inline int swapObjectMetaIsHot(swapObjectMeta *som) {
    if (som->value == NULL) return 0;
    serverAssert((som->object_meta->swap_type == SWAP_TYPE_BITMAP && som->value->type == OBJ_STRING) || som->object_meta->swap_type == som->value->type);
    if (som->omtype->objectIsHot) {
      return som->omtype->objectIsHot(som->object_meta,som->value);
    } else {
      return 0;
    }
}
int keyIsHot(objectMeta *object_meta, robj *value);

typedef enum {
    NORMAL_MODE = 0,
    RORDB_MODE,
} meta_encode_mode;

#endif /* __CTRIP_STORAGE_OBJECT_META_H__ */
