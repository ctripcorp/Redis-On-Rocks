
#include "ctrip_storage_object_meta.h"

/* objectMeta */
static inline objectMetaType *getObjectMetaType(int swap_type) {
    objectMetaType *omtype = NULL;
    switch (swap_type) {
    case SWAP_TYPE_STRING:
        omtype = NULL;
        break;
    case SWAP_TYPE_HASH:
    case SWAP_TYPE_SET:
    case SWAP_TYPE_ZSET:
        omtype = &lenObjectMetaType;
        break;
    case SWAP_TYPE_LIST:
        omtype = &listObjectMetaType;
        break;
    case SWAP_TYPE_BITMAP:
        omtype = &bitmapObjectMetaType;
        break;
    default:
        break;
    }
    return omtype;
}

objectMeta *createObjectMeta(int swap_type, uint64_t version) {
	objectMeta *m = zmalloc(sizeof(objectMeta));
    m->version = version;
    m->swap_type = swap_type;
    m->len = 0;
    return m;
}


int buildObjectMeta(int swap_type, uint64_t version, const char *extend,
        size_t extlen, OUT objectMeta **pobject_meta) {
    objectMeta *object_meta;
    objectMetaType *omtype = getObjectMetaType(swap_type);

    if (omtype == NULL || omtype->decodeObjectMeta == NULL || extend == NULL) {
        if (pobject_meta) *pobject_meta = NULL;
        return 0;
    }

    if (pobject_meta == NULL) return 0;

    object_meta = createObjectMeta(swap_type, version);
    if (omtype->decodeObjectMeta(object_meta,extend,extlen)) {
        zfree(object_meta);
        *pobject_meta = NULL;
        return -1;
    }

    *pobject_meta = object_meta;
    return 0;
}

void freeObjectMeta(objectMeta *object_meta) {
    objectMetaType *omtype;
    if (object_meta == NULL) return;
    omtype = getObjectMetaType(object_meta->swap_type);
    if (omtype != NULL && omtype->free) omtype->free(object_meta);
    zfree(object_meta);
}



metaScanResult *metaScanResultCreate() {
    metaScanResult *result = zcalloc(sizeof(metaScanResult));
    result->metas = result->buffer;
    result->size = DEFAULT_SCANMETA_BUFFER;
    result->num = 0;
    result->nextseek = NULL;
    return result;
}

void scanMetaInit(scanMeta *meta, int swap_type, sds key, long long expire) {
    meta->key = key;
    meta->expire = expire;
    meta->swap_type = swap_type;
}


int keyIsHot(objectMeta *object_meta, robj *value) {
    swapObjectMeta som;
    objectMetaType *type;
    if (value == NULL) return 0;
    if (object_meta == NULL) return 1;
    type = getObjectMetaType(object_meta->swap_type);
    initStaticSwapObjectMeta(som,type,object_meta,value);
    return swapObjectMetaIsHot(&som);
}