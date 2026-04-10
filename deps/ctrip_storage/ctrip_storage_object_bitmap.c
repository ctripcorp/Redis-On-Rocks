#include "ctrip_storage_objects.h"








swapDataType bitmapSwapDataType = {
        .name = "bitmap",
        // .cmd_swap_flags = CMD_SWAP_DATATYPE_BITMAP,
        // .swapAna = bitmapSwapAna,
        // .swapAnaAction = bitmapSwapAnaAction,
        // .encodeKeys = bitmapEncodeKeys,
        // .encodeData = bitmapEncodeData,
        // .encodeRange = bitmapEncodeRange,
        // .decodeData = bitmapDecodeData,
        // .swapIn = bitmapSwapIn,
        // .swapOut = bitmapSwapOut,
        // .swapDel = bitmapSwapDel,
        // .createOrMergeObject = bitmapCreateOrMergeObject,
        // .cleanObject = bitmapCleanObject,
        // .beforeCall = bitmapBeforeCall,
        // .free = bitmapSwapDataFree,
        // .rocksDel = NULL,
        // .mergedIsHot = bitmapMergedIsHot,
        // .getObjectMetaAux = NULL,
};

int swapDataSetupBitmap(swapData *d, void **pdatactx) {
    d->type = &bitmapSwapDataType;
    d->omtype = &bitmapObjectMetaType;
    bitmapDataCtx *datactx = zmalloc(sizeof(bitmapDataCtx));
    memset(datactx, 0, sizeof(bitmapDataCtx));
    argRewriteRequestInit(datactx->arg_reqs + 0);
    argRewriteRequestInit(datactx->arg_reqs + 1);
    *pdatactx = datactx;
    return 0;
}

int bitmapObjectMetaIsMarker(objectMeta *object_meta) {
    serverAssert(object_meta->swap_type == SWAP_TYPE_BITMAP);
    return NULL == objectMetaGetPtr(object_meta);
}



void bitmapMetaFree(bitmapMeta *bitmap_meta) {
    if (bitmap_meta == NULL) return;
    rbmDestory(bitmap_meta->subkeys_status);
    bitmap_meta->subkeys_status = NULL;
    zfree(bitmap_meta);
}
void bitmapMetaTransToMarkerIfNeeded(objectMeta *object_meta) {
    if (!bitmapObjectMetaIsMarker(object_meta)) {
        bitmapMetaFree(objectMetaGetPtr(object_meta));
        objectMetaSetPtr(object_meta, NULL);
    }
}

/* bitmap object meta (CAN be marker) */

objectMeta *createBitmapObjectMeta(uint64_t version, bitmapMeta *bitmap_meta) {
    objectMeta *object_meta = createObjectMeta(SWAP_TYPE_BITMAP, version);
    objectMetaSetPtr(object_meta, bitmap_meta);
    return object_meta;
}
objectMeta *createBitmapObjectMarker() {
    return createBitmapObjectMeta(swapGetAndIncrVersion(), NULL);
}

int bitmapSetObjectMarkerIfNotExist(redisDb *db, robj *key) {
    objectMeta *object_meta = lookupMeta(db,key);
    if (object_meta == NULL) {
        dbAddMeta(db,key,createBitmapObjectMarker());
        return 1;
    }
    return 0;
}


objectMetaType bitmapObjectMetaType = {
        // .encodeObjectMeta = bitmapObjectMetaEncode,
        // .decodeObjectMeta = bitmapObjectMetaDecode,
        // .objectIsHot = bitmapObjectMetaIsHot,
        // .free = bitmapObjectMetaFreeMeta,
        // .duplicate = bitmapObjectMetaDup,
        // .equal = bitmapObjectMetaEqual,
        // .rebuildFeed = bitmapObjectMetaRebuildFeed
};