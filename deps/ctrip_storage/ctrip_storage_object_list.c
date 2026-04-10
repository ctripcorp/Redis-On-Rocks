#include "ctrip_storage_objects.h"


/* List */
typedef struct listSwapData {
  swapData d;
} listSwapData;


swapDataType listSwapDataType = {
    .name = "list",
    // .cmd_swap_flags = CMD_SWAP_DATATYPE_LIST,
    // .swapAna = listSwapAna,
    // .swapAnaAction = listSwapAnaAction,
    // .encodeKeys = listEncodeKeys,
    // .encodeData = listEncodeData,
    // .encodeRange = listEncodeRange,
    // .decodeData = listDecodeData,
    // .swapIn = listSwapIn,
    // .swapOut = listSwapOut,
    // .swapDel = listSwapDel,
    // .createOrMergeObject = listCreateOrMergeObject,
    // .cleanObject = listCleanObject,
    // .beforeCall = listBeforeCall,
    // .free = freeListSwapData,
    // .rocksDel = NULL,
    // .mergedIsHot = listMergedIsHot,
    // .getObjectMetaAux = NULL,
};
// sds encodeListObjectMeta(struct objectMeta *object_meta, void *aux, int meta_enc_mode) {
//     UNUSED(aux);
//     UNUSED(meta_enc_mode);
//     if (object_meta == NULL) return NULL;
//     serverAssert(object_meta->swap_type == SWAP_TYPE_LIST);
//     return encodeListMeta(objectMetaGetPtr(object_meta));
// }
objectMetaType listObjectMetaType = {
    // .encodeObjectMeta = encodeListObjectMeta,
    // .decodeObjectMeta = decodeListObjectMeta,
    // .objectIsHot = listObjectMetaIsHot,
    // .free = listObjectMetaFreeMeta,
    // .duplicate = listObjectMetaDup,
    // .equal = listObjectMetaEqual,
    // .rebuildFeed = listObjectMetaRebuildFeed,
};
int swapDataSetupList(swapData *d, void **pdatactx) {
    d->type = &listSwapDataType;
    d->omtype = &listObjectMetaType;
    listDataCtx *datactx = zmalloc(sizeof(listDataCtx));
    datactx->swap_meta = NULL;
    datactx->ctx_flag = BIG_DATA_CTX_FLAG_NONE;
    argRewriteRequestInit(datactx->arg_reqs+0);
    argRewriteRequestInit(datactx->arg_reqs+1);
    *pdatactx = datactx;
    return 0;
}