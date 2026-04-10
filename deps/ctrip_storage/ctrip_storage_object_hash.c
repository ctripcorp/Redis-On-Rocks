#include "ctrip_storage_objects.h"
#define hashObjectMetaType lenObjectMetaType
swapDataType hashSwapDataType = {
    .name = "hash",
    // .cmd_swap_flags = CMD_SWAP_DATATYPE_HASH,
    // .swapAna = hashSwapAna,
    // .swapAnaAction = hashSwapAnaAction,
    // .encodeKeys = hashEncodeKeys,
    // .encodeRange = hashEncodeRange,
    // .encodeData = hashEncodeData,
    // .decodeData = hashDecodeData,
    // .swapIn = hashSwapIn,
    // .swapOut = hashSwapOut,
    // .swapDel = hashSwapDel,
    // .createOrMergeObject = hashCreateOrMergeObject,
    // .cleanObject = hashCleanObject,
    // .beforeCall = NULL,
    // .free = freeHashSwapData,
    // .rocksDel = NULL,
    // .mergedIsHot = hashMergedIsHot,
    // .getObjectMetaAux = hashGetObjectMetaAux,
};
int swapDataSetupHash(swapData *d, void **pdatactx) {
    d->type = &hashSwapDataType;
    d->omtype = &hashObjectMetaType;
    hashDataCtx *datactx = zmalloc(sizeof(hashDataCtx));
    datactx->ctx.sub.num = 0;
    datactx->ctx.sub.subkeys = NULL;
    *pdatactx = datactx;
    return 0;
}