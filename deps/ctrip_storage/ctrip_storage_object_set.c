#include "ctrip_storage_objects.h"

swapDataType setSwapDataType = {
    .name = "set",
    // .cmd_swap_flags = CMD_SWAP_DATATYPE_SET,
    // .swapAna = setSwapAna,
    // .swapAnaAction = setSwapAnaAction,
    // .encodeKeys = setEncodeKeys,
    // .encodeData = setEncodeData,
    // .encodeRange = setEncodeRange,
    // .decodeData = setDecodeData,
    // .swapIn = setSwapIn,
    // .swapOut = setSwapOut,
    // .swapDel = setSwapDel,
    // .createOrMergeObject = setCreateOrMergeObject,
    // .cleanObject = setCleanObject,
    // .beforeCall = NULL,
    // .free = freeSetSwapData,
    // .rocksDel = NULL,
    // .mergedIsHot = setMergedIsHot,
    // .getObjectMetaAux = setGetObjectMetaAux,
};
#define setObjectMetaType lenObjectMetaType
int swapDataSetupSet(swapData *d, OUT void **pdatactx) {
    d->type = &setSwapDataType;
    d->omtype = &setObjectMetaType;
    setDataCtx *datactx = zmalloc(sizeof(setDataCtx));
    datactx->ctx.type = BASE_SWAP_CTX_TYPE_SUBKEY;
    datactx->ctx.sub.num = 0;
    datactx->ctx.ctx_flag = BIG_DATA_CTX_FLAG_NONE;
    datactx->ctx.sub.subkeys = NULL;
    *pdatactx = datactx;
    return 0;
}