#include "ctrip_storage_objects.h"

#define zsetObjectMetaType lenObjectMetaType
swapDataType zsetSwapDataType = {
    .name = "zset",
    // .cmd_swap_flags = CMD_SWAP_DATATYPE_ZSET,
    // .swapAna = zsetSwapAna,
    // .swapAnaAction = zsetSwapAnaAction,
    // .encodeKeys = zsetEncodeKeys,
    // .encodeData = zsetEncodeData,
    // .encodeRange = zsetEncodeRange,
    // .decodeData = zsetDecodeData,
    // .swapIn = zsetSwapIn,
    // .swapOut = zsetSwapOut,
    // .swapDel = zsetSwapDel,
    // .createOrMergeObject = zsetCreateOrMergeObject,
    // .cleanObject = zsetCleanObject,
    // .rocksDel = zsetRocksDel,
    // .free = freeZsetSwapData,
    // .mergedIsHot = zsetMergedIsHot,
    // .getObjectMetaAux = zsetGetObjectMetaAux,
};

int swapDataSetupZSet(swapData *d, void **pdatactx) {
    d->type = &zsetSwapDataType;
    d->omtype = &zsetObjectMetaType;
    zsetDataCtx *datactx = zcalloc(sizeof(zsetDataCtx));
    datactx->bdc.type = BASE_SWAP_CTX_TYPE_SUBKEY;
    datactx->bdc.sub.num = 0;
    datactx->bdc.ctx_flag = BIG_DATA_CTX_FLAG_NONE;
    datactx->bdc.sub.subkeys = NULL;
    datactx->type = ZSET_SWAP_CTX_TYPE_NONE;
    *pdatactx = datactx;
    return 0;
}