
#include "ctrip_storage_request.h"
#include "ctrip_storage_debug.h"
#include "ctrip_storage_metric.h"
#include "ctrip_storage_filter.h"
/* Called by async-complete-queue or parallel-sync in server thread
 * to swap in/out/del data */
void swapRequestMerge(swapRequest *req) {
    DEBUG_MSGS_APPEND(req->msgs,"exec-finish","intention=%s",
            swapIntentionName(req->intention));
    int retval = 0, del_skip = 0, swap_out_completely = 0;
    swapData *data = req->data;
    void *datactx = req->datactx;

    serverAssert(!swapRequestGetError(req));

    switch (req->intention) {
    case SWAP_NOP:
        /* No swap for req if meta not found. */
        if (!swapDataAlreadySetup(data)) {
            coldFilterKeyNotFound(data->db->storage.cold_filter,data->key->ptr);
        }

        break;
    case SWAP_IN:
        retval = swapDataSwapIn(data,&(req->result),datactx);
        if (retval == 0) {
            if (swapDataIsCold(data) && req->result) {
                swapDataTurnWarmOrHot(data);
            }
            swapDataMergeAbsentSubkey(data);
        }
        break;
    case SWAP_OUT:
        /* object meta will be persisted every time, so meta turns clean .*/
        clearObjectMetaDirty(data->value);
        int clear_dirty = req->intention_flags & SWAP_EXEC_OUT_KEEP_DATA;
        retval = swapDataSwapOut(data,datactx,clear_dirty,&swap_out_completely);
        if (!swapDataIsCold(data)) {
            if (swap_out_completely) {
                swapDataTurnCold(data);
            } else {
                coldFilterSubkeyAdded(data->db->storage.cold_filter,data->key->ptr);
            }
        }
        break;
    case SWAP_DEL:
        del_skip = req->intention_flags & SWAP_FIN_DEL_SKIP;
        swapDataTurnDeleted(data,del_skip);
        retval = swapDataSwapDel(data,datactx,del_skip);
        break;
    case SWAP_UTILS:
        retval = 0;
        break;
    default:
        serverPanic("merge: unexpected request intention");
        retval = SWAP_ERR_DATA_FIN_FAIL;
        break;
    }

    if (retval) swapRequestSetError(req, retval);
}
void swapRequestBatchCallback(swapRequestBatch *reqs) {
    size_t swap_memory = 0;

    if (reqs->notify_queue_timer) {
        metricDebugInfo(SWAP_DEBUG_NOTIFY_QUEUE_WAIT, elapsedUs(reqs->notify_queue_timer));
    }

    for (size_t i = 0; i < reqs->count; i++) {
        swapRequest *req = reqs->reqs[i];
        swap_memory += req->swap_memory;

        if (!swapRequestGetError(req))
            swapRequestMerge(req);

        if (req->trace) swapTraceCallback(req->trace);
        req->finish_cb(req->data,req->finish_pd,swapRequestGetError(req));
    }

    atomicDecr(server.storage.swap_inprogress_batch,1);
    atomicDecr(server.storage.swap_inprogress_count,reqs->count);
    atomicDecr(server.storage.swap_inprogress_memory,swap_memory);
}