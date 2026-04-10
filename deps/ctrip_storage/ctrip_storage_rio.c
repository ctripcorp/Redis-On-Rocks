#include "ctrip_storage_rio.h"
#include "ctrip_storage_request_utils.h"
#include "ctrip_storage_error.h"
#include "ctrip_storage_metric.h"
void RIODeinit(RIO *rio) {
    int i;

    if (rio->err) {
        sdsfree(rio->err);
        rio->err = NULL;
    }
    switch (rio->action) {
    case  ROCKS_GET:
    case  ROCKS_PUT:
    case  ROCKS_DEL:
        for (i = 0; i < rio->generic.numkeys; i++) {
            if (rio->generic.rawkeys) sdsfree(rio->generic.rawkeys[i]);
            if (rio->generic.rawvals) sdsfree(rio->generic.rawvals[i]);
        }
        zfree(rio->generic.cfs);
        rio->generic.cfs = NULL;
        zfree(rio->generic.rawkeys);
        rio->generic.rawkeys = NULL;
        zfree(rio->generic.rawvals);
        rio->generic.rawvals = NULL;
        break;
    case ROCKS_ITERATE:
        if (rio->iterate.start) {
            sdsfree(rio->iterate.start);
            rio->iterate.start = NULL;
        }
        if (rio->iterate.end) {
            sdsfree(rio->iterate.end);
            rio->iterate.end = NULL;
        }
        for (i = 0; i < rio->iterate.numkeys; i++) {
            if (rio->iterate.rawkeys) sdsfree(rio->iterate.rawkeys[i]);
            if (rio->iterate.rawvals) sdsfree(rio->iterate.rawvals[i]);
        }
        if (rio->iterate.rawkeys) zfree(rio->iterate.rawkeys);
        rio->iterate.rawkeys = NULL;
        if (rio->iterate.rawvals) zfree(rio->iterate.rawvals);
        rio->iterate.rawvals = NULL;
        if (rio->iterate.nextseek) sdsfree(rio->iterate.nextseek);
        rio->iterate.nextseek = NULL;
        break;
    default:
        break;
    }
}
void RIOBatchDeinit(RIOBatch *rios) {
    if (rios == NULL) return;
    for (size_t i = 0; i < rios->count; i++) {
        RIO *rio = rios->rios+i;
        RIODeinit(rio);
    }
    rios->count = 0;
    if (rios->rios != rios->rio_buf) {
        zfree(rios->rios);
        rios->rios = NULL;
    }
}

static inline int RIOGetCF(RIO *rio) {
    int cf;
    if (rio->action == ROCKS_ITERATE) {
        cf = rio->iterate.cf;
    } else if (rio->generic.numkeys > 0) {
        cf = rio->generic.cfs[0];
    } else {
        cf = META_CF;
    };
    return cf;
}

void RIODoGet(RIO *rio) {
    server.storage.engine->get(server.storage.engine->context, rio);
}
void RIODoPut(RIO *rio) {
    server.storage.engine->put(server.storage.engine->context, rio);
}
void RIODoDel(RIO *rio) {
    server.storage.engine->del(server.storage.engine->context, rio);
}
void RIODoIterate(RIO *rio) {
    server.storage.engine->iterate(server.storage.engine->context, rio);
}

void RIOUpdateStatsDataNotFound(RIO *rio) {
    if (rio->action == ROCKS_GET && rio->get.notfound) {
        atomicIncr(server.storage.swap_hit_stats->stat_swapin_data_not_found_count,
                rio->get.notfound);
    }
}

#define RIO_ITERATE_NUMKEYS_ALLOC_INIT 8
#define RIO_ITERATE_NUMKEYS_ALLOC_LINER 4096
#define RIO_ESTIMATE_PAYLOAD_SAMPLE 8

size_t RIOEstimatePayloadSize(RIO *rio) {
    int i;
    size_t memory = 0;

    switch (rio->action) {
    case ROCKS_GET:
    case ROCKS_PUT:
    case ROCKS_DEL:
        for (i = 0; i < rio->get.numkeys && i < RIO_ESTIMATE_PAYLOAD_SAMPLE; i++) {
            memory += sdsalloc(rio->get.rawkeys[i]);
            if (rio->get.rawvals && rio->get.rawvals[i])
                memory += sdsalloc(rio->get.rawvals[i]);
        }
        if (rio->get.numkeys > RIO_ESTIMATE_PAYLOAD_SAMPLE) {
            memory = memory*rio->get.numkeys/RIO_ESTIMATE_PAYLOAD_SAMPLE;
        }

        break;
    case ROCKS_ITERATE:
        for (i = 0; i < rio->iterate.numkeys && i < RIO_ESTIMATE_PAYLOAD_SAMPLE; i++) {
            memory += sdsalloc(rio->iterate.rawkeys[i]);
            if (rio->iterate.rawvals && rio->iterate.rawvals[i])
                memory += sdsalloc(rio->iterate.rawvals[i]);
        }
        if (rio->get.numkeys > RIO_ESTIMATE_PAYLOAD_SAMPLE) {
            memory = memory*rio->get.numkeys/RIO_ESTIMATE_PAYLOAD_SAMPLE;
        }
        break;
    default:
        break;
    }

    return memory;
}


void RIOUpdateStatsDo(RIO *rio, long duration) {
    int action = rio->action;
    size_t payload_size = RIOEstimatePayloadSize(rio);
    atomicIncr(server.storage.ror_stats->rio_stats[action].memory,payload_size);
    atomicIncr(server.storage.ror_stats->rio_stats[action].count,1);
    atomicIncr(server.storage.ror_stats->rio_stats[action].batch,1);
    atomicIncr(server.storage.ror_stats->rio_stats[action].time,duration);
}


void RIODo(RIO *rio) {
    monotime io_timer;

    elapsedStart(&io_timer);

    if (server.storage.swap_debug_rio_delay_micro)
        usleep(server.storage.swap_debug_rio_delay_micro);

    if (server.storage.swap_debug_rio_error > 0) {
        if (!server.storage.swap_debug_rio_error_action || rio->action == server.storage.swap_debug_rio_error_action) {
            server.storage.swap_debug_rio_error--;
            RIOSetError(rio,SWAP_ERR_RIO_FAIL,sdsnew("rio mock error"));
            goto end;
        }
    }

    switch (rio->action) {
    case ROCKS_GET:
        RIODoGet(rio);
        break;
    case ROCKS_PUT:
        RIODoPut(rio);
        break;
    case ROCKS_DEL:
        RIODoDel(rio);
        break;
    case ROCKS_ITERATE:
        RIODoIterate(rio);
        break;
    default:
        serverPanic("[RIO] Unknown io action: %d", rio->action);
    }

#ifdef ROCKS_DEBUG
    RIODump(rio);
#endif

end:
    if (RIOGetCF(rio) != META_CF) {
        RIOUpdateStatsDo(rio, elapsedUs(io_timer));
        RIOUpdateStatsDataNotFound(rio);
    }
}

static void RIOBatchDoIndividually(RIOBatch *rios) {
    for (size_t i = 0; i < rios->count; i++) {
        RIO *rio = rios->rios+i;
        serverAssert(rio->action == rios->action);
        RIODo(rios->rios+i);
    }
}

static void RIOBatchSetError(RIOBatch *rios, int errcode, const char *err) {
    for (size_t i = 0; i < rios->count; i++) {
        RIO *rio = rios->rios+i;
        RIOSetError(rio,errcode,sdsnew(err));
    }
}


void RIOBatchDoPut(RIOBatch *rios) {
    server.storage.engine->batch_put(server.storage.engine->context, rios);
}
void RIOBatchDoDel(RIOBatch *rios) {
    server.storage.engine->batch_del(server.storage.engine->context, rios);
}

void RIOBatchDoGet(RIOBatch *rios) {
    server.storage.engine->batch_get(server.storage.engine->context, rios);
}



void RIOBatchUpdateStatsDataNotFound(RIOBatch *rios) {
    int notfound = 0;
    for (size_t i = 0; i < rios->count; i++) {
        RIO *rio = rios->rios+i;
        int cf = RIOGetCF(rio);
        if (cf != META_CF) notfound += rio->get.notfound;
    }

    if (notfound) {
        atomicIncr(server.storage.swap_hit_stats->stat_swapin_data_not_found_count,
                notfound);
    }
}

void RIOBatchUpdateStatsDo(RIOBatch *rios, long duration) {
    int action = rios->action;
    size_t payload_size = 0;
    size_t count = 0;
    for (size_t i = 0; i < rios->count; i++) {
        RIO *rio = rios->rios+i;
        int cf = RIOGetCF(rio);
        if (cf != META_CF) {
            payload_size += RIOEstimatePayloadSize(rio);
            count++;
        }
    }
    atomicIncr(server.storage.ror_stats->rio_stats[action].memory,payload_size);
    atomicIncr(server.storage.ror_stats->rio_stats[action].count,count);
    atomicIncr(server.storage.ror_stats->rio_stats[action].batch,1);
    atomicIncr(server.storage.ror_stats->rio_stats[action].time,duration);
}

/* GET -- multiget; PUT/DEL -- write; ITERATE -- cant batch; */
void RIOBatchDo(RIOBatch *rios) {
    monotime io_timer;

    /* Fallback to RIODo for actions that cant batch */
    if (rios->action == ROCKS_ITERATE) {
        RIOBatchDoIndividually(rios);
        return;
    }

    elapsedStart(&io_timer);

    if (server.storage.swap_debug_rio_delay_micro)
        usleep(server.storage.swap_debug_rio_delay_micro);

    if (server.storage.swap_debug_rio_error > 0) {
        if (!server.storage.swap_debug_rio_error_action || rios->action == server.storage.swap_debug_rio_error_action) {
            server.storage.swap_debug_rio_error--;
            RIOBatchSetError(rios,SWAP_ERR_RIO_FAIL,"rio mock error");
            goto end;
        }
    }

    switch (rios->action) {
    case ROCKS_GET:
        RIOBatchDoGet(rios);
        break;
    case ROCKS_PUT:
        RIOBatchDoPut(rios);
        break;
    case ROCKS_DEL:
        RIOBatchDoDel(rios);
        break;
    default:
        serverPanic("[RIOBatch] Unknown io action %d", rios->action);
        break;
    }
#ifdef ROCKS_DEBUG
    RIOBatchDump(rios);
#endif

end:
    RIOBatchUpdateStatsDo(rios, elapsedUs(io_timer));
    RIOBatchUpdateStatsDataNotFound(rios);
}

static inline void RIOInitGeneric(RIO *rio, int action, int numkeys,
        int *cfs, sds *rawkeys, sds *rawvals) {
    rio->action = action;
    rio->generic.numkeys = numkeys;
    rio->generic.cfs = cfs;
    rio->generic.rawkeys = rawkeys;
    rio->generic.rawvals = rawvals;
    rio->generic.notfound = 0;
    rio->err = NULL;
    rio->errcode = 0;
    rio->oom_check = 0;
}

void RIOInitGet(RIO *rio, int numkeys, int *cfs, sds *rawkeys) {
    RIOInitGeneric(rio,ROCKS_GET,numkeys,cfs,rawkeys,NULL);
}

void RIOInitPut(RIO *rio, int numkeys, int *cfs, sds *rawkeys, sds *rawvals) {
    RIOInitGeneric(rio,ROCKS_PUT,numkeys,cfs,rawkeys,rawvals);
}

void RIOInitDel(RIO *rio, int numkeys, int *cfs, sds *rawkeys) {
    RIOInitGeneric(rio,ROCKS_DEL,numkeys,cfs,rawkeys,NULL);
}

void RIOInitIterate(RIO *rio, int cf, uint32_t flags, sds start, sds end, size_t limit) {
    rio->action = ROCKS_ITERATE;
    rio->iterate.cf = cf;
    rio->iterate.flags = flags;
    rio->iterate.start = start;
    rio->iterate.end = end;
    rio->iterate.limit = limit;
    rio->iterate.numkeys = 0;
    rio->iterate.rawkeys = NULL;
    rio->iterate.rawvals = NULL;
    rio->iterate.nextseek = NULL;
    rio->err = NULL;
    rio->errcode = 0;
    rio->oom_check = 0;
}