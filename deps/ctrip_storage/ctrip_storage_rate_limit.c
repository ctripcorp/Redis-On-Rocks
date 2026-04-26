
#include "ctrip_storage_rate_limit.h"
#include "server.h"
/* ----------------------------- ratelimit ------------------------------ */

void swapRatelimitStart(swapRatelimitCtx *rlctx, client *c) {
    rlctx->is_read_command = (c->cmd->flags & CMD_READONLY) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_READONLY));
    rlctx->is_write_command = (c->cmd->flags & CMD_WRITE) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_WRITE));
    rlctx->is_denyoom_command = (c->cmd->flags & CMD_DENYOOM) ||
                             (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_DENYOOM));
}
/* return 1 if command rejected */
int swapRateLimitReject(swapRatelimitCtx *rlctx, client *c) {
    if (server.storage.swap_ratelimit_policy != SWAP_RATELIMIT_POLICY_REJECT_OOM &&
        server.storage.swap_ratelimit_policy != SWAP_RATELIMIT_POLICY_REJECT_ALL) {
        return 0;
    }

    /* Never reject replicated commands from master */
    if (c->flags & CLIENT_MASTER) return 0;

    if ((server.storage.swap_ratelimit_policy == SWAP_RATELIMIT_POLICY_REJECT_OOM &&
                rlctx->is_denyoom_command) ||
        (server.storage.swap_ratelimit_policy == SWAP_RATELIMIT_POLICY_REJECT_ALL &&
               (rlctx->is_read_command || rlctx->is_write_command))) {
        if (swapRatelimitNeeded(rlctx,server.storage.swap_ratelimit_policy,NULL)) {
            rejectCommand(c, shared.oomerr);
            server.storage.stat_swap_ratelimit_rejected_cmd_count++;
            return 1;
        }
    }

    return 0;
}


static inline sds persistingKeysEarliest_(persistingKeys *keys, int state,
        persistingKeyEntry **entry) {
    list *list = state == SWAP_PERSIST_STATE_TODO ? keys->todo : keys->doing;
    if (listLength(list) == 0) {
        *entry = NULL;
        return NULL;
    } else {
        listNode *ln = listFirst(list);
        sds key = listNodeValue(ln);
        *entry = dictFetchValue(keys->map,key);
        return key;
    }
}

sds persistingKeysEarliest(persistingKeys *keys, persistingKeyEntry **entry) {
    sds k1, k2;
    persistingKeyEntry *e1, *e2;

    k1 = persistingKeysEarliest_(keys,SWAP_PERSIST_STATE_TODO,&e1);
    k2 = persistingKeysEarliest_(keys,SWAP_PERSIST_STATE_DOING,&e2);
    if (k1 && k2) {
        if (e1->mstime < e2->mstime) {
            *entry = e1;
            return k1;
        } else {
            *entry = e2;
            return k2;
        }
    } else if (k1) {
        *entry = e1;
        return k1;
    } else if (k2) {
        *entry = e2;
        return k2;
    } else {
        *entry = NULL;
        return NULL;
    }
}


inline mstime_t swapPersistCtxLag(swapPersistCtx *ctx) {
    mstime_t lag = 0;
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        persistingKeys *keys = ctx->keys[dbid];
        persistingKeyEntry *entry;
        if (persistingKeysEarliest(keys,&entry)) {
            lag = MAX(lag, server.mstime - entry->mstime);
        }
    }
    return lag;
}

inline int swapRatelimitPersistNeeded(swapRatelimitCtx *rlctx, int policy, int *pms) {
    int pause_ms;
    static mstime_t prev_logtime;

    if (pms) *pms = 0;

    if (!server.storage.swap_persist_enabled) return 0;

    mstime_t lag = swapPersistCtxLag(server.storage.swap_persist_ctx) / 1000;
    if (lag <= server.storage.swap_ratelimit_persist_lag) return 0;

    if (policy == SWAP_RATELIMIT_POLICY_PAUSE) {
        if (!rlctx->is_write_command) return 0;
        pause_ms = (lag - server.storage.swap_ratelimit_persist_lag)/server.storage.swap_ratelimit_persist_pause_growth_rate;
        pause_ms = pause_ms < SWAP_RATELIMIT_PAUSE_MAX_MS ? pause_ms : SWAP_RATELIMIT_PAUSE_MAX_MS;
        if (pms) *pms = pause_ms;
    }

    if (server.mstime - prev_logtime > 1000) {
        char msg[32] = {0};
        if (policy == SWAP_RATELIMIT_POLICY_PAUSE) {
            snprintf(msg,sizeof(msg)-1,"pause (%d)ms", pause_ms);
        } else {
            snprintf(msg,sizeof(msg)-1,"reject");
        }

        serverLog(LL_NOTICE,"[ratelimit] persist lag(%lld) > (%d): %s", lag, server.storage.swap_ratelimit_persist_lag, msg);
        prev_logtime = server.mstime;
    }

    return 1;
}

unsigned long long calculateNextMemoryLimit(size_t mem_used, unsigned long long from, unsigned long long to) {
    if (from <= to || to == 0) return to;
    /* scale down maxmemory step by step for low evict concurrent */
    unsigned long long safe_mem_limit = mem_used - server.storage.maxmemory_scaledown_rate;
    if (safe_mem_limit < from) {
        return safe_mem_limit > to ? safe_mem_limit : to;
    }
    return from;
}

inline int swapRatelimitMaxmemoryNeeded(swapRatelimitCtx *rlctx, int policy, int *pms) {
    int pause_ms;
    static mstime_t prev_logtime;
    size_t mem_reported, mem_used, mem_ratelimit, actual_maxmemory;

    if (pms) *pms = 0;

    /* No need to pause hot read or management commands(ping/info/config) */
    if (!rlctx->keyrequests_count && !rlctx->is_denyoom_command) return 0;

    /* mem_used are not returned if not overmaxmemory. */
    if (!getMaxmemoryState(&mem_reported,&mem_used,NULL,NULL)) return 0;

    actual_maxmemory = calculateNextMemoryLimit(mem_used,
            server.storage.maxmemory_scale_from,server.maxmemory);

    mem_ratelimit = actual_maxmemory*server.storage.swap_ratelimit_maxmemory_percentage/100;
    if (mem_used <= mem_ratelimit || mem_ratelimit == 0)  return 0;

    if (policy == SWAP_RATELIMIT_POLICY_PAUSE) {
        pause_ms = (mem_used - mem_ratelimit)/server.storage.swap_ratelimit_maxmemory_pause_growth_rate;
        pause_ms = pause_ms < SWAP_RATELIMIT_PAUSE_MAX_MS ? pause_ms : SWAP_RATELIMIT_PAUSE_MAX_MS;
        if (pms) *pms = pause_ms;
    }

    if (server.mstime - prev_logtime > 1000) {
        char msg[32] = {0};
        if (policy == SWAP_RATELIMIT_POLICY_PAUSE) {
            snprintf(msg,sizeof(msg)-1,"pause (%d)ms", pause_ms);
        } else {
            snprintf(msg,sizeof(msg)-1,"reject");
        }

        serverLog(LL_NOTICE,"[ratelimit] mem_used(%ld) > (%ld): %s", mem_used, mem_ratelimit, msg);
        prev_logtime = server.mstime;
    }

    return 1;
}

static int unprotectClientdProc(
        struct aeEventLoop *el, long long id, void *clientData) {
    client *c = clientData;
    UNUSED(el), UNUSED(id);
    unprotectClient(c);
    c->deferred_cmd->rate_limit_event_id = -1;
    return AE_NOMORE;
}
void swapRateLimitPause(swapRatelimitCtx *rlctx, client *c) {
    int pause_ms;
    rlctx->keyrequests_count = c->deferred_cmd->keyrequests_count;

    if (server.storage.swap_ratelimit_policy != SWAP_RATELIMIT_POLICY_PAUSE) return;

    if (swapRatelimitNeeded(rlctx,server.storage.swap_ratelimit_policy,&pause_ms) &&
            pause_ms > 0 && c->deferred_cmd->rate_limit_event_id == -1) {
        protectClient(c);
        c->deferred_cmd->rate_limit_event_id = aeCreateTimeEvent(server.el,pause_ms,unprotectClientdProc,c,NULL);
        server.storage.stat_swap_ratelimit_client_pause_count++;
        server.storage.stat_swap_ratelimit_client_pause_ms += pause_ms;
    }
}