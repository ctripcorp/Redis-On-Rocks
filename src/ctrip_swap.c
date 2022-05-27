/* Copyright (c) 2021, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "ctrip_swap.h"

static void dupKeyRequest(keyRequest *dst, keyRequest *src) {
    if (src->key) incrRefCount(src->key);
    dst->key = src->key;
    dst->num_subkeys = src->num_subkeys;
    for (int i = 0; i < src->num_subkeys; i++) {
        if (src->subkeys[i]) incrRefCount(src->subkeys[i]);
        dst->subkeys[i] = src->subkeys[i];
    }
}

static void keyRequestDeinit(keyRequest *key_request) {
    if (key_request->key) decrRefCount(key_request->key);
    key_request->key = NULL;
    key_request->num_subkeys = 0;
    for (int i = 0; i < key_request->num_subkeys; i++) {
        if (key_request->subkeys[i]) decrRefCount(key_request->subkeys[i]);
        key_request->subkeys[i] = NULL;
    }
}

/* SwapCtx manages context and data for swapping specific key. Note that:
 * - key_request copy to swapCtx.key_request
 * - swapdata moved to swapCtx,
 * - swapRequest managed by async/sync complete queue (not by swapCtx).
 * swapCtx released when keyRequest finishes. */
swapCtx *swapCtxCreate(client *c, keyRequest *key_request,
         clientKeyRequestFinished finished) {
    swapCtx *ctx = zcalloc(sizeof(swapCtx));
    ctx->c = c;
    ctx->cmd_intention = c->cmd->intention;
    dupKeyRequest(ctx->key_request,key_request);
    ctx->finished = finished;
#ifdef SWAP_DEBUG
    char *key = key_request->key ? key_request->key->ptr : "(nil)";
    snprintf(ctx->msgs.identity,MAX_MSG,"[%s:%s:%.*s]",
            swapIntentionName(ctx->cmd_intention),c->cmd->name,MAX_MSG/2,key);
#endif
    return ctx;
}

void swapCtxFree(swapCtx *ctx) {
    if (!ctx) return;
#ifdef SWAP_DEBUG
    swapCtxMsgDump(ctx);
#endif
    keyRequestDeinit(ctx->key_request);
    if (ctx->data) {
        swapDataFree(ctx->data,ctx->datactx);
        ctx->data = NULL;
    }
    zfree(ctx);
}

#ifdef SWAP_DEBUG

void swapCtxMsgAppend(swapCtx *ctx, char *step, char *fmt, ...) {
    va_list ap;
    char *name = ctx->msgs.steps[ctx->msgs.index].name;
    char *info = ctx->msgs.steps[ctx->msgs.index].info;
    strncpy(name,step,MAX_MSG-1);
    va_start(ap,fmt);
    vsnprintf(info,MAX_MSG,fmt,ap);
    va_end(ap);
    // serverLog(LL_WARNING, " %02d %16s : %s",ctx->msgs.index,name,info);
    ctx->msgs.index++;
}

void swapCtxMsgDump(swapCtx *ctx) {
    serverLog(LL_NOTICE,"=== %s ===", ctx->msgs.identity);
    for (int i = 0; i < ctx->msgs.index; i++) {
        char *name = ctx->msgs.steps[i].name;
        char *info = ctx->msgs.steps[i].info;
        serverLog(LL_NOTICE,"%2d %25s : %s",i,name,info);
    }
}

#endif

void continueProcessCommand(client *c) {
	c->flags &= ~CLIENT_SWAPPING;
    server.current_client = c;
    server.in_swap_cb = 1;
	call(c,CMD_CALL_FULL);
    server.in_swap_cb = 0;
    /* post call */
    c->woff = server.master_repl_offset;
    if (listLength(server.ready_keys))
        handleClientsBlockedOnKeys();
    /* post command */
    commandProcessed(c);
    /* unhold keys for current command. */
    serverAssert(c->client_hold_mode == CLIENT_HOLD_MODE_CMD);
    clientUnholdKeys(c);
    /* pipelined command might already read into querybuf, if process not
     * restarted, pending commands would not be processed again. */
    processInputBuffer(c);
}

void normalClientKeyRequestFinished(client *c, swapCtx *ctx) {
    robj *key = ctx->key_request->key;
    DEBUG_APPEND(ctx,"request-finished","key=%s, keyrequests_count=%d",
            key?(sds)key->ptr:"<nil>", c->keyrequests_count);
    swapCtxFree(ctx);
    c->keyrequests_count--;
    if (c->keyrequests_count == 0) {
        if (!c->CLIENT_DEFERED_CLOSING) continueProcessCommand(c);
    }
}

int keyRequestSwapFinished(swapData *data, void *pd) {
    UNUSED(data);
    swapCtx *ctx = pd;
    void *listeners = ctx->listeners;
    ctx->finished(ctx->c,ctx);
    requestNotify(listeners);
    return 0;
}

/* Note keyRequestProceed include key/db/svr level request, only key level
 * requests might need swap. */
int genericRequestProceed(void *listeners, redisDb *db, robj *key,
        client *c, void *pd) {
    int retval = C_OK;
    void *datactx;
    swapData *data;
    swapCtx *ctx = pd;
    robj *value, *evict;
    char *reason;
    UNUSED(reason);
    
    if (db == NULL || key == NULL) {
        reason = "noswap needed for db/svr level request";
        goto noswap;
    }

    value = lookupKey(db,key,LOOKUP_NOTOUCH);
    evict = lookupEvictKey(db,key);

    if (!value && !evict) {
        reason = "key not exists";
        goto noswap;
    }

    data = createSwapData(db,key,value,evict,&datactx);

    if (data == NULL) {
        reason = "data not support swap";
        goto noswap;
    }

    ctx->listeners = listeners;
    ctx->data = data;
    ctx->datactx = datactx;

    if (swapDataAna(data,ctx->cmd_intention,ctx->key_request,
                &ctx->swap_intention)) {
        ctx->errcode = SWAP_ERR_ANA_FAIL;
        retval = C_ERR;
        reason = "swap ana failed";
        goto noswap;
    }

    if (ctx->swap_intention == SWAP_NOP) {
        reason = "swapana decided no swap";
        goto noswap;
    }

    DEBUG_APPEND(ctx,"request-proceed","start swap=%s",
            swapIntentionName(ctx->swap_intention));
    submitSwapRequest(SWAP_MODE_ASYNC,ctx->swap_intention,data,datactx,
            keyRequestSwapFinished,ctx);

    return C_OK;

noswap:
    DEBUG_APPEND(ctx,"request-proceed","no swap needed: %s", reason);
    ctx->finished(c,ctx);
    requestNotify(listeners);
    return retval;
}

void submitClientKeyRequests(client *c, getKeyRequestsResult *result,
        clientKeyRequestFinished cb) {
    for (int i = 0; i < result->num; i++) {
        keyRequest *key_request = result->key_requests + i;
        swapCtx *ctx = swapCtxCreate(c,key_request,cb);
        redisDb *db = key_request->level == REQUEST_LEVEL_SVR ? NULL : c->db;
        robj *key = key_request->key;
        DEBUG_APPEND(ctx,"request-wait","key=%s",key?(sds)key->ptr:"<nil>");
        requestWait(db,key,genericRequestProceed,c,ctx);
    }
}

/* Returns submited keyrequest count, if any keyrequest submitted, command
 * gets called in contiunueProcessCommand instead of normal call(). */
int submitNormalClientRequests(client *c) {
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    getKeyRequests(c,&result);
    c->keyrequests_count = result.num;
    submitClientKeyRequests(c,&result,normalClientKeyRequestFinished);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
    return result.num;
}

int dbSwap(client *c) {
    int keyrequests_submit;
    if (!(c->flags & CLIENT_MASTER)) {
        keyrequests_submit = submitNormalClientRequests(c);
    } else {
        keyrequests_submit = submitReplClientRequests(c);
    }
    if (c->keyrequests_count) swapRateLimit(c);
    return keyrequests_submit;
}

void swapInit() {
    int i;
    char *swap_type_names[] = {"nop", "get", "put", "del"};

    server.swap_stats = zmalloc(SWAP_TYPES*sizeof(swapStat));
    for (i = 0; i < SWAP_TYPES; i++) {
        server.swap_stats[i].name = swap_type_names[i];
        server.swap_stats[i].started = 0;
        server.swap_stats[i].finished = 0;
        server.swap_stats[i].last_start_time = 0;
        server.swap_stats[i].last_finish_time = 0;
        server.swap_stats[i].started_rawkey_bytes = 0;
        server.swap_stats[i].finished_rawkey_bytes = 0;
        server.swap_stats[i].started_rawval_bytes = 0;
        server.swap_stats[i].finished_rawval_bytes = 0;
    }

    server.evict_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->cmd = lookupCommandByCString("EVICT");
        c->db = server.db+i;
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.evict_clients[i] = c;
    }

    server.rksdel_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->cmd = lookupCommandByCString("RKSDEL");
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.rksdel_clients[i] = c;
    }

    server.rksget_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->cmd = lookupCommandByCString("RKSGET");
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.rksget_clients[i] = c;
    }

    server.dummy_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.dummy_clients[i] = c;
    }

    server.repl_workers = 256;
    server.repl_swapping_clients = listCreate();
    server.repl_worker_clients_free = listCreate();
    server.repl_worker_clients_used = listCreate();
    for (i = 0; i < server.repl_workers; i++) {
        client *c = createClient(NULL);
        c->client_hold_mode = CLIENT_HOLD_MODE_REPL;
        listAddNodeTail(server.repl_worker_clients_free, c);
    }

    server.rdb_load_ctx = NULL;
    server.request_listeners = serverRequestListenersCreate();
}



#ifdef REDIS_TEST
int clearTestRedisDb() {
    emptyDbStructure(server.db, -1, 0, NULL);
    return 1;
}
int initTestRedisServer() {
    server.maxmemory_policy = MAXMEMORY_FLAG_LFU;
    server.logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);
    server.dbnum = 1;
    server.db = zmalloc(sizeof(redisDb)*server.dbnum);
    /* Create the Redis databases, and initialize other internal state. */
    for (int j = 0; j < server.dbnum; j++) {
        server.db[j].dict = dictCreate(&dbDictType,NULL);
        server.db[j].expires = dictCreate(&dbExpiresDictType,NULL);
        server.db[j].evict = dictCreate(&evictDictType, NULL);
        server.db[j].hold_keys = dictCreate(&objectKeyPointerValueDictType, NULL);
        server.db[j].evict_asap = listCreate();
        server.db[j].expires_cursor = 0;
        // server.db[j].blocking_keys = dictCreate(&keylistDictType,NULL);
        // server.db[j].ready_keys = dictCreate(&objectKeyPointerValueDictType,NULL);
        // server.db[j].watched_keys = dictCreate(&keylistDictType,NULL);
        server.db[j].id = j;
        server.db[j].avg_ttl = 0;
        server.db[j].defrag_later = listCreate();
        listSetFreeMethod(server.db[j].defrag_later,(void (*)(void*))sdsfree);
    }
    return 1;
}
int clearTestRedisServer() {
    return 1;
}
int swapTest(int argc, char **argv, int accurate) {
  int result = 0;
  result += swapDataTest(argc, argv, accurate);
  result += swapWaitTest(argc, argv, accurate);
  result += swapCmdTest(argc, argv, accurate);
  return result;
}
#endif
