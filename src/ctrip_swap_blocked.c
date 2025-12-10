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
#include "server.h"

typedef struct swapUnblockedKeyChain  {
    robj* key; /* key root */
    redisDb* db;
    long long version;
    int keyrequests_count;
    dict* keys;
    long long swap_err_count;
    list* swap_data_wrong_type_keys;
    client* client;
} swapUnblockedKeyChain ;

#define waitIoDictType  objectKeyPointerValueDictType

void incrSwapUnBlockCtxVersion() {
    server.swap_dependency_block_ctx->version++;
}

swapUnblockedKeyChain* createSwapUnblockedKeyChain(client* c,redisDb* db, robj* key) {
    swapUnblockedKeyChain* chain = zmalloc(sizeof(swapUnblockedKeyChain));
    chain->version = server.swap_dependency_block_ctx->version;
    incrRefCount(key);
    chain->key = key;
    chain->db = db;
    chain->keyrequests_count = 0;
    chain->swap_err_count = 0;
    chain->keys = dictCreate(&waitIoDictType);
    chain->swap_data_wrong_type_keys = listCreate();
    chain->client = c;
    return chain;
}

void releaseSwapUnblockedKeyChain(void* val) {
    swapUnblockedKeyChain* chain = val;
    // freeClient(wait_io->c);
    if (chain->key) {
        decrRefCount(chain->key);
    }
    if (chain->keys) {
        dictRelease(chain->keys);
    }
    if (chain->swap_data_wrong_type_keys) {
        listRelease(chain->swap_data_wrong_type_keys);
    }
    zfree(chain);
}

swapUnblockCtx* createSwapUnblockCtx() {
    swapUnblockCtx* swap_dependency_block_ctx = zmalloc(sizeof(swapUnblockCtx));
    swap_dependency_block_ctx->version = 0;
    swap_dependency_block_ctx->swap_total_count = 0;
    swap_dependency_block_ctx->swapping_count = 0;
    /* version change will retry swap*/
    swap_dependency_block_ctx->swap_retry_count = 0;
    swap_dependency_block_ctx->swap_err_count = 0;
    swap_dependency_block_ctx->mock_clients = zmalloc(server.dbnum*sizeof(client*));
    for (int i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->cmd = lookupCommandByCString("brpoplpush");
        c->db = server.db+i;
        c->swap_lock_mode = SWAP_LOCK_SHARED;
        swap_dependency_block_ctx->mock_clients[i] = c;
    }
    return swap_dependency_block_ctx;
}

void releaseSwapUnblockCtx(swapUnblockCtx* swap_dependency_block_ctx) {
    for (int i = 0; i < server.dbnum; i++) {
        freeClient(swap_dependency_block_ctx->mock_clients[i]);
    }
    zfree(swap_dependency_block_ctx->mock_clients);
    zfree(swap_dependency_block_ctx);
}

void findSwapBlockedListKeyChain(redisDb* db, robj* key, dict* key_sets) {
    dictEntry *de = dictFind(db->blocking_keys,key);
    if(de) {
        listIter li;
        listNode *ln;
        list *clients = dictGetVal(de);
        listRewind(clients,&li);
        while ((ln = listNext(&li))) {
            client *receiver = listNodeValue(ln);
            if (receiver->bstate.btype != BLOCKED_LIST) continue;
            if (receiver->cmd->proc == blmoveCommand || 
                receiver->cmd->proc == brpoplpushCommand) {
                robj* dstkey = receiver->argv[2];
                if (sdscmp(key->ptr, dstkey->ptr) == 0) continue;
                if (dictAdd(key_sets, dstkey, NULL) != C_OK)  continue;
                incrRefCount(dstkey);
                findSwapBlockedListKeyChain(db, dstkey, key_sets);
            }
            
            
        }
    }

}


void unblockClientOnKey(client *c, robj *key);
void handleBlockedOnListKey(client* client, redisDb* db, robj* key) {
    robj *o = lookupKeyWrite(db, key);
    if (o == NULL || o->type != OBJ_LIST) {
        return;
    }
    readyList rl = {
        .db = db,
        .key = key
    };
    unblockClientOnKey(client, key);
}


void continueServeClientsBlockedOnListKeys(client* client,redisDb* db, robj* key) {
    list* _ready_keys = server.ready_keys;
    server.ready_keys = listCreate();
    handleBlockedOnListKey(client, db, key);

    handleClientsBlockedOnKeys();
    serverAssert(listLength(server.ready_keys) == 0);
    listRelease(server.ready_keys);
    server.ready_keys = _ready_keys;
}

void blockedOnListKeyClientKeyRequestFinished(client *c, swapCtx *ctx) {
    swapUnblockedKeyChain* chain = ctx->pd;
    dictIterator* di = NULL;
    dictEntry *de = NULL;
    if (ctx->errcode != 0) {
        if (ctx->errcode == SWAP_ERR_DATA_WRONG_TYPE_ERROR) {
            serverAssert(ctx->key_request[0].key != NULL);
            chain->swap_data_wrong_type_keys = listAddNodeTail(chain->swap_data_wrong_type_keys, ctx->key_request[0].key);
        } else {
            chain->swap_err_count++;
        }
    } else {
        keyRequestBeforeCall(c, ctx);
    }
    dictAdd(chain->keys, ctx->data->key, ctx->swap_lock);
    chain->keyrequests_count--;

    if (chain->keyrequests_count == 0) {
        if (chain->swap_err_count > 0) {
            server.swap_dependency_block_ctx->swap_err_count++;
            signalKeyAsReady(chain->db, chain->key, OBJ_LIST);
        } else if(chain->version != server.swap_dependency_block_ctx->version) {
            server.swap_dependency_block_ctx->swap_retry_count++;
            signalKeyAsReady(chain->db, chain->key, OBJ_LIST);
        } else {
            continueServeClientsBlockedOnListKeys(chain->client, chain->db, chain->key);
        }
        di = dictGetIterator(chain->keys);
        while ((de = dictNext(di)) != NULL) {
            lockUnlock(dictGetVal(de));
        }
        dictReleaseIterator(di);
        releaseSwapUnblockedKeyChain(chain);
        server.swap_dependency_block_ctx->swapping_count--;
    }

}

void submitSwapBlockedClientRequest(client* c, client* receiver,readyList *rl, dict* key_sets) {
    int dbid = rl->db->id;
    dictIterator* di = dictGetIterator(key_sets);
    dictEntry* de;
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    getKeyRequestsPrepareResult(&result, dictSize(key_sets));
    while (NULL != (de = dictNext(di))) {
        robj* rkey = dictGetKey(de);
        incrRefCount(rkey);
        getKeyRequestsSwapBlockedLmove(dbid, SWAP_IN, c->cmd->intention_flags, CMD_SWAP_DATATYPE_LIST,
            rkey, &result, -1, -1, 1/*num_ranges*/, -1L, -1L, (int)0);
    }
    dictReleaseIterator(di);
    swapUnblockedKeyChain* chain = createSwapUnblockedKeyChain(receiver, rl->db, rl->key);
    chain->keyrequests_count = result.num;
    server.swap_dependency_block_ctx->swap_total_count++;
    server.swap_dependency_block_ctx->swapping_count++;
    submitClientKeyRequests(c, &result, blockedOnListKeyClientKeyRequestFinished, chain);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
}

void swapUnblockClientOnKey(robj* o, client* receiver, readyList* rl) {
    dict* key_sets = dictCreate(&waitIoDictType);
    serverAssert(dictAdd(key_sets, rl->key, NULL) == C_OK);
    incrRefCount(rl->key);
    findSwapBlockedListKeyChain(rl->db, rl->key, key_sets);
    if (dictSize(key_sets) == 1) { /* no swap key */
        unblockClientOnKey(receiver, rl->key);
        goto end;
    }
    //create submit
    client* mock_client = server.swap_dependency_block_ctx->mock_clients[rl->db->id];
    submitSwapBlockedClientRequest(mock_client,receiver, rl, key_sets);

end:
    dictRelease(key_sets);

}