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

/* See replicationHandleMasterDisconnection for more details */
void replicationHandleMasterDisconnectionWithoutReconnect(void) {
    /* Fire the master link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);
    server.master = NULL;
    server.repl_down_since = server.unixtime;
}

/* See replicationCacheMaster for more details */
void replicationCacheSwapDrainingMaster(client *c) {
    serverAssert(server.swap_draining_master != NULL && server.cached_master == NULL);
    serverLog(LL_NOTICE, "Caching the disconnected swap draining master state.");

    /* Unlink the client from the server structures. */
    unlinkClient(c);

    /* Reset the master client so that's ready to accept new commands:
     * we want to discard te non processed query buffers and non processed
     * offsets, including pending transactions, already populated arguments,
     * pending outputs to the master. */
    sdsclear(server.swap_draining_master->querybuf);
    server.swap_draining_master->qb_pos = 0;
    server.swap_draining_master->qb_applied = 0;
    if (server.swap_draining_master->repl_data) {
        server.swap_draining_master->repl_data->repl_applied = 0;
        server.swap_draining_master->repl_data->read_reploff = server.swap_draining_master->repl_data->reploff;
    }
    if (c->flag.multi) discardTransaction(c);
    listEmpty(c->reply);
    c->sentlen = 0;
    c->reply_bytes = 0;
    c->bufpos = 0;
    resetClient(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;
    discardCommandQueue(c);

    /* Save the master. Server.master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    server.cached_master = server.swap_draining_master;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }
    /* Invalidate the Sock Name cache. */
    if (c->sockname) {
        sdsfree(c->sockname);
        c->sockname = NULL;
    }

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.master to NULL. */
    if (c->flag.swap_dont_reconnect_master)
        replicationHandleMasterDisconnectionWithoutReconnect();
    else
        replicationHandleMasterDisconnection();
}

void replClientDiscardSwappingState(client *c) {
    listNode *ln;

    ln = listSearchKey(server.swap_repl_swapping_clients, c);
    if (ln == NULL) return;

    listDelNode(server.swap_repl_swapping_clients, ln);
    c->flag.swapping = 0;
    serverLog(LL_NOTICE, "discarded: swapping repl client (reploff=%lld, read_reploff=%lld)",
              c->repl_data ? c->repl_data->reploff : 0LL,
              c->repl_data ? c->repl_data->read_reploff : 0LL);
}

static void replClientUpdateSelectedDb(client *c) {
    int dbid = -1;
    long long value;

    if (c->flag.multi && c->mstate) {
        for (int i = 0; i < c->mstate->count; i++) {
            if (c->mstate->commands[i].cmd->proc == selectCommand) {
                if (getLongLongFromObject(c->mstate->commands[i].argv[1],
                                          &value) == C_OK) {
                    /* The last select in multi will take effect. */
                    dbid = value;
                }
            }
        }
    } else {
        if (c->cmd->proc == selectCommand) {
            if (getLongLongFromObject(c->argv[1], &value) == C_OK) {
                dbid = value;
            }
        }
    }

    if (dbid < 0) {
        /* repl client db not updated */
    } else if (dbid >= server.dbnum) {
        serverLog(LL_WARNING, "repl client select db out of range %d", dbid);
    } else {
        selectDb(c, dbid);
    }
}

/* Move command from repl client to repl worker client. */
static void replCommandDispatch(client *wc, client *c) {
    if (wc->argv) zfree(wc->argv);

    wc->db = c->db;

    /* master client selected db are pass to worker clients when dispatch,
     * so we need to keep track of the selected db as if commands are
     * executed by master clients instantly. */
    replClientUpdateSelectedDb(c);

    wc->argc = c->argc, c->argc = 0;
    wc->argv = c->argv, c->argv = NULL;
    wc->cmd = c->cmd;
    wc->lastcmd = c->lastcmd;
    wc->realcmd = c->realcmd ? c->realcmd : c->cmd;
    wc->flag = c->flag;
    wc->user = c->user;
    wc->swap_cmd_reploff = c->swap_cmd_reploff;
    wc->swap_repl_client = c;

    /* Move repl client mstate to worker client if needed. */
    if (c->flag.multi) {
        c->flag.multi = 0;
        wc->flag.multi = 1;
        if (wc->mstate) freeClientMultiState(wc);
        wc->mstate = c->mstate;
        c->mstate = NULL;
        initClientMultiState(c);
    }

    /* keyrequest_count is dispatched command count. Note that free repl
     * client would be defered untill swapping count drops to 0. */
    c->keyrequests_count++;
}

static void processFinishedReplCommands(void) {
    listNode *ln;
    client *wc, *c;
    struct redisCommand *backup_cmd;
    robj *gtid_repr;

    serverLog(LL_DEBUG, "> processFinishedReplCommands");

    while ((ln = listFirst(server.swap_repl_worker_clients_used))) {
        wc = listNodeValue(ln);
        if (wc->CLIENT_REPL_SWAPPING) break;
        c = wc->swap_repl_client;

        wc->flag.swapping = 0;
        client *old_client = server.current_client;
        c->keyrequests_count--;
        listDelNode(server.swap_repl_worker_clients_used, ln);
        listAddNodeTail(server.swap_repl_worker_clients_free, wc);

        serverAssert(c->flag.primary);

        backup_cmd = c->cmd;
        c->cmd = wc->cmd;
        server.current_client = c;

        if (wc->cmd == server.gtidCommand) {
            gtid_repr = wc->argv[1];
            incrRefCount(gtid_repr);
        } else {
            gtid_repr = NULL;
        }

        if (wc->swap_errcode) {
            clientArgRewritesRestore(wc);
            rejectCommandFormat(c, 0, "Swap failed (code=%d)", wc->swap_errcode);
            wc->swap_errcode = 0;
        } else {
            call(wc, CMD_CALL_FULL);

            /* post call */
            c->woff = server.master_repl_offset;
            if (listLength(server.ready_keys))
                handleClientsBlockedOnKeys();
        }

        c->cmd = backup_cmd;

        commandProcessed(wc);

        long long prev_offset = c->repl_data->reploff;
        /* update reploff */
        if (c->flag.primary) {
            /* transaction commands wont dispatch to worker client untill
             * exec (queued by repl client), so worker client wont have
             * CLIENT_MULTI flag after call(). */
            serverAssert(!wc->flag.multi);
            /* Update the applied replication offset of our master. */
            c->repl_data->reploff = wc->swap_cmd_reploff;
        }

        /* If the client is a master we need to compute the difference
         * between the applied offset before and after processing the buffer,
         * to understand how much of the replication stream was actually
         * applied to the master state: this quantity, and its corresponding
         * part of the replication stream, will be propagated to the
         * sub-replicas and to the replication backlog. */
        if (c->flag.primary) {
            size_t applied = c->repl_data->reploff - prev_offset;
            if (applied) {
                gno_t gno = 0;
                char *uuid = NULL;
                size_t uuid_len = 0;
                if (gtid_repr) {
                    sds repr = gtid_repr->ptr;
                    uuid = uuidGnoDecode(repr, sdslen(repr), &gno, &uuid_len);
                }

                char *stream = c->querybuf + c->repl_data->repl_applied;
                if (!server.repl_slave_repl_all) {
                    ctrip_replicationFeedSlavesFromMasterStream(server.slaves,
                                                                stream, applied, uuid, uuid_len, gno, server.master_repl_offset + 1);
                }
                c->repl_data->repl_applied += applied;
                sdsrange(c->querybuf, c->repl_data->repl_applied, -1);
                c->qb_pos -= c->repl_data->repl_applied;
                serverAssert(c->qb_applied >= (size_t)c->repl_data->repl_applied);
                c->qb_applied -= c->repl_data->repl_applied;
                c->repl_data->repl_applied = 0;
            }
        }

        if (gtid_repr) decrRefCount(gtid_repr);
        server.current_client = old_client;
        clientReleaseLocks(wc, NULL /*ctx unused*/);
    }
    serverLog(LL_DEBUG, "< processFinishedReplCommands");
}

void replWorkerClientKeyRequestFinished(client *wc, swapCtx *ctx) {
    client *c;
    listNode *ln;
    list *repl_swapping_clients;
    UNUSED(ctx);

    serverLog(LL_DEBUG, "> replWorkerClientSwapFinished client(id=%llu,cmd=%s,key=%s)",
              (unsigned long long)wc->id, wc->cmd->name, wc->argc <= 1 ? "" : (sds)wc->argv[1]->ptr);

    DEBUG_MSGS_APPEND(&ctx->msgs, "request-finished", "errcode=%d", ctx->errcode);

    if (ctx->errcode) clientSwapError(wc, ctx->errcode);
    keyRequestBeforeCall(wc, ctx);
    if (ctx->data) {
        swapDataFree(ctx->data, ctx->datactx);
        ctx->data = NULL;
        ctx->datactx = NULL;
    }

    /* Flag swap finished, note that command processing will be defered to
     * processFinishedReplCommands becasue there might be unfinished preceeding swap. */
    wc->keyrequests_count--;
    swapCmdSwapFinished(ctx->key_request->swap_cmd);
    if (wc->keyrequests_count == 0) wc->CLIENT_REPL_SWAPPING = 0;

    processFinishedReplCommands();

    /* Dispatch repl command again for repl client blocked waiting free
     * worker repl client, because repl client might already read repl requests
     * into querybuf, read event will not trigger if we do not parse and
     * process again.  */
    if (!listFirst(server.swap_repl_swapping_clients) ||
        !listFirst(server.swap_repl_worker_clients_free)) {
        serverLog(LL_DEBUG, "< replWorkerClientSwapFinished");
        return;
    }

    repl_swapping_clients = server.swap_repl_swapping_clients;
    server.swap_repl_swapping_clients = listCreate();
    while ((ln = listFirst(repl_swapping_clients))) {
        int swap_result;

        c = listNodeValue(ln);
        /* Swapping repl clients are bound to:
         * - have pending parsed but not processed commands
         * - in server.repl_swapping_client list
         * - flag have CLIENT_SWAPPING */
        serverAssert(c->argc);
        serverAssert(c->flag.swapping);

        /* Must make sure swapping clients satistity above constrains. also
         * note that repl client never call(only dispatch). */
        c->flag.swapping = 0;
        swap_result = submitReplClientRequests(c);
        /* replClientSwap return 1 on dispatch fail, -1 on dispatch success,
         * never return 0. */
        if (swap_result > 0) {
            c->flag.swapping = 1;
        } else {
            commandProcessed(c);
        }

        if (processInputBuffer(c) == C_OK && c->conn) {
            beforeNextClient(c);
        }

        listDelNode(repl_swapping_clients, ln);
    }
    listRelease(repl_swapping_clients);

    serverLog(LL_DEBUG, "< replWorkerClientSwapFinished");
}

int submitReplWorkerClientRequest(client *wc) {
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    getKeyRequests(wc, &result);
    wc->keyrequests_count = result.num;
    submitClientKeyRequests(wc, &result, replWorkerClientKeyRequestFinished, NULL);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
    return result.num;
}

/* Different from original replication stream process, slave.master client
 * might trigger swap and block untill rocksdb IO finish. because there is
 * only one master client so rocksdb IO will be done sequentially, thus slave
 * can't catch up with master.
 * In order to speed up replication stream processing, slave.master client
 * dispatches command to multiple worker client and execute commands when
 * rocks IO finishes. Note that replicated commands swap in-parallel but
 * processed in received order. */
int isGtidExecCommand(client *c) {
    if (!c || !c->cmd || !c->cmd->name) return 0;
    if (c->argc > 3 && c->argv[3] && sdsEncodedObject(c->argv[3]) && !strcasecmp(c->cmd->name, "gtid")) {
        int idx = (strncmp(c->argv[3]->ptr, "/*", 2) == 0) ? 4 : 3;
        if (c->argc > idx && c->argv[idx] && sdsEncodedObject(c->argv[idx]) && !strcasecmp(c->argv[idx]->ptr, "exec")) {
            return 1;
        }
    }
    return 0;
}

void ctrip_replicationFeedSlavesFromMasterStream(list *slaves, char *buf, size_t buflen, const char *uuid, size_t uuid_len, gno_t gno, long long offset) {
    UNUSED(slaves);
    UNUSED(uuid);
    UNUSED(uuid_len);
    UNUSED(gno);
    UNUSED(offset);
    replicationFeedStreamFromPrimaryStream(buf, buflen);
}

static int validateGtidCommandInternal(client *c, int *out_dbid, int *out_start_index, struct serverCommand **out_cmd) {
    int id = 0;
    if (c->argc < 4) {
        addReplyErrorArity(c);
        return C_ERR;
    }
    if (getIntFromObjectOrReply(c, c->argv[2], &id, NULL) != C_OK)
        return C_ERR;

    if (id < 0 || id >= server.dbnum) {
        addReplyError(c, "DB index is out of range");
        return C_ERR;
    }

    int start_index = (sdsEncodedObject(c->argv[3]) && strncmp(c->argv[3]->ptr, "/*", 2) == 0) ? 4 : 3;
    if (c->argc <= start_index) {
        addReplyErrorArity(c);
        return C_ERR;
    }

    if (!sdsEncodedObject(c->argv[start_index])) {
        addReplyError(c, "unknown command in gtid");
        return C_ERR;
    }

    struct serverCommand *cmd = lookupCommand(c->argv + start_index, c->argc - start_index);
    if (!cmd || !cmd->proc || cmd->proc == gtidCommand) {
        addReplyErrorFormat(c, "unknown command `%s` in gtid", (char *)objectGetVal(c->argv[start_index]));
        return C_ERR;
    }

    robj **orig_argv = c->argv;
    int orig_argc = c->argc;
    struct serverCommand *orig_cmd = c->cmd;
    serverDb *orig_db = c->db;

    c->argv = orig_argv + start_index;
    c->argc = orig_argc - start_index;
    c->cmd = cmd;
    c->db = server.db[id];

    int retval = C_OK;
    if ((cmd->arity > 0 && cmd->arity != c->argc) || (c->argc < -cmd->arity)) {
        addReplyErrorArity(c);
        retval = C_ERR;
    } else if (c->flag.script && (cmd->flags & CMD_NOSCRIPT)) {
        addReplyError(c, "This Redis command is not allowed from script");
        retval = C_ERR;
    } else if (server.primary_host && server.repl_replica_ro && !mustObeyClient(c) && (cmd->flags & CMD_WRITE)) {
        addReplyErrorObject(c, shared.roreplicaerr);
        retval = C_ERR;
    } else if (c->user != NULL) {
        int acl_errpos;
        int acl_retval = ACLCheckAllPerm(c, &acl_errpos);
        if (acl_retval != ACL_OK) {
            addACLLogEntry(c, acl_retval, ACL_LOG_CTX_TOPLEVEL, acl_errpos, NULL, NULL);
            addReplyErrorFormat(c, "-NOPERM this user has no permissions to run the '%s' command", cmd->fullname);
            retval = C_ERR;
        }
    }
    if (retval == C_OK && swapCmdShouldBlock(c, cmd)) {
        addReplyErrorFormat(c, "-ERR Can't execute '%s' command: data type is not supported by swap", cmd->fullname);
        retval = C_ERR;
    }

    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;
    c->db = orig_db;

    if (retval == C_OK) {
        if (out_dbid) *out_dbid = id;
        if (out_start_index) *out_start_index = start_index;
        if (out_cmd) *out_cmd = cmd;
    }
    return retval;
}

int validateGtidCommand(client *c) {
    return validateGtidCommandInternal(c, NULL, NULL, NULL);
}

void gtidCommand(client *c) {
    int id = 0, start_index = 0;
    struct serverCommand *cmd = NULL;
    if (validateGtidCommandInternal(c, &id, &start_index, &cmd) != C_OK)
        return;

    if (selectDb(c, id) == C_ERR) {
        addReplyError(c, "DB index is out of range");
        return;
    }

    robj **orig_argv = c->argv;
    int orig_argc = c->argc;
    struct serverCommand *orig_cmd = c->cmd;

    c->argv = orig_argv + start_index;
    c->argc = orig_argc - start_index;
    c->cmd = cmd;

    cmd->proc(c);

    if (cmd->proc == execCommand || (cmd->flags & CMD_MODULE)) {
        preventCommandPropagation(c);
    }

    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;
}

void gtidxCommand(client *c) {
    addReplyError(c, "ERR gtidx not supported");
}

int submitReplClientRequests(client *c) {
    client *wc;
    listNode *ln;

    c->swap_cmd_reploff = c->repl_data->read_reploff - sdslen(c->querybuf) + c->qb_applied;
    serverAssert(!c->flag.swapping);
    if (!(ln = listFirst(server.swap_repl_worker_clients_free))) {
        /* return swapping if there are no worker to dispatch, so command
         * processing loop would break out.
         * Note that peer client might register no rocks callback but repl
         * stream read and parsed, we need to processInputBuffer again. */
        listAddNodeTail(server.swap_repl_swapping_clients, c);
        /* Note repl client will be flagged CLIENT_SWAPPING when return. */
        return 1;
    }

    wc = listNodeValue(ln);
    serverAssert(wc && !wc->CLIENT_REPL_SWAPPING);

    /* Because c is a repl client, only normal multi {cmd} exec will be
     * received (multiple multi, exec without multi, ... will no happen) */
    if (c->cmd->proc == multiCommand) {
        serverAssert(!c->flag.multi);
        if (!c->mstate) initClientMultiState(c);
        c->mstate->transaction_db_id = c->db->id;
        c->flag.multi = 1;
    } else if (c->flag.multi &&
               c->cmd->proc != execCommand &&
               !isGtidExecCommand(c)) {
        serverPanic("command should be already queued.");
    } else {
        /* either vanilla command or transaction are stored in client state,
         * client is ready to dispatch now. */
        replCommandDispatch(wc, c);

        /* swap data for replicated commands, note that command will be
         * processed later in processFinishedReplCommands untill all preceeding
         * commands finished. */
        submitReplWorkerClientRequest(wc);
        wc->CLIENT_REPL_SWAPPING = wc->keyrequests_count;

        listDelNode(server.swap_repl_worker_clients_free, ln);
        listAddNodeTail(server.swap_repl_worker_clients_used, wc);
    }

    /* process repl commands in received order (not swap finished order) so
     * that slave is consistent with master. */
    processFinishedReplCommands();

    /* return dispatched(-1) when repl dispatched command to workers, caller
     * should skip call and continue command processing loop. */
    return -1;
}

sds genSwapReplInfoString(sds info) {
    info = sdscatprintf(info,
                        "swap_repl_workers:free=%lu,used=%lu,swapping=%lu\r\n",
                        listLength(server.swap_repl_worker_clients_free),
                        listLength(server.swap_repl_worker_clients_used),
                        listLength(server.swap_repl_swapping_clients));
    return info;
}

bool isSwapInfoSupported(void) {
    if (server.swap_swap_info_supported == SWAP_INFO_SUPPORTED_YES) return true;
    if (server.swap_swap_info_supported == SWAP_INFO_SUPPORTED_NO) return false;

    /* SWAP_INFO_SUPPORTED_AUTO */
    /* depends on capa of all slaves,
     * once there is one slave without capa of swap.info, return false. */

    listNode *ln;
    listIter li;

    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;

        if (!(slave->slave_capa & SLAVE_CAPA_SWAP_INFO)) {
            return false;
        }
    }
    return true;
}

/* The swap.info command, propagate system info to slave.
 * SWAP.INFO <subcommand> [<arg> [value] [opt] ...]
 *
 * subcommand supported:
 * SWAP.INFO SST-AGE-LIMIT <sst age limit> */
void swapBuildSwapInfoSstAgeLimitCmd(robj *argv[3], long long sst_age_limit) {
    argv[0] = swap_shared.swap_info;
    argv[1] = swap_shared.sst_age_limit;
    argv[2] = createObject(OBJ_STRING, sdsfromlonglong(sst_age_limit));
}

void swapDestorySwapInfoSstAgeLimitCmd(robj *argv[3]) {
    decrRefCount(argv[2]);
}

void swapPropagateSwapInfoCmd(int argc, robj **argv) {
    if (server.swap_swap_info_propagate_mode == SWAP_INFO_PROPAGATE_BY_SWAP_INFO) {
        if (!isSwapInfoSupported()) return;
        replicationFeedSlaves(server.slaves, 0, argv, argc);
        return;
    }

    /* SWAP_INFO_PROPAGATE_BY_PING */
    sds *argv_str = zmalloc(sizeof(sds) * argc);
    for (int i = 0; i < argc; i++) {
        argv_str[i] = argv[i]->ptr;
    }

    sds ping_argv_str = swapEncodeSwapInfo(argc, argv_str);

    robj *ping_argv[2];
    ping_argv[0] = shared.ping;
    ping_argv[1] = createObject(OBJ_STRING, ping_argv_str);

    replicationFeedSlaves(server.slaves, 0, ping_argv, 2);
    decrRefCount(ping_argv[1]);
    zfree(argv_str);
    return;
}

sds swapEncodeSwapInfo(int swap_info_argc, sds *swap_info_argv) {
    return sdsjoinsds(swap_info_argv, swap_info_argc, " ", 1);
}

sds *swapDecodeSwapInfo(sds argv, int *swap_info_argc) {
    return sdssplitlen(argv, sdslen(argv), " ", 1, swap_info_argc);
}

void swapApplySwapInfo(int swap_info_argc, sds *swap_info_argv) {
    if (strcasecmp(swap_info_argv[0], "swap.info")) {
        return;
    }

    if (swap_info_argc == 3 && !strcasecmp(swap_info_argv[1], "SST-AGE-LIMIT")) {
        /* SWAP.INFO SST-AGE-LIMIT <sst age limit> */
        long long sst_age_limit = 0;
        if (isSdsRepresentableAsLongLong(swap_info_argv[2], &sst_age_limit) == C_OK) {
            server.swap_ttl_compact_ctx->expire_stats->sst_age_limit = sst_age_limit;
        }
        return;
    }
}

#ifdef REDIS_TEST

int swapReplTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    int error = 0;

    TEST("swap info: sst-limit-age encode decode") {
        sds *swap_info_argv = zmalloc(sizeof(sds) * 3);
        swap_info_argv[0] = sdsnew("SWAP.INFO");
        swap_info_argv[1] = sdsnew("SST-AGE-LIMIT");
        swap_info_argv[2] = sdsfromlonglong(1111);
        sds swap_info = swapEncodeSwapInfo(3, swap_info_argv);

        int argc = 0;
        sds *argv = swapDecodeSwapInfo(swap_info, &argc);

        test_assert(argc == 3);
        test_assert(!sdscmp(argv[2], swap_info_argv[2]));
        test_assert(!sdscmp(argv[1], swap_info_argv[1]));
        test_assert(!sdscmp(argv[0], swap_info_argv[0]));

        sdsfree(swap_info_argv[0]);
        sdsfree(swap_info_argv[1]);
        sdsfree(swap_info_argv[2]);
        zfree(swap_info_argv);

        sdsfree(swap_info);

        sdsfreesplitres(argv, argc);
    }

    return error;
}

#endif
