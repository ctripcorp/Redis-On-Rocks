#include "ctrip_storage_request.h"
#include "server.h"
#include "ctrip_storage_debug.h"
#include "ctrip_storage.h"
#include "versions/functions.h"
static void processFinishedReplCommands() {
    listNode *ln;
    client *wc, *c;
    struct redisCommand *backup_cmd;
    robj *gtid_repr;

    serverLog(LL_DEBUG, "> processFinishedReplCommands");

    while ((ln = listFirst(server.storage.swap_repl_worker_clients_used))) {
        wc = listNodeValue(ln);
        if (wc->deferred_cmd->CLIENT_REPL_SWAPPING) break;
        c = wc->deferred_cmd->swap_repl_client;

        wc->deferred_cmd->flags &= ~CLIENT_DEFERRED_SWAPPING;
        c->deferred_cmd->keyrequests_count--;
        listDelNode(server.storage.swap_repl_worker_clients_used, ln);

        serverAssert(c->flags&CLIENT_MASTER);
        backup_cmd = c->cmd;
        c->cmd = wc->cmd;
        client *old_client = server.current_client;
        server.current_client = c;

        if (isGtidExecCommand(wc)) {
            gtid_repr = wc->argv[1];
            incrRefCount(gtid_repr);
        } else {
            gtid_repr = NULL;
        }

        if (wc->deferred_cmd->swap_errcode) {
            rejectCommandFormat(c,"Swap failed (code=%d)",wc->deferred_cmd->swap_errcode);
            wc->deferred_cmd->swap_errcode = 0;
        } else {
            call(wc, CMD_CALL_FULL);

            /* post call */
            c->woff = server.master_repl_offset;
            if (listLength(server.ready_keys))
                handleClientsBlockedOnKeys();
        }

        c->cmd = backup_cmd;

        commandProcessed(wc);
        long long prev_offset = c->reploff;
        /* update reploff */
        if (c->flags&CLIENT_MASTER) {
            /* transaction commands wont dispatch to worker client untill
             * exec (queued by repl client), so worker client wont have
             * CLIENT_MULTI flag after call(). */
            serverAssert(!(wc->flags & CLIENT_MULTI));
            /* Update the applied replication offset of our master. */
            c->reploff = wc->deferred_cmd->swap_cmd_reploff;
        }

		/* If the client is a master we need to compute the difference
		 * between the applied offset before and after processing the buffer,
		 * to understand how much of the replication stream was actually
		 * applied to the master state: this quantity, and its corresponding
		 * part of the replication stream, will be propagated to the
		 * sub-replicas and to the replication backlog. */
		if ((c->flags&CLIENT_MASTER)) {
			size_t applied = c->reploff - prev_offset;
			if (applied) {
                //TODO gtid update stream
                // gno_t gno = 0;
                // char *uuid = NULL;
                // size_t uuid_len = 0;
                // if (gtid_repr) {
                //     sds repr = gtid_repr->ptr;
                //     uuid = uuidGnoDecode(repr,sdslen(repr),&gno,&uuid_len);
                // }

				// if(!server.repl_slave_repl_all){
				// 	ctrip_replicationFeedSlavesFromMasterStream(
				// 			c->querybuf+c->repl_applied, applied, uuid,uuid_len,gno,server.master_repl_offset+1);
				// }
                c->repl_applied += applied;
			}
		}

        if (gtid_repr) decrRefCount(gtid_repr);
        server.current_client = old_client;
        clientReleaseLocks(wc,NULL/*ctx unused*/);
        listAddNodeTail(server.storage.swap_repl_worker_clients_free, wc);
    }
    serverLog(LL_DEBUG, "< processFinishedReplCommands");
}

void replWorkerClientKeyRequestFinished(client *wc, swapCtx *ctx) {
    client *c;
    listNode *ln;
    list *repl_swapping_clients;
    UNUSED(ctx);

    serverLog(LL_DEBUG, "> replWorkerClientSwapFinished client(id=%ld,cmd=%s,key=%s)",
        wc->id,wc->cmd->fullname,wc->argc <= 1 ? "": (sds)wc->argv[1]->ptr);

    DEBUG_MSGS_APPEND(&ctx->msgs, "request-finished", "errcode=%d",ctx->errcode);

    if (ctx->errcode) clientSwapError(wc,ctx->errcode);
    keyRequestBeforeCall(wc,ctx);
    if (ctx->data && ctx->data->value != NULL) {
        decrRefCount(ctx->data->value);
        ctx->data->value = NULL;
    }
    /* Flag swap finished, note that command processing will be defered to
     * processFinishedReplCommands becasue there might be unfinished preceeding swap. */
    wc->deferred_cmd->keyrequests_count--;
    swapCmdSwapFinished(ctx->key_request->swap_cmd);
    if (wc->deferred_cmd->keyrequests_count == 0) wc->deferred_cmd->CLIENT_REPL_SWAPPING = 0;

    processFinishedReplCommands();

    /* Dispatch repl command again for repl client blocked waiting free
     * worker repl client, because repl client might already read repl requests
     * into querybuf, read event will not trigger if we do not parse and
     * process again.  */
    if (!listFirst(server.storage.swap_repl_swapping_clients) ||
            !listFirst(server.storage.swap_repl_worker_clients_free)) {
        serverLog(LL_DEBUG, "< replWorkerClientSwapFinished");
        return;
    }

    repl_swapping_clients = server.storage.swap_repl_swapping_clients;
    server.storage.swap_repl_swapping_clients = listCreate();
    while ((ln = listFirst(repl_swapping_clients))) {
        int swap_result;

        c = listNodeValue(ln);
        /* Swapping repl clients are bound to:
         * - have pending parsed but not processed commands
         * - in server.repl_swapping_client list
         * - flag have CLIENT_DEFERRED_SWAPPING */
        serverAssert(c->argc);
        serverAssert(c->deferred_cmd->flags & CLIENT_DEFERRED_SWAPPING);

        /* Must make sure swapping clients satistity above constrains. also
         * note that repl client never call(only dispatch). */
        c->deferred_cmd->flags &= ~CLIENT_DEFERRED_SWAPPING;
        swap_result = submitReplClientRequests(c);
        /* replClientSwap return 1 on dispatch fail, -1 on dispatch success,
         * never return 0. */
        if (swap_result > 0) {
            c->deferred_cmd->flags |= CLIENT_DEFERRED_SWAPPING;
        } else {
            commandProcessed(c);
        }

        processInputBuffer(c);

        listDelNode(repl_swapping_clients,ln);
    }
    listRelease(repl_swapping_clients);

    serverLog(LL_DEBUG, "< replWorkerClientSwapFinished");
}



int submitReplWorkerClientRequest(client *wc) {
    getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
    getKeyRequests(wc, &result);
    wc->deferred_cmd->keyrequests_count = result.num;
    submitClientKeyRequests(wc,&result,replWorkerClientKeyRequestFinished,NULL);
    releaseKeyRequests(&result);
    getKeyRequestsFreeResult(&result);
    return result.num;
}
#include "versions/functions.h"

static void replClientUpdateSelectedDb(client *c) {
    int dbid = -1;
    long long value;

    if (c->flags & CLIENT_MULTI) {
        for (int i = 0; i < c->mstate.count; i++) {
            struct redisCommand* cmd = clientMstateGetCmd(c, i);
            if (cmd->proc == selectCommand) {
                if (getLongLongFromObject(clientMstateGetArgv(c, i)[1],
                            &value) == C_OK) {
                    /* The last select in multi will take effect. */
                    dbid = value;
                }
            }
        }
    } else {
        if (c->cmd->proc == selectCommand) {
            if (getLongLongFromObject(c->argv[1],&value) == C_OK) {
                dbid = value;
            }
        }
    }

    if (dbid < 0) {
        /* repl client db not updated */
    } else if (dbid >= server.dbnum) {
        serverLog(LL_WARNING,"repl client select db out of range %d",dbid);
    } else {
        selectDb(c,dbid);
    }
}


/* Move command from repl client to repl worker client. */
static void replCommandDispatch(client *wc, client *c) {
    /* wc may still have argv from last dispatched command, free it safely. */
    if (wc->argv) freeClientArgv(wc);

    wc->db = c->db;

    /* master client selected db are pass to worker clients when dispatch,
     * so we need to keep track of the selected db as if commands are
     * executed by master clients instantly. */
    replClientUpdateSelectedDb(c);

    wc->argc = c->argc, c->argc = 0;
    wc->argv = c->argv, c->argv = NULL;
    wc->cmd = c->cmd;
    wc->realcmd = c->realcmd;
    wc->lastcmd = c->lastcmd;
    wc->flags = c->flags;
    wc->deferred_cmd->swap_cmd_reploff = c->deferred_cmd->swap_cmd_reploff;
    wc->deferred_cmd->swap_repl_client = c;

    /* Move repl client mstate to worker client if needed. */
    if (c->flags & CLIENT_MULTI) {
        c->flags &= ~CLIENT_MULTI;
        wc->mstate = c->mstate;
        initClientMultiState(c);
    }

    /* keyrequest_count is dispatched command count. Note that free repl
     * client would be defered untill swapping count drops to 0. */
    c->deferred_cmd->keyrequests_count++;
}

/* Different from original replication stream process, slave.master client
 * might trigger swap and block untill rocksdb IO finish. because there is
 * only one master client so rocksdb IO will be done sequentially, thus slave
 * can't catch up with master.
 * In order to speed up replication stream processing, slave.master client
 * dispatches command to multiple worker client and execute commands when
 * rocks IO finishes. Note that replicated commands swap in-parallel but
 * processed in received order. */
int submitReplClientRequests(client *c) {
    client *wc;
    listNode *ln;

    c->deferred_cmd->swap_cmd_reploff = c->read_reploff - sdslen(c->querybuf) + c->qb_pos;
    serverAssert(!(c->deferred_cmd->flags & CLIENT_DEFERRED_SWAPPING));
    if (!(ln = listFirst(server.storage.swap_repl_worker_clients_free))) {
        /* return swapping if there are no worker to dispatch, so command
         * processing loop would break out.
         * Note that peer client might register no rocks callback but repl
         * stream read and parsed, we need to processInputBuffer again. */
        listAddNodeTail(server.storage.swap_repl_swapping_clients, c);
        /* Note repl client will be flagged CLIENT_SWAPPING when return. */
        return 1;
    }

    wc = listNodeValue(ln);
    serverAssert(wc && !(wc->deferred_cmd->flags & CLIENT_DEFERRED_SWAPPING));

    /* Because c is a repl client, only normal multi {cmd} exec will be
     * received (multiple multi, exec without multi, ... will no happen) */
    if (c->cmd->proc == multiCommand) {
        serverAssert(!(c->flags & CLIENT_MULTI));
        c->flags |= CLIENT_MULTI;
    } else if (c->flags & CLIENT_MULTI &&
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
        wc->deferred_cmd->CLIENT_REPL_SWAPPING = wc->deferred_cmd->keyrequests_count;

        listDelNode(server.storage.swap_repl_worker_clients_free, ln);
        listAddNodeTail(server.storage.swap_repl_worker_clients_used, wc);
    }

    /* process repl commands in received order (not swap finished order) so
     * that slave is consistent with master. */
    processFinishedReplCommands();

    /* return dispatched(-1) when repl dispatched command to workers, caller
     * should skip call and continue command processing loop. */
    return -1;
}