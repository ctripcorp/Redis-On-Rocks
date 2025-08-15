#include "server.h"

void refullsyncCommand(client *c) {

    sds client = catClientInfoString(sdsempty(),c);
    serverLog(LL_NOTICE,"refullsync called (user request from '%s')", client);
    sdsfree(client);

    disconnectSlaves(); /* Force our slaves to resync with us as well. */
    ctrip_freeReplicationBacklog(); /* Don't allow our chained slaves to PSYNC. */

    addReply(c,shared.ok);
}

void xslaveofCommand(client *c) {
    /* SLAVEOF is not allowed in cluster mode as replication is automatically
     * configured using the current address of the master node. */
    if (server.cluster_enabled) {
        addReplyError(c,"SLAVEOF not allowed in cluster mode.");
        return;
    }

    /* The special host/port combination "NO" "ONE" turns the instance
     * into a master. Otherwise the new master address is set. */
    if (!strcasecmp(c->argv[1]->ptr,"no") &&
        !strcasecmp(c->argv[2]->ptr,"one")) {
        if (server.masterhost) {
            replicationUnsetMaster();
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,"(XSLAVEOF)MASTER MODE enabled (user request from '%s')",
                client);
            sdsfree(client);
        }
    } else {
        long port;

        if ((getLongFromObjectOrReply(c, c->argv[2], &port, NULL) != C_OK))
            return;

        /* Check if we are already attached to the specified slave */
        if (server.masterhost && !strcasecmp(server.masterhost,c->argv[1]->ptr)
            && server.masterport == port) {
            serverLog(LL_NOTICE,"XSLAVE OF would result into synchronization with the master we are already connected with. No operation performed.");
            addReplySds(c,sdsnew("+OK Already connected to specified master\r\n"));
            return;
        }
        /* There was no previous master or the user specified a different one,
         * we can continue. */
        replicationSetMaster(c->argv[1]->ptr, port);
        sds client = catClientInfoString(sdsempty(),c);
        serverLog(LL_NOTICE,"XSLAVE OF %s:%d enabled (user request from '%s')",
            server.masterhost, server.masterport, client);
        sdsfree(client);

        /* reconnect to master immdediately */
        serverLog(LL_NOTICE,"XSLAVE OF %s:%d, connect to master immediately", server.masterhost, server.masterport);
        replicationCron();
    }
    addReply(c,shared.ok);
}

static int clients_write_task_num = 0;

int clientsWriteEventHandler(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);
    clients_write_task_num--;
    handleClientsWithPendingWrites();
    return AE_NOMORE;
}

void tryRegisterClientsWriteEvent(void) {
    if (clients_write_task_num == 0) {
        if (aeCreateTimeEvent(server.el, 0, clientsWriteEventHandler, NULL, NULL) == AE_ERR) {
            serverLog(LL_NOTICE,"Failed to create time event for clients write.");
        } else {
            clients_write_task_num++;
        }
    }
}

void dirtyArraysTryAlloc(size_t n) {
    if (n == 0) return;
    if (n > server.dirty_cap) {
        zfree(server.dirty_subkeys);
        zfree(server.dirty_sublens);
        server.dirty_subkeys = zmalloc(sizeof(sds)*n*2);
        server.dirty_sublens = zmalloc(sizeof(size_t)*n*2);
        server.dirty_cap = n*2;
    }
}

sds *dirtyArraysSubkeys(void) {
    return server.dirty_subkeys;
}

size_t *dirtyArraysSublens(void) {
    return server.dirty_sublens;
}

void dirtyArraysFree(void) {
    zfree(server.dirty_subkeys);
    zfree(server.dirty_sublens);
    server.dirty_subkeys = NULL;
    server.dirty_sublens = NULL;
    server.dirty_cap = 0;
}
