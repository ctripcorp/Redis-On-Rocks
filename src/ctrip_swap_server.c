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

struct swapSharedObjectsStruct swap_shared;

void createSwapSharedObjects(void) {
    swap_shared.emptystring = createObject(OBJ_STRING,sdsnew(""));
    swap_shared.outofdiskerr = createObject(OBJ_STRING,sdsnew(
        "-ERR command not allowed when used disk > 'swap-max-db-size'.\r\n"));
    swap_shared.rocksdbdiskerr = createObject(OBJ_STRING,sdsnew(
        "-ERR command not allowed when rocksdb disk error.\r\n"));
    swap_shared.swap_info = createStringObject("swap.info",9);
    swap_shared.sst_age_limit = createStringObject("SST-AGE-LIMIT",13);
}

void swapInitServerConfig(void) {
    /* ignore accept */
    server.ctrip_monitor_port = 0;

    /* Swap batch limits presets. */
    for (int j = 0; j < SWAP_TYPES; j++)
        server.swap_batch_limits[j] = swapBatchLimitsDefaults[j];
}

void ctrip_ignoreAcceptEvent() {
    if (server.ctrip_ignore_accept) return;
    serverLog(LL_NOTICE, "[ctrip] ignore accept for clients overflow.");
    server.ctrip_ignore_accept = 1;

    int j;
    for (j = 0; j < server.ipfd.count; j++) {
        if (server.ipfd.fd[j] == -1) continue;
        aeDeleteFileEvent(server.el, server.ipfd.fd[j], AE_READABLE);
    }
    for (j = 0; j < server.tlsfd.count; j++) {
        if (server.tlsfd.fd[j] == -1) continue;
        aeDeleteFileEvent(server.el, server.tlsfd.fd[j], AE_READABLE);
    }
    if (server.sofd > 0) {
        aeDeleteFileEvent(server.el,server.sofd,AE_READABLE);
    }
}

unsigned long getClusterConnectionsCount(void);
void ctrip_resetAcceptIgnore() {
    if (!server.ctrip_ignore_accept) return;
    server.ctrip_ignore_accept = 0;
    serverLog(LL_NOTICE, "[ctrip] reset accept ignore, current clients %lu/%u",
              listLength(server.clients) + getClusterConnectionsCount(), server.maxclients);

    if (createSocketAcceptHandler(&server.ipfd, acceptTcpHandler) != C_OK) {
        serverPanic("Unrecoverable error creating TCP socket accept handler.");
    }
    if (createSocketAcceptHandler(&server.tlsfd, acceptTLSHandler) != C_OK) {
        serverPanic("Unrecoverable error creating TLS socket accept handler.");
    }
    if (server.sofd > 0 &&
        aeCreateFileEvent(server.el,server.sofd,AE_READABLE, acceptUnixHandler,NULL) == AE_ERR) {
        serverPanic("Unrecoverable error creating server.sofd file event.");
    }
}

/* keep work even if clients overflow. */
void ctrip_initMonitorAcceptor() {
    serverAssert(server.ctrip_monitor_port > 0);
    server.ctrip_monitorfd = anetTcpServer(server.neterr,server.ctrip_monitor_port,"127.0.0.1",server.tcp_backlog);

    if (server.ctrip_monitorfd == ANET_ERR) {
        serverPanic("Unrecoverable error binding ctrip monitor port.");
        exit(1);
    }

    anetNonBlock(NULL,server.ctrip_monitorfd);
    anetCloexec(server.ctrip_monitorfd);

    if (aeCreateFileEvent(server.el, server.ctrip_monitorfd, AE_READABLE, acceptMonitorHandler,NULL) == AE_ERR) {
        serverPanic("Unrecoverable error creating ctrip monitor file event.");
        exit(1);
    }
}

void swapInitServer(void) {
    int i, j;

    server.clients_to_free = listCreate();
    server.ctrip_ignore_accept = 0;
    server.ctrip_monitorfd = 0;
    server.swap_lastsave = time(NULL);
    server.swap_rdb_size = 0;
    server.swap_inprogress_batch = 0;
    server.swap_inprogress_count = 0;
    server.swap_inprogress_memory = 0;
    server.swap_error_count = 0;
    server.swap_load_paused = 0;
    server.swap_load_err_cnt = 0;
    server.swap_rocksdb_stats_collect_interval_ms = 2000;
    server.swap_txid = 0;
    server.swap_rewind_type = SWAP_REWIND_OFF;
    server.swap_torewind_clients = listCreate();
    server.swap_rewinding_clients = listCreate();
    server.swap_draining_master = NULL;
    server.swap_string_switched_to_bitmap_count = 0;
    server.swap_bitmap_switched_to_string_count = 0;
    server.rocksdb_disk_used = 0;
    server.rocksdb_disk_error = 0;
    server.rocksdb_disk_error_since = 0;
    server.rocksdb_checkpoint = NULL;
    server.rocksdb_checkpoint_dir = NULL;
    server.rocksdb_rdb_checkpoint_dir = NULL;
    server.rocksdb_internal_stats = NULL;
    server.util_task_manager = createRocksdbUtilTaskManager();

    asyncCompleteQueueInit();
    parallelSyncInit(server.ps_parallism_rdb);

    if (server.ctrip_monitor_port > 0) {
        ctrip_initMonitorAcceptor();
    }

    for (j = 0; j < server.dbnum; j++) {
        server.db[j].meta = dictCreate(&objectMetaDictType, NULL);
        server.db[j].dirty_subkeys = dictCreate(&dbDirtySubkeysDictType, NULL);
        server.db[j].evict_asap = listCreate();
        server.db[j].cold_keys = 0;
        server.db[j].scan_expire = scanExpireCreate();
        server.db[j].randomkey_nextseek = NULL;
        server.db[j].cold_filter = NULL;
    }

    initStatsSwap();
    swapInitVersion();

    server.swap_eviction_ctx = swapEvictionCtxCreate();

    server.swap_load_inprogress_count = 0;

    server.evict_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->cmd = lookupCommandByCString("SWAP.EVICT");
        c->db = server.db+i;
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.evict_clients[i] = c;
    }

    server.load_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->cmd = lookupCommandByCString("SWAP.LOAD");
        c->db = server.db+i;
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.load_clients[i] = c;
    }

    server.expire_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->cmd = lookupCommandByCString("SWAP.EXPIRED");
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.expire_clients[i] = c;
    }

    server.scan_expire_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->cmd = lookupCommandByCString("SWAP.SCANEXPIRE");
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.scan_expire_clients[i] = c;
    }

    server.ttl_clients = zmalloc(server.dbnum*sizeof(client*));
    for (i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->db = server.db+i;
        c->cmd = lookupCommandByCString("ttl");
        c->client_hold_mode = CLIENT_HOLD_MODE_EVICT;
        server.ttl_clients[i] = c;
    }

    server.mutex_client = createClient(NULL);
    server.mutex_client->cmd = lookupCommandByCString("SWAP.MUTEXOP");
    server.mutex_client->client_hold_mode = CLIENT_HOLD_MODE_EVICT;

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

    swapLockCreate();

    server.swap_scan_sessions = swapScanSessionsCreate(server.swap_scan_session_bits);

    server.swap_dependency_block_ctx = createSwapUnblockCtx();

    for (i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        db->cold_filter = coldFilterCreate();
    }

    server.swap_batch_ctx = swapBatchCtxNew();

    if (server.swap_persist_enabled)
        server.swap_persist_ctx = swapPersistCtxNew();
    else
        server.swap_persist_ctx = NULL;

    server.swap_ttl_compact_ctx = swapTtlCompactCtxNew();
}
