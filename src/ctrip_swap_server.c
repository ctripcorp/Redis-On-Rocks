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
#define BUFFERED_ALLOCATOR_CAPACITY_SWAPDATA 4096
#define BUFFERED_ALLOCATOR_CAPACITY_SWAPCTX 4096

struct swapSharedObjectsStruct swap_shared;

void createSwapSharedObjects(void) {
    swap_shared.emptystring = createObject(OBJ_STRING, sdsnew(""));
    swap_shared.outofdiskerr = createObject(OBJ_STRING, sdsnew(
                                                            "-ERR command not allowed when used disk > 'swap-max-db-size'.\r\n"));
    swap_shared.rocksdbdiskerr = createObject(OBJ_STRING, sdsnew(
                                                              "-ERR command not allowed when rocksdb disk error.\r\n"));
    swap_shared.swap_info = createStringObject("swap.info", 9);
    swap_shared.sst_age_limit = createStringObject("SST-AGE-LIMIT", 13);
}

swapBatchLimitsConfig swapBatchLimitsDefaults[SWAP_TYPES] = {
    {0, 0},                                     /* NOP */
    {SWAP_BATCH_DEFAULT_SIZE, 1024 * 1024 * 1}, /* IN */
    {SWAP_BATCH_DEFAULT_SIZE, 1024 * 1024 * 1}, /* OUT */
    {SWAP_BATCH_DEFAULT_SIZE, 1024 * 1024 * 1}, /* DEL */
    {0, 0},                                     /* UTILS */
};

int updateRocksdbCFOption(int cf, char *key, char *val, const char **err) {
    rocks *rocks = serverRocksGetTryReadLock();
    if (rocks == NULL) {
        *err = "Fail to set option, check rocksdb state.";
        return 0;
    }

    char *inner_err = NULL;
    const char *const option_keys[] = {key};
    const char *const option_vals[] = {val};
    rocksdb_set_options_cf(rocks->db, rocks->cf_handles[cf],
                           1, option_keys, option_vals, &inner_err);
    if (inner_err != NULL) {
        serverLog(LL_WARNING, "[ROCKS] rocksdb set options %s:%s failed: %s", key, val, inner_err);
        zlibc_free(inner_err);
        *err = "Fail to set option, rocksdb fail.";
        serverRocksUnlock(rocks);
        return 0;
    }

    serverRocksUnlock(rocks);
    return 1;
}

int updateRocksdbCFOptionBoolean(int cf, char *key, int val, const char **err) {
    char *val_str = val ? "true" : "false";
    return updateRocksdbCFOption(cf, key, val_str, err);
}

int swapPushBlobListGcOption(int cf, int val, const char **err) {
    if (!updateRocksdbCFOptionBoolean(cf,
                                      "enable_blob_list_garbage_collection", val, err))
        return 0;

    swapBlobListSetPending(cf, 0);
    return 1;
}

void swapInitServerConfig(void) {
    /* Scalar swap and rocksdb configs are initialized by initConfigValues()
     * from ctrip_swap_config.inc (single source of truth). Only non-table
     * array presets are initialized here. */
    for (int j = 0; j < SWAP_TYPES; j++)
        server.swap_batch_limits[j] = swapBatchLimitsDefaults[j];
}

void ctrip_ignoreAcceptEvent(void) {
    if (server.ctrip_ignore_accept) return;
    serverLog(LL_NOTICE, "[ctrip] ignore accept for clients overflow.");
    server.ctrip_ignore_accept = 1;

    for (int i = 0; i < CONN_TYPE_MAX; i++) {
        connListener *listener = &server.listeners[i];
        if (listener->ct == NULL) continue;
        for (int j = 0; j < listener->count; j++) {
            if (listener->fd[j] == -1) continue;
            aeDeleteFileEvent(server.el, listener->fd[j], AE_READABLE);
        }
    }
}

void ctrip_resetAcceptIgnore(void) {
    if (!server.ctrip_ignore_accept) return;
    server.ctrip_ignore_accept = 0;
    serverLog(LL_NOTICE, "[ctrip] reset accept ignore, current clients %lu/%u",
              listLength(server.clients) + getClusterConnectionsCount(), server.maxclients);

    for (int i = 0; i < CONN_TYPE_MAX; i++) {
        connListener *listener = &server.listeners[i];
        if (listener->ct == NULL) continue;
        if (createSocketAcceptHandler(listener, connAcceptHandler(listener->ct)) != C_OK) {
            serverPanic("Unrecoverable error creating %s listener accept handler.",
                        getConnectionTypeName(listener->ct->get_type()));
        }
    }
}

void acceptMonitorHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = server.max_new_conns_per_cycle;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while (max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                          "Accepting ctrip_monitor client connection: %s", server.neterr);
            return;
        }
        anetCloexec(cfd);
        serverLog(LL_VERBOSE, "Accepted ctrip_monitor %s:%d", cip, cport);
        ClientFlags flags = {0};
#ifdef ENABLE_SWAP
        flags.ctrip_monitor = 1;
#endif
        acceptCommonHandler(connCreateAccepted(connectionTypeTcp(), cfd, NULL), flags, cip);
    }
}

/* keep work even if clients overflow. */
void ctrip_initMonitorAcceptor(void) {
    serverAssert(server.ctrip_monitor_port > 0);
    server.ctrip_monitorfd = anetTcpServer(server.neterr, server.ctrip_monitor_port, "127.0.0.1", server.tcp_backlog, 0);

    if (server.ctrip_monitorfd == ANET_ERR) {
        serverLog(LL_WARNING, "Failed listening on port %d (ctrip monitor): %s, aborting.",
                  server.ctrip_monitor_port, server.neterr);
        exit(1);
    }

    anetNonBlock(NULL, server.ctrip_monitorfd);
    anetCloexec(server.ctrip_monitorfd);

    if (aeCreateFileEvent(server.el, server.ctrip_monitorfd, AE_READABLE, acceptMonitorHandler, NULL) == AE_ERR) {
        serverPanic("Unrecoverable error creating ctrip monitor file event.");
        exit(1);
    }
}

static client **createSwapDbClients(const char *cmdname) {
    struct serverCommand *cmd = lookupCommandByCString(cmdname);
    client **clients = zmalloc(server.dbnum * sizeof(client *));
    for (int i = 0; i < server.dbnum; i++) {
        client *c = createClient(NULL);
        c->cmd = cmd;
        c->db = server.db[i];
        c->swap_lock_mode = SWAP_LOCK_SHARED;
        clients[i] = c;
    }
    return clients;
}

/* Initialize only part of swap module, used by both tests and the main program. */
void swapInit(void) {
    int i;
    swapInitVersion();

    server.swap_eviction_ctx = swapEvictionCtxCreate();

    server.swap_load_inprogress_count = 0;

    buffered_allocator_swapdata = bufferedAllocatorCreate(
        BUFFERED_ALLOCATOR_CAPACITY_SWAPDATA, sizeof(struct swapData), NULL, NULL);
    buffered_allocator_swapctx = bufferedAllocatorCreate(
        BUFFERED_ALLOCATOR_CAPACITY_SWAPCTX, sizeof(struct swapCtx), NULL, NULL);

    server.swap_evict_clients = createSwapDbClients("swap.evict");
    server.swap_load_clients = createSwapDbClients("swap.load");
    server.swap_expire_clients = createSwapDbClients("swap.expired");
    server.swap_scan_expire_clients = createSwapDbClients("swap.scanexpire");
    server.swap_ttl_clients = createSwapDbClients("ttl");

    server.swap_mutex_client = createClient(NULL);
    server.swap_mutex_client->cmd = lookupCommandByCString("swap.mutexop");
    server.swap_mutex_client->swap_lock_mode = SWAP_LOCK_SHARED;

    server.swap_repl_workers = 256;
    server.swap_repl_swapping_clients = listCreate();
    server.swap_repl_worker_clients_free = listCreate();
    server.swap_repl_worker_clients_used = listCreate();
    for (i = 0; i < server.swap_repl_workers; i++) {
        client *c = createClient(NULL);
        c->swap_lock_mode = SWAP_LOCK_UNIQUE;
        listAddNodeTail(server.swap_repl_worker_clients_free, c);
    }

    server.swap_rdb_load_object_ctx = NULL;

    swapLockCreate();

    server.swap_scan_sessions = swapScanSessionsCreate(server.swap_scan_session_bits);

    server.swap_dependency_block_ctx = createSwapUnblockCtx();

    server.swap_batch_ctx = swapBatchCtxNew();

    if (server.swap_persist_enabled)
        server.swap_persist_ctx = swapPersistCtxNew();
    else
        server.swap_persist_ctx = NULL;

    server.swap_ttl_compact_ctx = swapTtlCompactCtxNew();
    server.swap_full_compact_ctx = swapFullCompactCtxNew();

    createSwapSharedObjects();
}

void swapInitDb(redisDb *db) {
    if (db == NULL) return;
    db->meta = dictCreate(&objectMetaDictType);
    db->dirty_subkeys = dictCreate(&dbDirtySubkeysDictType);
    db->evict_asap = listCreate();
    db->cold_keys = 0;
    db->scan_expire = scanExpireCreate();
    db->randomkey_nextseek = NULL;
    db->cold_filter = coldFilterCreate();
}

void swapReleaseDb(redisDb *db) {
    if (db == NULL) return;
    if (db->meta) dictRelease(db->meta);
    if (db->dirty_subkeys) dictRelease(db->dirty_subkeys);
    if (db->evict_asap) listRelease(db->evict_asap);
    if (db->scan_expire) scanExpireFree(db->scan_expire);
    if (db->cold_filter) coldFilterDestroy(db->cold_filter);
    if (db->randomkey_nextseek) sdsfree(db->randomkey_nextseek);
    db->meta = NULL;
    db->dirty_subkeys = NULL;
    db->evict_asap = NULL;
    db->scan_expire = NULL;
    db->cold_filter = NULL;
    db->randomkey_nextseek = NULL;
}

void swapInitServer(void) {
    int j;

    server.clients_to_free = listCreate();
    server.ctrip_ignore_accept = 0;
    server.ctrip_monitorfd = 0;
    server.swap_lastsave = 0;
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
    server.rocksdb_default_live_blob_file_size = 0;
    server.rocksdb_default_live_blob_file_garbage_size = 0;
    server.rocksdb_default_total_blob_file_size = 0;
    server.rocksdb_disk_error = 0;
    server.rocksdb_disk_error_since = 0;
    server.rocksdb_checkpoint = NULL;
    server.rocksdb_checkpoint_dir = NULL;
    server.rocksdb_rdb_checkpoint_dir = NULL;
    server.swap_child_err_pipe[0] = -1;
    server.swap_child_err_pipe[1] = -1;
    server.rocksdb_internal_stats = NULL;
    server.swap_util_task_manager = createRocksdbUtilTaskManager();
    server.swap_reject_blocked_cmd_count = 0;
    server.swap_allowed_blocked_cmd_count = 0;
    server.swap_master_allowed_cmd_count = 0;

    asyncCompleteQueueInit();
    parallelSyncInit(server.swap_ps_parallism_rdb);

    if (server.ctrip_monitor_port > 0) {
        ctrip_initMonitorAcceptor();
    }

    for (j = 0; j < server.dbnum; j++) {
        createDatabaseIfNeeded(j);
    }

    swapInit();
}
