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

int debugGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int *keys = NULL;
    UNUSED(cmd);
    if (argc == 3 && !strcasecmp(argv[1]->ptr,"object")) {
        keys = getKeysPrepareResult(result,1);
        result->numkeys = 1;
        keys[0] = 2;
    } else if (argc >= 3 && !strcasecmp(argv[1]->ptr,"digest-value")) {
        keys = getKeysPrepareResult(result,argc-2);
        result->numkeys = argc-2;
        for (int i = 2; i < argc; i++) keys[i-2] = i;
    } else {
        keys = getKeysPrepareResult(result,1);
        result->numkeys = 0;
    }
    return result->numkeys;
}

static sds debugRioGet(int cf, sds rawkey) {
    sds rawval;
    RIO _rio, *rio = &_rio;
    int *cfs = zmalloc(1*sizeof(int));
    sds *rawkeys = zmalloc(1*sizeof(sds));
    cfs[0] = cf;
    rawkeys[0] = sdsdup(rawkey);
    RIOInitGet(rio,1,cfs,rawkeys);
    RIODo(rio);
    rawval = rio->get.rawvals[0] ? sdsdup(rio->get.rawvals[0]) : NULL;
    RIODeinit(rio);
    return rawval;
}

static sds getSwapObjectInfo(robj *o) {
    if (o) {
        return sdscatprintf(sdsempty(),
                "at=%p,refcount=%d,type=%s,encoding=%s,dirty_meta=%d,dirty_data=%d,"
                "persistent=%d,persist_keep=%d,lru=%d,lru_seconds_idle=%llu",
                (void*)o,o->refcount,strObjectType(o->type),
                strEncoding(o->encoding),o->dirty_meta,o->dirty_data,
                o->persistent,o->persist_keep,o->lru,estimateObjectIdleTime(o)/1000);
    } else {
        return sdsnew("<nil>");
    }
}

static sds getSwapMetaInfo(int swap_type, long long expire,objectMeta *m) {
    if (swap_type == -1) return sdsnew("<nil>");
    sds info = sdscatprintf(sdsempty(),"swap_type=%d,expire=%lld",
            swap_type,expire);
    if (m) {
        sds omdump = dumpObjectMeta(m);
        info = sdscatprintf(info, ",at=%p,%s",(void*)m,omdump);
        sdsfree(omdump);
    } else {
        info = sdscatprintf(info, ",at=<nil>");
    }
    return info;
}

static int getCfOrReply(client *c, robj *cf) {
    if (!strcasecmp(cf->ptr,"meta")) {
        return META_CF;
    } else if (!strcasecmp(cf->ptr, "data")) {
        return DATA_CF;
    } else if (!strcasecmp(cf->ptr, "score")) {
        return SCORE_CF;
    } else {
        addReplyError(c,"invalid cf");
        return -1;
    }
}

static sds calculateNextPrefix(sds current) {
    sds next = NULL;
    size_t nextlen = sdslen(current);

    do {
        if (current[nextlen - 1] != (char)0xff) break;
        nextlen--;
    } while(nextlen > 0);

    if (0 == nextlen) return NULL;

    next = sdsnewlen(current, nextlen);
    next[nextlen - 1]++;

    return next;
}

static int swapBlobListResolveCfs(const char *cfnames, int cfs[CF_COUNT]) {
    int i = 0;
    char *ptr, *saveptr, *dupnames = NULL;

    if (cfnames == NULL || strlen(cfnames) == 0) {
        for (; i < CF_COUNT; i++) cfs[i] = i;
        goto end;
    }

    dupnames = sdsnew(cfnames);
    for (ptr = strtok_r(dupnames,", ",&saveptr);
            ptr != NULL && i < CF_COUNT;
            ptr = strtok_r(NULL,", ",&saveptr)) {
        if (!strcasecmp(ptr,data_cf_name)) {
            cfs[i] = DATA_CF;
        } else if (!strcasecmp(ptr,meta_cf_name)) {
            cfs[i] = META_CF;
        } else if (!strcasecmp(ptr,score_cf_name)) {
            cfs[i] = SCORE_CF;
        } else {
            i = -1;
            goto end;
        }

        i++;
    }

end:
    if (dupnames) sdsfree(dupnames);
    return i;
}

/* SWAP BLOB-LIST-PENDING [<cfname,cfname...>]
 *
 * Replies with the cf whose blob list is still being built up, that is the cf
 * having ssts that reference blob files but carry no blob file set record yet.
 * An empty reply means the build up is done. Cf with the record option off are
 * skipped, since there is nothing to build there. */
static void swapBlobListPendingCommand(client *c, const char *cfnames) {
    int cfs[CF_COUNT], hit[CF_COUNT], cf_num, hit_num = 0;
    rocks *rocks;

    if ((cf_num = swapBlobListResolveCfs(cfnames,cfs)) < 0) {
        addReplyError(c,"Invalid cf name");
        return;
    }

    rocks = serverRocksGetReadLock();
    for (int i = 0; i < cf_num; i++) {
        int cf = cfs[i];

        if (!swapBlobFileSetRecordEnabled(cf)) continue;

        rocksdb_column_family_metadata_t *cf_meta =
            rocksdb_get_column_family_metadata_cf(rocks->db,
                    rocks->cf_handles[cf]);
        if (cfMetaBlobListIncomplete(cf_meta)) hit[hit_num++] = cf;
        if (cf_meta) rocksdb_column_family_metadata_destroy(cf_meta);
    }
    serverRocksUnlock(rocks);

    addReplyArrayLen(c, hit_num);
    for (int i = 0; i < hit_num; i++) {
        addReplyBulkCString(c, swapGetCFName(hit[i]));
    }
}

/* SWAP BLOB-LIST-ORPHAN [<cfname,cfname...>]
 *
 * Replies with flat cf name and garbage bytes pairs, the bytes being what list
 * based blob gc can never reclaim in that cf. Any entry means a bug or a race
 * left blob files without the record gc needs, an empty reply means clean. Only
 * cf that already applied list based gc are examined */
static void swapBlobListOrphanCommand(client *c, const char *cfnames) {
    int cfs[CF_COUNT], hit[CF_COUNT], cf_num, hit_num = 0;
    uint64_t garbage[CF_COUNT];
    rocks *rocks;

    if ((cf_num = swapBlobListResolveCfs(cfnames,cfs)) < 0) {
        addReplyError(c,"Invalid cf name");
        return;
    }

    rocks = serverRocksGetReadLock();
    for (int i = 0; i < cf_num; i++) {
        int cf = cfs[i];

        if (!swapBlobListGcIntended(cf) || !swapBlobListGcApplied(cf)) continue;

        rocksdb_column_family_metadata_t *cf_meta =
            rocksdb_get_column_family_metadata_cf(rocks->db,
                    rocks->cf_handles[cf]);
        uint64_t bytes = cfMetaOrphanBlobGarbageBytes(cf_meta);
        if (bytes > 0) {
            hit[hit_num] = cf;
            garbage[hit_num++] = bytes;
        }
        if (cf_meta) rocksdb_column_family_metadata_destroy(cf_meta);
    }
    serverRocksUnlock(rocks);

    addReplyArrayLen(c, hit_num*2);
    for (int i = 0; i < hit_num; i++) {
        addReplyBulkCString(c, swapGetCFName(hit[i]));
        addReplyLongLong(c, (long long)garbage[i]);
    }
}

/* SWAP BLOB-LIST-GC-APPLIED [<cfname,cfname...>]
 *
 * Replies with flat cf name and state pairs, same shape as CONFIG GET, telling
 * whether list based blob gc is really in effect per cf. may differ from the configured value
 * once pushing the option down gets deferred until the blob list is built. */
static void swapBlobListGcAppliedCommand(client *c, const char *cfnames) {
    int cfs[CF_COUNT], cf_num;

    /* Reports server side state only, should be same as what rocksdb get unless there is a bug. */
    if ((cf_num = swapBlobListResolveCfs(cfnames,cfs)) < 0) {
        addReplyError(c,"Invalid cf name");
        return;
    }

    addReplyArrayLen(c, cf_num*2);
    for (int i = 0; i < cf_num; i++) {
        addReplyBulkCString(c, swapGetCFName(cfs[i]));
        addReplyBulkCString(c, swapBlobListGcIntended(cfs[i]) &&
                swapBlobListGcApplied(cfs[i]) ? "on" : "off");
    }
}

void swapCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"OBJECT <key>",
"    Show info about `key` and assosiated value.",
"ENCODE-META-KEY <key>",
"    Encode meta key.",
"DECODE-META-KEY <rawkey>",
"    Decode meta key.",
"ENCODE-DATA-KEY <key> <version> <subkey>",
"    Encode data key.",
"DECODE-DATA-KEY <rawkey>",
"    Decode data key.",
"RIO-GET meta|data <rawkey> <rawkey> ...",
"    Get raw value from rocksdb.",
"RIO-SCAN meta|data <prefix>",
"    Scan rocksdb with prefix.",
"RIO-ERROR <count> [ACTION name]",
"    Make next count rio return error.",
"RESET-STATS",
"    Reset swap stats.",
"COMPACT",
"   COMPACT rocksdb",
"FLUSH [<cfname,cfname...>]",
"   Flush rocksdb",
"BLOB-LIST-PENDING [<cfname,cfname...>]",
"    List the cfs whose blob list is not built yet, that is cfs with the blob",
"    file set record enabled that still have ssts referencing blob files",
"    without one. Empty means the build is done. Cfs with the record disabled",
"    are skipped.",
"BLOB-LIST-ORPHAN [<cfname,cfname...>]",
"    Report per cf the garbage bytes held by blob files that no recorded blob",
"    file set points at, listing only the cfs that have any. Empty means gc",
"    can see all the garbage. Cfs not running list based blob gc yet are",
"    skipped, use BLOB-LIST-PENDING for those.",
"BLOB-LIST-GC-APPLIED [<cfname,cfname...>]",
"    Report per cf whether list based blob gc is really in effect, which is",
"    what decides the cfs BLOB-LIST-ORPHAN looks at. May differ from the",
"    configured value once pushing the option gets deferred until the blob",
"    list is built.",
"ROCKSDB-PROPERTY-INT <rocksdb-prop-name> [<cfname,cfname...>]",
"    Get rocksdb property value (int type)",
"ROCKSDB-PROPERTY-VALUE <rocksdb-prop-name> [<cfname,cfname...>]",
"    Get rocksdb property value (string type)",
"SCAN-SESSION [<cursor>]",
"    List assigned scan sesions",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"object") && c->argc == 3) {
        redisDb *db = c->db;
        robj *key = c->argv[2];
        robj *value = lookupKey(db,key,LOOKUP_NOTOUCH);
        objectMeta *hot_meta = lookupMeta(db,key), *cold_meta = NULL;
        long long hot_expire = getExpire(db,key), cold_expire = -1;
        sds meta_rawkey = NULL, meta_rawval = NULL;
        int hot_swap_type = hot_meta ? hot_meta->swap_type : -1;
        uint64_t cold_version;
        int cold_swap_type = -1;

        meta_rawkey = rocksEncodeMetaKey(db,key->ptr);
        meta_rawval = debugRioGet(META_CF,meta_rawkey);
        if (meta_rawval) {
            const char *extend;
            size_t extlen;
            rocksDecodeMetaVal(meta_rawval,sdslen(meta_rawval),
                    &cold_swap_type,&cold_expire,&cold_version,&extend,&extlen);
            if (extend) {
                buildObjectMeta(cold_swap_type,cold_version,extend,extlen,&cold_meta);
            }
        }

        if (!value && !hot_meta && !meta_rawval) {
            addReplyErrorObject(c,shared.nokeyerr);
            if (meta_rawkey) sdsfree(meta_rawkey);
            if (meta_rawval) sdsfree(meta_rawval);
            return;
        }

        sds value_info = getSwapObjectInfo(value);
        sds hot_meta_info = getSwapMetaInfo(hot_swap_type,hot_expire,hot_meta);
        sds cold_meta_info = getSwapMetaInfo(cold_swap_type,cold_expire,cold_meta);
        sds info = sdscatprintf(sdsempty(),
                "value: %s\nhot_meta: %s\ncold_meta: %s\n",
                value_info,hot_meta_info,cold_meta_info);
        addReplyVerbatim(c,info,sdslen(info),"txt");
        sdsfree(value_info);
        sdsfree(hot_meta_info);
        sdsfree(cold_meta_info);
        sdsfree(info);
        if (meta_rawkey) sdsfree(meta_rawkey);
        if (meta_rawval) sdsfree(meta_rawval);
        if (cold_meta) freeObjectMeta(cold_meta);
    } else if (!strcasecmp(c->argv[1]->ptr,"encode-meta-key") && c->argc == 3) {
        addReplyBulkSds(c,rocksEncodeMetaKey(c->db,c->argv[2]->ptr));
    } else if (!strcasecmp(c->argv[1]->ptr,"decode-meta-key") && c->argc == 3) {
        int dbid, retval;
        sds rawkey = c->argv[2]->ptr;
        const char *key;
        size_t keylen;

        retval = rocksDecodeMetaKey(rawkey,sdslen(rawkey),&dbid,&key,&keylen);
        if (retval) {
            addReplyError(c,"invalid meta key");
        } else {
            addReplyArrayLen(c,2);
            addReplyBulkLongLong(c,dbid);
            addReplyBulkCBuffer(c,key,keylen);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"encode-data-key") && c->argc == 5) {
        long long version;
        if (getLongLongFromObject(c->argv[3],&version)) {
            addReplyError(c,"invalid version");
        } else {
            addReplyBulkSds(c,rocksEncodeDataKey(c->db,c->argv[2]->ptr,version,
                        c->argv[4]->ptr));
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"decode-data-key") && c->argc == 3) {
        int dbid, retval;
        sds rawkey = c->argv[2]->ptr;
        const char *key, *subkey;
        size_t keylen, sublen;
        uint64_t version;

        retval = rocksDecodeDataKey(rawkey,sdslen(rawkey),
                &dbid,&key,&keylen,&version,&subkey,&sublen);
        if (retval) {
            addReplyError(c,"invalid data key");
        } else {
            addReplyArrayLen(c,4);
            addReplyBulkLongLong(c,dbid);
            addReplyBulkCBuffer(c,key,keylen);
            addReplyBulkLongLong(c,version);
            addReplyBulkCBuffer(c,subkey,sublen);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"rio-get") && c->argc >= 4) {
        int cf;
        addReplyArrayLen(c, c->argc-3);
        if ((cf = getCfOrReply(c,c->argv[2])) < 0) return;
        for (int i = 3; i < c->argc; i++) {
            sds rawval = debugRioGet(cf,c->argv[i]->ptr);
            if (rawval == NULL) {
                addReplyNull(c);
            } else {
                addReplyBulkSds(c,sdsdup(rawval));
            }
            sdsfree(rawval);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"rio-scan") && c->argc == 4) {
        int cf;
        RIO _rio, *rio = &_rio;
        if ((cf = getCfOrReply(c,c->argv[2])) < 0) return;
        sds prefix = sdsdup(c->argv[3]->ptr);
        sds end = NULL;
        if (0 != sdslen(prefix)) end = calculateNextPrefix(prefix);
        RIOInitIterate(rio, cf, 0, prefix, end, ROCKS_ITERATE_NO_LIMIT);
        RIODo(rio);
        addReplyArrayLen(c,rio->iterate.numkeys);
        for (int i = 0; i < rio->iterate.numkeys; i++) {
            sds repr = sdsempty();
            repr = sdscatsds(repr, rio->iterate.rawkeys[i]);
            repr = sdscat(repr, "=>");
            repr = sdscatsds(repr, rio->iterate.rawvals[i]);
            addReplyBulkSds(c,repr);
        }
        RIODeinit(rio);
    } else if (!strcasecmp(c->argv[1]->ptr,"rio-error") && c->argc >= 3) {
        long long count;
        int action = 0;
        if (getLongLongFromObjectOrReply(c,c->argv[2],&count,NULL))
            return;
        if (c->argc > 3) {
            if (c->argc == 5 && !strcasecmp(c->argv[3]->ptr,"action")) {
                action = rocksActionFromName(c->argv[4]->ptr);
                if (action == -1) {
                    addReplyError(c,"rio-error invalid action");
                    return;
                }
            } else {
                addReplyError(c,"rio-error invalid arg");
                return;
            }
        }
        if (count > INT_MAX || count < 0) {
            addReplyError(c,"rio-error count invalid");
        } else {
            server.swap_debug_rio_error = (int)count;
            server.swap_debug_rio_error_action = action;
            addReply(c,shared.ok);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"reset-stats") && c->argc == 2) {
        resetStatsSwap();
        resetSwapHitStat();
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"compact") && c->argc == 2) {
        sds error = NULL;
        /* no specified compact range, to launch a full compact. */
        compactTask *task = compactTaskNew(TYPE_FULL_COMPACT);

        for (int i = 0; i < CF_COUNT; i++) {
            compactKeyRange *key_range = compactKeyRangeNew(i, NULL, NULL, 0, 0);
            compactTaskAppend(task, key_range);
        }

        if (submitUtilTask(ROCKSDB_COMPACT_RANGE_TASK, task, rocksdbCompactRangeTaskDone, task, &error)) {
            addReply(c,shared.ok);
        } else {
            compactTaskFree(task);
            addReplyErrorSds(c,error);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"flush") && c->argc >= 2) {
        sds error = NULL;
        const char *cfnames = c->argc > 2 ? c->argv[2]->ptr : NULL;
        rocks *rocks = serverRocksGetReadLock();
        swapData4RocksdbFlush *data = rocksdbFlushTaskArgCreate(rocks,cfnames);
        serverRocksUnlock(rocks);
        if (submitUtilTask(ROCKSDB_FLUSH_TASK, data, rocksdbFlushTaskDone, data, &error)) {
            addReply(c,shared.ok);
        } else {
            addReplyErrorSds(c,error);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"blob-list-pending") &&
               (c->argc == 2 || c->argc == 3)) {
        swapBlobListPendingCommand(c, c->argc > 2 ? c->argv[2]->ptr : NULL);
    } else if (!strcasecmp(c->argv[1]->ptr,"blob-list-orphan") &&
               (c->argc == 2 || c->argc == 3)) {
        swapBlobListOrphanCommand(c, c->argc > 2 ? c->argv[2]->ptr : NULL);
    } else if (!strcasecmp(c->argv[1]->ptr,"blob-list-gc-applied") &&
               (c->argc == 2 || c->argc == 3)) {
        swapBlobListGcAppliedCommand(c, c->argc > 2 ? c->argv[2]->ptr : NULL);
    } else if (!strcasecmp(c->argv[1]->ptr,"rocksdb-property-int") && c->argc >= 3) {
        uint64_t property_int = 0;
        const char *cfnames = c->argc > 3 ? c->argv[3]->ptr : NULL;
        rocks *rocks = serverRocksGetReadLock();
        rocksPropertyInt(rocks,cfnames,c->argv[2]->ptr,&property_int);
        serverRocksUnlock(rocks);
        addReplyLongLong(c, property_int);
    } else if (!strcasecmp(c->argv[1]->ptr,"rocksdb-property-value") && c->argc >= 3) {
        const char *cfnames = c->argc > 3 ? c->argv[3]->ptr : NULL;
        rocks *rocks = serverRocksGetReadLock();
        sds property_value = rocksPropertyValue(rocks,cfnames,c->argv[2]->ptr);
        serverRocksUnlock(rocks);
        addReplyBulkCString(c, property_value);
        if (property_value) sdsfree(property_value);
    } else if (!strcasecmp(c->argv[1]->ptr,"scan-session") &&
            (c->argc == 2 || c->argc == 3)) {
        long long outer_cursor;
        if (c->argc == 2) {
            outer_cursor = -1;
        } else {
            if (getLongLongFromObjectOrReply(c,c->argv[2],&outer_cursor,
                        "Invalid cursor")) {
                return;
            }
            if (outer_cursor < 0) {
                addReplyError(c,"Invalid cursor");
                return;
            }
        }
        sds o = getAllSwapScanSessionsInfoString(outer_cursor);
        addReplyVerbatim(c,o,sdslen(o),"txt");
        sdsfree(o);
    } else {
        addReplySubcommandSyntaxError(c);
        return;
    }
}

/* swap.debug will requires global lock. */
void swapDebugCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"RORDB BGSAVE|RELOAD",
"    Background save or reload with rordb format.",
"THREAD AUTO-SCALE-UP|AUTO-SCALE-UP CHECK|AUTO-SCALE-DOWN|AUTO-SCALE-DOWN-CHECK",
"    swap thread auto scale",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"rordb") && c->argc == 3) {
        if (!strcasecmp(c->argv[2]->ptr,"bgsave")) {
            rdbSaveInfo rsi, *rsiptr;

            rsiptr = rdbPopulateSaveInfo(&rsi);
            rdbSaveInfoSetRordb(rsiptr,1);
            if (server.child_type == CHILD_TYPE_RDB || hasActiveChildProcess()) {
                addReplyError(c,"Background child already in progress");
                return;
            }
            rdbSaveInfoSetSfrctx(rsiptr,
                    swapForkRocksdbCtxCreate(SWAP_FORK_ROCKSDB_TYPE_CHECKPOINT));
            if (rdbSaveBackground(server.rdb_filename,rsiptr) == C_OK) {
                addReplyStatus(c,"Background saving started(rordb mode)");
            } else {
                addReplyErrorObject(c,shared.err);
            }
        } else if (!strcasecmp(c->argv[2]->ptr,"reload")) {
            rdbSaveInfo rsi, *rsiptr;
            rsiptr = rdbPopulateSaveInfo(&rsi);
            rdbSaveInfoSetRordb(rsiptr,1);
            sds checkpoint_dir = sdscatprintf(sdsempty(),"%s/tmp_%lld",ROCKS_DATA,ustime());
            rocks *rocks = serverRocksGetReadLock();
            int ret2 = rocksCreateCheckpoint(rocks, checkpoint_dir);
            if (ret2 != C_OK) {
                addReplyError(c,"Error creating checkpoint");
                return;
            }
            if (rdbSave(server.rdb_filename,rsiptr) != C_OK) {
                addReplyError(c,"Error saving rordb");
                return;
            }
            rocksReleaseCheckpoint(rocks);
            serverRocksUnlock(rocks);
            emptyDb(-1,EMPTYDB_NO_FLAGS,NULL);
            protectClient(c);
            int ret = rdbLoad(server.rdb_filename,NULL,RDBFLAGS_NONE);
            unprotectClient(c);
            if (ret != C_OK) {
                addReplyError(c,"Error trying to load RORDB");
                return;
            }
            serverLog(LL_WARNING,"DB reloaded by SWAP.DEBUG RORDB RELOAD");
            addReply(c,shared.ok);
        } else {
            addReplySubcommandSyntaxError(c);
        }
    } else if(!strcasecmp(c->argv[1]->ptr,"thread") && c->argc >= 3) {
        if (!strcasecmp(c->argv[2]->ptr,"list")) {
            long long now_time = ustime();
            sds info = sdsempty();
            for (int i = EXTRA_SWAP_THREADS_NUM; i < server.swap_total_threads_num; i++) {
                swapThread *thread = server.swap_threads+i;
                size_t inflight_reqs;
                atomicGet(thread->inflight_reqs, inflight_reqs);
                info = sdscatprintf(info, "swap_thread%d:inflight_reqs=%ld,idle_time=%lld\r\n", i, inflight_reqs, thread->start_idle_time != -1? now_time - thread->start_idle_time: -1);
            }
            return addReplyBulkSds(c, info);
        } else if (!strcasecmp(c->argv[2]->ptr,"auto-scale-up")) {
            if (c->argc == 3) {
                return addReplyLongLong(c, (long long)swapThreadsAutoScaleUp());
            } else if (c->argc == 4 && !strcasecmp(c->argv[3]->ptr, "check")) {
                size_t swap_threads_inflight_reqs[server.swap_total_threads_num];
                swapThreadsGetInflightReqs(swap_threads_inflight_reqs);
                return addReplyLongLong(c, (long long)swapThreadsAutoScaleUpIfNeeded(swap_threads_inflight_reqs));
            }  
        } else if (!strcasecmp(c->argv[2]->ptr,"auto-scale-down")) { 
            if (c->argc == 3) {
                return addReplyLongLong(c, (long long)swapThreadsAutoScaleDown());  
            } else if (c->argc == 4 && !strcasecmp(c->argv[3]->ptr, "check")) {
                return addReplyLongLong(c, (long long)swapThreadsAutoScaleDownIfNeeded());  
            }
        } 
        addReplySubcommandSyntaxError(c);
        return;    
    } else {
        addReplySubcommandSyntaxError(c);
        return;
    }
}

#ifdef SWAP_DEBUG

void swapDebugMsgsInit(swapDebugMsgs *msgs, char *identity) {
    snprintf(msgs->identity,MAX_MSG,"[%s]",identity);
}

void swapDebugMsgsAppendV(swapDebugMsgs *msgs, char *step, char *fmt, va_list ap) {
    char *name = msgs->steps[msgs->index].name;
    char *info = msgs->steps[msgs->index].info;
    strncpy(name,step,MAX_MSG-1);
    vsnprintf(info,MAX_MSG,fmt,ap);
    msgs->index++;
}

void swapDebugMsgsAppend(swapDebugMsgs *msgs, char *step, char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    swapDebugMsgsAppendV(msgs, step, fmt, ap);
    va_end(ap);
}

void swapDebugBatchMsgsAppend(swapExecBatch *batch, char *step, char *fmt, ...) {
    for (size_t i = 0; i < batch->count; i++) {
        va_list ap;
        va_start(ap,fmt);
        swapDebugMsgsAppend(batch->reqs[i]->msgs, step, fmt, ap);
        va_end(ap);
    }
}

void swapDebugMsgsDump(swapDebugMsgs *msgs) {
    serverLog(LL_NOTICE,"=== %s ===", msgs->identity);
    for (int i = 0; i < msgs->index; i++) {
        char *name = msgs->steps[i].name;
        char *info = msgs->steps[i].info;
        serverLog(LL_NOTICE,"%2d %25s : %s",i,name,info);
    }
}

#endif

