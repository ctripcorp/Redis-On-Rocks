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

static redisAtomic filterState filter_state = FILTER_STATE_CLOSE;
int setFilterState(filterState state) {
    atomicSet(filter_state, state);
    return C_OK;
}

filterState getFilterState() {
    filterState state;
    atomicGet(filter_state, state);
    return state;
}

/* server.rocks can be used without lock because:
 *   background canceled when reopen
 *   filter state closed during reopen */
static sds rocksdbGet(rocksdb_readoptions_t* ropts, int cf, sds rawkey, char** err) {
    serverAssert(cf < CF_COUNT);
    size_t vallen;
    *err = NULL;
    char* val = rocksdb_get_cf(server.rocks->db, ropts,
            server.rocks->cf_handles[cf],
            rawkey, sdslen(rawkey), &vallen, err);
    if (*err != NULL || val == NULL)  return NULL;
    sds result = sdsnewlen(val, vallen);
    zlibc_free(val);
    return result;
}

typedef struct metaVersionFilter {
    uint64_t cached_keyversion;
    sds cached_metakey;
    uint64_t cached_metaversion;
} metaVersionFilter;

static inline metaVersionFilter *metaVersionFilterCreate() {
    metaVersionFilter *mvfilter = zcalloc(sizeof(metaVersionFilter));
    return mvfilter;
}

static inline void metaVersionFilterUpdateCache(metaVersionFilter *mvfilter,
        uint64_t keyversion, MOVE sds metakey, uint64_t metaversion) {
    mvfilter->cached_keyversion = keyversion;
    if (mvfilter->cached_metakey) sdsfree(mvfilter->cached_metakey);
    mvfilter->cached_metakey = metakey;
    mvfilter->cached_metaversion = metaversion;
}

static inline int metaVersionFilterMatchCache(metaVersionFilter *mvfilter,
        uint64_t keyversion, sds metakey) {
    return mvfilter->cached_keyversion == keyversion &&
        sdscmp(mvfilter->cached_metakey, metakey) == 0;
}

static inline void metaVersionFilterDestroy(void* mvfilter_) {
    metaVersionFilter *mvfilter = mvfilter_;
    if (mvfilter == NULL) return;
    if (mvfilter->cached_metakey) {
        sdsfree(mvfilter->cached_metakey);
        mvfilter->cached_metakey = NULL;
    }
    zfree(mvfilter);
}

static unsigned char metaVersionFilterFilt(void* mvfilter_, int level, int cf, const char* rawkey,
                                   size_t rawkey_length,
                                   int (*decodekey)(const char*, size_t , int* , const char**, size_t* ,uint64_t*)) {
    int dbid, result = 0;
    uint64_t key_version;
    const char* key;
    size_t key_len;
    filterState state;
    size_t inflight_snapshot;
    uint64_t meta_version;
    char* err = NULL;
    sds meta_val = NULL;
    metaVersionFilter *mvfilter = mvfilter_;

    if (server.unixtime < (time_t)server.swap_compaction_filter_disable_until)
        return 0;

    atomicGet(filter_state, state);
    if (state == FILTER_STATE_CLOSE) return 0;
    /* Since release 6.0, with compaction filter enabled, RocksDB always invoke filtering for any key,
     * even if it knows it will make a snapshot not repeatable. */
    atomicGet(server.inflight_snapshot, inflight_snapshot);
    if (inflight_snapshot > 0) return 0;

    updateCompactionFiltScanCount(cf);

    /* Skip compaction filter to speed up compaction process. */
    if (level <= server.swap_compaction_filter_skip_level) return 0;

    int retval = decodekey(rawkey, rawkey_length, &dbid, &key, &key_len, &key_version);
    if (retval != 0) return 0;

    /* Type is string*/
    if (key_version == SWAP_VERSION_ZERO) return 0;

    if (server.swap_debug_compaction_filter_delay_micro > 0)
        usleep(server.swap_debug_compaction_filter_delay_micro);

    sds meta_key = encodeMetaKey(dbid, key, key_len);

    if (metaVersionFilterMatchCache(mvfilter,key_version,meta_key)) {
        meta_version = mvfilter->cached_metaversion;
    } else {
        updateCompactionFiltRioCount(cf);
        meta_val = rocksdbGet(server.rocks->filter_meta_ropts, META_CF, meta_key, &err);
        if (err != NULL) {
            serverLog(LL_NOTICE, "[metaVersionFilter] rockget (%s) meta val fail: %s ", meta_key, err);
            /* if error happened, key will not be filtered. */
            meta_version = key_version;
            goto end;
        }

        if (meta_val != NULL) {
            int object_type;
            long long expire;
            const char* extend;
            size_t extend_len;

            retval = rocksDecodeMetaVal(meta_val,sdslen(meta_val),&object_type,&expire,
                    &meta_version, &extend,&extend_len);
            if (retval) {
                serverLog(LL_NOTICE, "[metaVersionFilter] decode meta val fail: %s", meta_val);
                /* if error happened, key will not be filtered. */
                meta_version = key_version;
                goto end;
            }
        } else {
            /* if metakey not found, meta_version assigned to max so that key
             * gets filtered. */
            meta_version = SWAP_VERSION_MAX;
        }

        metaVersionFilterUpdateCache(mvfilter,key_version,meta_key,meta_version);
        meta_key = NULL; /*moved*/
    }

end:
    result = meta_version > key_version;
    if (result) updateCompactionFiltSuccessCount(cf);
    sdsfree(meta_key);
    if (meta_val != NULL) sdsfree(meta_val);
    if (err != NULL) zlibc_free(err);
    return result;
}

/* data cf compaction filter */
static const char* dataFilterName(void* arg) {
  (void)arg;
  return "data_cf_filter";
}

static int decodeDataVersion(const char* rawkey, size_t rawkey_len, int* dbid, const char** key, size_t* key_len, uint64_t* version) {
    const char* subkey;
    size_t subkey_len;
    return rocksDecodeDataKey(rawkey, rawkey_len, dbid, key, key_len, version, &subkey, &subkey_len);
}

static unsigned char dataFilterFilter(void* mvfilter, int level, const char* rawkey,
                                   size_t rawkey_length,
                                   const char* existing_value,
                                   size_t value_length, char** new_value,
                                   size_t* new_value_length,
                                   unsigned char* value_changed) {
    UNUSED(existing_value);
    UNUSED(value_length);
    UNUSED(new_value);
    UNUSED(new_value_length);
    UNUSED(value_changed);
    return metaVersionFilterFilt(mvfilter, level, DATA_CF,rawkey, rawkey_length, decodeDataVersion);
}

rocksdb_compactionfilter_t* createDataCfCompactionFilter(void *state, rocksdb_compactionfiltercontext_t *context) {
    metaVersionFilter *mvfilter = metaVersionFilterCreate();
    UNUSED(state), UNUSED(context);
    return rocksdb_compactionfilter_create(mvfilter, metaVersionFilterDestroy,
                                              dataFilterFilter, dataFilterName);
}

static const char* dataFilterFactoryName(void* arg) {
  (void)arg;
  return "data_cf_filter_factory";
}

void filterFactoryDestructor(void *state) {
    UNUSED(state);
}

rocksdb_compactionfilterfactory_t* createDataCfCompactionFilterFactory() {
    return rocksdb_compactionfilterfactory_create(NULL,filterFactoryDestructor,
            createDataCfCompactionFilter,dataFilterFactoryName);
}

/* score cf compaction filter */
static const char* scoreFilterName(void* arg) {
  (void)arg;
  return "score_cf_filter";
}

static int decodeScoreVersion(const char* rawkey, size_t rawkey_len, int* dbid, const char** key, size_t* key_len,uint64_t* version) {
    const char* subkey;
    size_t subkey_len;
    double score;
    return decodeScoreKey(rawkey, rawkey_len, dbid, key, key_len, version, &score, &subkey, &subkey_len);
}

static unsigned char scoreFilterFilter(void* mvfilter, int level, const char* rawkey,
                                   size_t rawkey_length,
                                   const char* existing_value,
                                   size_t value_length, char** new_value,
                                   size_t* new_value_length,
                                   unsigned char* value_changed) {
    UNUSED(existing_value);
    UNUSED(value_length);
    UNUSED(new_value);
    UNUSED(new_value_length);
    UNUSED(value_changed);
    return metaVersionFilterFilt(mvfilter, level, SCORE_CF,rawkey, rawkey_length, decodeScoreVersion);
}

rocksdb_compactionfilter_t* createScoreCfCompactionFilter(void *state, rocksdb_compactionfiltercontext_t *context) {
    metaVersionFilter *mvfilter = metaVersionFilterCreate();
    UNUSED(state), UNUSED(context);
    return  rocksdb_compactionfilter_create(mvfilter, metaVersionFilterDestroy,
                                              scoreFilterFilter, scoreFilterName);
}

static const char* scoreFilterFactoryName(void* arg) {
  (void)arg;
  return "score_cf_filter_factory";
}

rocksdb_compactionfilterfactory_t* createScoreCfCompactionFilterFactory() {
    return rocksdb_compactionfilterfactory_create(NULL,filterFactoryDestructor,
            createScoreCfCompactionFilter,scoreFilterFactoryName);
}

#ifdef REDIS_TEST
static void rocksdbPut(int cf, sds rawkey, sds rawval, char** err) {
    serverAssert(cf < CF_COUNT);
    *err = NULL;
    rocksdb_put_cf(server.rocks->db, server.rocks->wopts,
            server.rocks->cf_handles[cf],
            rawkey, sdslen(rawkey), rawval, sdslen(rawval), err);
}

static void rocksdbDelete(int cf, sds rawkey, char** err) {
    rocksdb_delete_cf(server.rocks->db, server.rocks->wopts,
            server.rocks->cf_handles[cf],
            rawkey, sdslen(rawkey), err);
    if(rocksdbGet(server.rocks->ropts, cf, rawkey, err) != NULL) {
        *err = "delete fail";
    }
}

void initServer(void);
void initServerConfig(void);
void InitServerLast();
int swapFilterTest(int argc, char **argv, int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    int error = 0;
    server.hz = 10;
    server.verbosity = LL_WARNING;
    robj *key1 = createStringObject("key1",4);
    robj *val1 = createStringObject("val1",4);
    initTestRedisDb();
    setFilterState(FILTER_STATE_OPEN);
    redisDb *db = server.db;
    if (server.swap_batch_ctx == NULL)
        server.swap_batch_ctx = swapBatchCtxNew();
    server.swap_compaction_filter_skip_level = -1;

    sds subkey = sdsnew("subkey");
    char* err = NULL;
    long long filt_count, scan_count;
    TEST("exec: data compaction filter func") {
        /* test1 no-meta filter */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            resetStatsSwap();
            /* mock hash */
            sds rawkey = rocksEncodeDataKey(db, key1->ptr, 1, subkey );
            rocksdbPut(DATA_CF,rawkey,val1->ptr, &err);
            test_assert(err == NULL);
            /* compact filter will del data when no-meta */
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            test_assert(val == NULL);
            sdsfree(rawkey);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].scan_count, scan_count);
            test_assert(filt_count == 1);
            test_assert(scan_count >= 1);
        }

        /* test2 metaversion > dataversion */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            resetStatsSwap();
            /* mock hash data */
            sds rawkey = rocksEncodeDataKey(db, key1->ptr, 1, subkey);
            rocksdbPut(DATA_CF,rawkey,val1->ptr, &err);
            test_assert(err == NULL);
            /* mock meta version */
            sds rawmetakey = rocksEncodeMetaKey(db, key1->ptr);
            sds extend = rocksEncodeObjectMetaLen(1);
            sds rawmetaval = rocksEncodeMetaVal(OBJ_HASH, -1, 2, extend);
            rocksdbPut(META_CF, rawmetakey, rawmetaval, &err);
            test_assert(err == NULL);

            //compact filter will del data when metaversion > dataversion
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            test_assert(val == NULL);

            //clean
            rocksdbDelete(META_CF, rawmetakey, &err);
            test_assert(err == NULL);

            sdsfree(rawkey);
            sdsfree(rawmetakey);
            sdsfree(rawmetaval);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].scan_count, scan_count);
            test_assert(filt_count == 1);
            test_assert(scan_count >= 1);
        }


        /* test3 metaversion <= dataversion */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            resetStatsSwap();
            /* mock string data */
            sds rawkey = rocksEncodeDataKey(db, key1->ptr, 1, subkey);
            rocksdbPut(DATA_CF,rawkey,val1->ptr, &err);
            test_assert(err == NULL);
            /* mock meta version &&  dataversion == metaversion */
            sds rawmetakey = rocksEncodeMetaKey(db, key1->ptr);
            sds extend = rocksEncodeObjectMetaLen(1);
            sds rawmetaval = rocksEncodeMetaVal(OBJ_HASH, -1, 1, extend);
            rocksdbPut(META_CF,rawmetakey,rawmetaval, &err);
            test_assert(err == NULL);

            //compact filter will del data when metaversion > dataversion
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);

            /* mock string data && dataversion > metaversion */
            rawkey = rocksEncodeDataKey(db, key1->ptr, 2, subkey);
            rocksdbPut(DATA_CF,rawkey,val1->ptr, &err);
            test_assert(err == NULL);
            /* compact filter will del data when metaversion > dataversion */
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            val = rocksdbGet(server.rocks->ropts, DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);

            /* clean && free */
            rocksdbDelete(META_CF, rawmetakey, &err);
            test_assert(err == NULL);
            rocksdbDelete(DATA_CF, rawkey, &err);
            test_assert(err == NULL);

            sdsfree(rawkey);
            sdsfree(rawmetakey);
            sdsfree(rawmetaval);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].scan_count, scan_count);
            test_assert(filt_count == 0);
            test_assert(scan_count >= 1);
        }

        /* unknow data (unfilte) */
        {
            /* clean stat */
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            resetStatsSwap();

            sds unknow = sdsnew("foo");
            rocksdbPut(DATA_CF,unknow,val1->ptr, &err);
            test_assert(err == NULL);

            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, DATA_CF, unknow, &err);
            test_assert(err == NULL);
            test_assert(!strncmp(val, val1->ptr, sdslen(val1->ptr)));
            sdsfree(val);

            /* clean */
            rocksdbDelete(DATA_CF, unknow, &err);
            test_assert(err == NULL);
            sdsfree(unknow);

            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].scan_count, scan_count);
            test_assert(filt_count == 0);
            test_assert(scan_count >= 1);
        }

        /* meta version unknow */
        {
            /* clean stat */
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            resetStatsSwap();

            sds rawkey = rocksEncodeDataKey(db, key1->ptr, 1, subkey);
            rocksdbPut(DATA_CF,rawkey,val1->ptr, &err);
            test_assert(err == NULL);
            sds rawmetakey = rocksEncodeMetaKey(db, key1->ptr);
            sds rawmetaval = sdsnew("foo");
            rocksdbPut(META_CF,rawmetakey,rawmetaval, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);
            /* clean */
            rocksdbDelete(META_CF, rawmetakey, &err);
            test_assert(err == NULL);
            rocksdbDelete(DATA_CF, rawkey, &err);
            test_assert(err == NULL);

            sdsfree(rawkey);
            sdsfree(rawmetaval);
            sdsfree(rawmetakey);

            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].scan_count, scan_count);
            test_assert(filt_count == 0);
            test_assert(scan_count >= 1);
        }

        /* version == 0 => (type is string)   (unfilter) */
        {
            /* clean stat */
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            resetStatsSwap();

            sds rawkey = rocksEncodeDataKey(db, key1->ptr, 0, NULL);
            rocksdbPut(DATA_CF,rawkey,val1->ptr, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);
            /* clean */
            rocksdbDelete(DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            sdsfree(rawkey);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].scan_count, scan_count);
            test_assert(filt_count == 0);
            test_assert(scan_count >= 1);
        }
   }

   TEST("exec: score compaction filter -data") {
        /* test1 no-meta filter */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            resetStatsSwap();
            /* mock score data */
            sds rawscorekey = encodeScoreKey(db, key1->ptr,  1, 10, subkey);
            rocksdbPut(SCORE_CF,rawscorekey,val1->ptr, &err);
            test_assert(err == NULL);
            /* compact filter will del data when no-meta */
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, SCORE_CF, rawscorekey, &err);
            test_assert(err == NULL);
            test_assert(val == NULL);
            sdsfree(rawscorekey);
            /* check stat update */
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].scan_count, scan_count);
            test_assert(filt_count == 1);
            test_assert(scan_count == 1);
        }

        /* test2 metaversion > dataversion */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            resetStatsSwap();

            sds rawscorekey = encodeScoreKey(db, key1->ptr,  1, 10, subkey);
            rocksdbPut(SCORE_CF,rawscorekey,val1->ptr, &err);
            test_assert(err == NULL);
            /* mock meta version */
            sds rawmetakey = rocksEncodeMetaKey(db, key1->ptr);
            sds extend = rocksEncodeObjectMetaLen(1);
            sds rawmetaval = rocksEncodeMetaVal(OBJ_ZSET, -1, 2, extend);
            rocksdbPut(META_CF,rawmetakey,rawmetaval, &err);
            test_assert(err == NULL);

            /* compact filter will del data when metaversion > dataversion */
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, SCORE_CF, rawscorekey, &err);
            test_assert(err == NULL);
            test_assert(val == NULL);

            /* clean */
            rocksdbDelete(META_CF, rawmetakey, &err);
            test_assert(err == NULL);
            sdsfree(rawscorekey);
            sdsfree(rawmetakey);
            sdsfree(rawmetaval);
            sdsfree(extend);

            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].scan_count, scan_count);
            test_assert(filt_count == 1);
            test_assert(scan_count == 1);
        }


        /* test3 metaversion <= dataversion */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            resetStatsSwap();

            sds rawscorekey = encodeScoreKey(db, key1->ptr, 1, 10, subkey);
            rocksdbPut(SCORE_CF,rawscorekey,val1->ptr, &err);
            test_assert(err == NULL);
            /* mock meta version &&  dataversion == metaversion */
            sds rawmetakey = rocksEncodeMetaKey(db, key1->ptr);
            sds extend = rocksEncodeObjectMetaLen(1);
            sds rawmetaval = rocksEncodeMetaVal(OBJ_HASH, -1,  1, extend);
            rocksdbPut(META_CF,rawmetakey,rawmetaval, &err);
            test_assert(err == NULL);

            /* compact filter will del data when metaversion > dataversion */
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, SCORE_CF, rawscorekey, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].scan_count, scan_count);
            test_assert(filt_count == 0);
            test_assert(scan_count == 1);

            /* mock string data && dataversion > metaversion */
            rawscorekey = encodeScoreKey(db, key1->ptr, 2, 10, subkey);
            rocksdbPut(SCORE_CF,rawscorekey,val1->ptr, &err);
            test_assert(err == NULL);
            /* compact filter will del data when metaversion > dataversion */
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            val = rocksdbGet(server.rocks->ropts, SCORE_CF, rawscorekey, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);

            /* clean */
            rocksdbDelete(META_CF, rawmetakey, &err);
            test_assert(err == NULL);
            rocksdbDelete(SCORE_CF, rawscorekey, &err);
            test_assert(err == NULL);
            sdsfree(rawscorekey);
            sdsfree(rawmetakey);
            sdsfree(rawmetaval);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].scan_count, scan_count);
            test_assert(filt_count == 0);
            test_assert(scan_count >= 2);
        }

        /* unknow data (unfilte) */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            resetStatsSwap();

            sds unknow = sdsnew("foo");
            rocksdbPut(SCORE_CF,unknow,val1->ptr, &err);
            test_assert(err == NULL);

            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, SCORE_CF, unknow, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);
            /* clean */
            rocksdbDelete(SCORE_CF, unknow, &err);
            test_assert(err == NULL);
            sdsfree(unknow);

            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].scan_count, scan_count);
            test_assert(filt_count == 0);
            test_assert(scan_count == 1);

        }
        /* meta version unknow */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            resetStatsSwap();

            sds rawscorekey = encodeScoreKey(db, key1->ptr, 1, 10, subkey);\
            rocksdbPut(SCORE_CF,rawscorekey,val1->ptr, &err);
            test_assert(err == NULL);
            /* mock meta version &&  dataversion == metaversion */
            sds rawmetakey = rocksEncodeMetaKey(db, key1->ptr);
            sds rawmetaval = sdsnew("foo");
            rocksdbPut(META_CF,rawmetakey,rawmetaval, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, SCORE_CF, rawscorekey, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);
            /* clean */
            rocksdbDelete(META_CF, rawmetakey, &err);
            test_assert(err == NULL);
            rocksdbDelete(SCORE_CF, rawscorekey, &err);
            test_assert(err == NULL);
            sdsfree(rawmetakey);
            sdsfree(rawmetaval);
            sdsfree(rawscorekey);

            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].scan_count, scan_count);
            test_assert(filt_count == 0);
            test_assert(scan_count == 1);
        }
    }
    return error;

}

#endif
