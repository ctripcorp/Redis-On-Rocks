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
            int swap_type;
            long long expire;
            const char* extend;
            size_t extend_len;

            retval = rocksDecodeMetaVal(meta_val,sdslen(meta_val),&swap_type,&expire,
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

/* compact task && ttl compact task in server */

compactTask *compactTaskNew() {
    return zcalloc(sizeof(compactTask));
}

void compactTaskFree(compactTask *task) {
    for (int i = 0; i < task->num_cf; i++) {
        if (task->key_range[i].start_key)
            free(task->key_range[i].start_key);
        if (task->key_range[i].end_key)
            free(task->key_range[i].end_key);
    }
    zfree(task->key_range);
    zfree(task);
}

void genServerTtlCompactTask(void *result, void *pd, int errcode) {
    UNUSED(errcode);
    cfMetas *metas = result;

    // getLevelMetadata(metas);

    // getSstMeta(server.swap_ttl_compact_ctx->sst_age_limit);

    // sort(sst_metas);

    rocksdb_sst_file_metadata_t* start_sst_meta;
    rocksdb_sst_file_metadata_t* end_sst_meta;

    server.swap_ttl_compact_ctx->task = compactTaskNew();
    server.swap_ttl_compact_ctx->task->num_cf = 1;
    server.swap_ttl_compact_ctx->task->key_range = zmalloc(sizeof(cfKeyRange));
    server.swap_ttl_compact_ctx->task->key_range[0].cf_index = DATA_CF;

    // server.swap_ttl_compact_ctx->task->key_range[0].start_key = start_sst_meta.start_key;
    // server.swap_ttl_compact_ctx->task->key_range[0].end_key = end_sst_meta.end_key;
    // server.swap_ttl_compact_ctx->task->key_range[0].start_key_size = start_sst_meta.start_key_size;
    // server.swap_ttl_compact_ctx->task->key_range[0].end_key_size = end_sst_meta.end_key_size;

    // destroy(sst_metas);
    cfMetasFree(metas);
    cfIndexesFree(pd);
}

swapTtlCompactCtx *swapTtlCompactCtxNew() {

    swapTtlCompactCtx *ctx = zmalloc(sizeof(swapTtlCompactCtx));

    ctx->expire_wt = wtdigestCreate(EXPIRE_WT_NUM_BUCKETS);
    wtdigestSetWindow(ctx->expire_wt, server.rocksdb_data_periodic_compaction_seconds);

    ctx->sst_age_limit = INT_MAX;
    ctx->task = NULL;
}

void swapTtlCompactCtxFree(swapTtlCompactCtx *ctx) {
    if (ctx->task) {
      compactTaskFree(ctx->task);
      ctx->task = NULL;
    }
    if (ctx->expire_wt) {
      wtdigestDestroy(ctx->expire_wt);
    }
    zfree(ctx);
}

void rocksdbCompactRangeTaskDone(void *result, void *pd, int errcode) {
    UNUSED(result), UNUSED(errcode);
    compactTaskFree(pd);
}

cfMetas *cfMetasNew() {
    return zmalloc(sizeof(cfMetas));
}

void cfMetasFree(cfMetas *metas) {
    for (int i = 0; i < metas->num; i++) {
        rocksdb_column_family_metadata_destroy(metas->cf_meta[i]);
    }
    zfree(metas);
}

cfIndexes *cfIndexesNew() {
    return zmalloc(sizeof(cfIndexes));
}

void cfIndexesFree(cfIndexes *idxes) {
    zfree(idxes->index);
    zfree(idxes);
}


// void collectSstInfo() {
//         serverLog(LL_NOTICE, "rocksdb initiative compact is working~~~~~");
//     rocks *rocks = serverRocksGetTryReadLock();
//     if (rocks == NULL) {
//         serverLog(LL_NOTICE, "[rocksdb initiative compact] lock failed ");
//         return;
//     }

//     rocksdb_column_family_metadata_t* cf_meta = rocksdb_get_column_family_metadata_cf(rocks->db, rocks->cf_handles[DATA_CF]);

//     size_t level_count = rocksdb_column_family_metadata_get_level_count(cf_meta);
//     // serverLog(LL_NOTICE, "[rocksdb initiative compact] level_count : %lu", level_count);

//     rocksdb_level_metadata_t* level_meta = NULL;
//     size_t files_num = 0;


//     for (int i = level_count - 1; i >= 1; i--) {

//         // from bottom_most level
//         level_meta = rocksdb_column_family_metadata_get_level_metadata(cf_meta, i);

//         // if (level_meta == NULL) {
//         //     rocksdb_column_family_metadata_destroy(cf_meta);
//         //     serverRocksUnlock(rocks);
//         //     serverLog(LL_NOTICE, "[rocksdb initiative compact] level_meta == NULL ");
//         //     continue;
//         // }

//         files_num = rocksdb_level_metadata_get_file_count(level_meta);

//         if (files_num != 0) {
//             // serverLog(LL_NOTICE, "[rocksdb initiative compact] level: %d have sst!!!", i);

//             break;
//         }

//         // serverLog(LL_NOTICE, "[rocksdb initiative compact] level: %d no sst", i);

//         rocksdb_level_metadata_destroy(level_meta);
//     }

//     if (files_num == 0) {
//         // int level = rocksdb_level_metadata_get_level(level_meta);
//         // uint64_t size = rocksdb_level_metadata_get_size(level_meta);

//         serverLog(LL_NOTICE, "[rocksdb initiative compact] L1 ~ L6 no sst");

//         // rocksdb_level_metadata_destroy(level_meta);
//         rocksdb_column_family_metadata_destroy(cf_meta);
//         serverRocksUnlock(rocks);
//         return;
//     }

//     int *file_index_arr = zmalloc(sizeof(int) * files_num);
//     memset(file_index_arr, -1, sizeof(int) * files_num);

//     uint64_t *exist_time_arr = zmalloc(sizeof(uint64_t) * files_num);
//     memset(exist_time_arr, 0, sizeof(uint64_t) * files_num);

//     int file_recorded_num = 0;

//     for (int i = 0; i < files_num; i++) {
//         rocksdb_sst_file_metadata_t* sst_meta = rocksdb_level_metadata_get_sst_file_metadata(level_meta, i);

//         if (sst_meta == NULL) {
//             continue;
//         }

//         uint64_t create_time = rocksdb_sst_file_metadata_get_create_time(sst_meta);

//         rocksdb_sst_file_metadata_destroy(sst_meta);

//         time_t nowtimestamp;
//         time(&nowtimestamp);

//         uint64_t exist_time = nowtimestamp - create_time;

//         if (exist_time <= server.rocksdb_initiative_compaction_SST_age_seconds) {
//             continue;
//         }

//         file_index_arr[file_recorded_num] = i;
//         exist_time_arr[file_recorded_num] = exist_time;

//         file_recorded_num++;

//     }

//     if (file_recorded_num == 0) {
//         zfree(file_index_arr);
//         zfree(exist_time_arr);

//         rocksdb_level_metadata_destroy(level_meta);
//         rocksdb_column_family_metadata_destroy(cf_meta);

//         serverRocksUnlock(rocks);

//         serverLog(LL_NOTICE, "[rocksdb initiative compact] file_recorded_num == 0 ");
//         return;
//     }

//     serverLog(LL_NOTICE, "[rocksdb initiative compact] file_recorded_num: %d ", file_recorded_num);
// }

// void sstInfoSort() {
//         bool is_increasing_order = true; // constitute file index for compaction range in file_index_arr
//     int constitute_index_cursor = 0;

//     for (int i = 0; i < file_recorded_num - 1; i++) {
        
//         for (int j = file_recorded_num - 1; j > i; j--) {
            
//             if (exist_time_arr[j] > exist_time_arr[j - 1]) {
//                 uint64_t tmp_exist_time;
//                 tmp_exist_time = exist_time_arr[j];
//                 exist_time_arr[j] = exist_time_arr[j - 1];
//                 exist_time_arr[j - 1] = tmp_exist_time;

//                 int tmp_file_idx;
//                 tmp_file_idx = file_index_arr[j];
//                 file_index_arr[j] = file_index_arr[j - 1];
//                 file_index_arr[j - 1] = tmp_file_idx;
//             } 
//         }

//         if (i == 0) {
//             constitute_index_cursor = 0;
//             continue;
//         }

//         if (i == 1) { // when there is two file num, order should be decided
//             if (file_index_arr[i] == file_index_arr[i - 1] + 1) {
//                 is_increasing_order = true;
//                 constitute_index_cursor = 1;
//                 continue;
//             } else if (file_index_arr[i] + 1 == file_index_arr[i - 1]) {
//                 is_increasing_order = false;
//                 constitute_index_cursor = 1;
//                 continue;
//             } else {
//                 break; // constitute file index has been broken, no need to continue sorting
//             }
//         }

//         // sort work will continue until the constitute file index is broken
//         // i > 1
//         if (((file_index_arr[i - 1] + 1 == file_index_arr[i]) && is_increasing_order) || 
//             ((file_index_arr[i - 1] == file_index_arr[i] + 1) && !is_increasing_order)) {
//             constitute_index_cursor = i;
//         } else {
//             break; // constitute file index has been broken, no need to continue sorting
//         }
//     }
// }

// void submitTask() {
//     rocksdb_sst_file_metadata_t* smallest_sst_meta;
//     rocksdb_sst_file_metadata_t* largest_sst_meta;

//     if (is_increasing_order) {
//         smallest_sst_meta = rocksdb_level_metadata_get_sst_file_metadata(level_meta, file_index_arr[0]);
//         largest_sst_meta = rocksdb_level_metadata_get_sst_file_metadata(level_meta, file_index_arr[constitute_index_cursor]);
//         serverLog(LL_NOTICE, "[rocksdb initiative compact range task] small file:%d, large file:%d, increase, constitute_index_cursor: %d",
//             file_index_arr[0], file_index_arr[constitute_index_cursor],constitute_index_cursor);
//     } else {
//         smallest_sst_meta = rocksdb_level_metadata_get_sst_file_metadata(level_meta, file_index_arr[constitute_index_cursor]);
//         largest_sst_meta = rocksdb_level_metadata_get_sst_file_metadata(level_meta, file_index_arr[0]);
//         serverLog(LL_NOTICE, "[rocksdb initiative compact range task] small file:%d, large file:%d, no increase, constitute_index_cursor: %d",
//             file_index_arr[constitute_index_cursor],file_index_arr[0], constitute_index_cursor);
//     }

//     serverAssert(smallest_sst_meta != NULL);
//     serverAssert(largest_sst_meta != NULL);

//     size_t smallest_key_name_size;
//     size_t largest_key_name_size;
//     char *smallest_key = rocksdb_sst_file_metadata_get_smallestkey(smallest_sst_meta, &smallest_key_name_size);
//     char *largest_key = rocksdb_sst_file_metadata_get_largestkey(largest_sst_meta, &largest_key_name_size);

//     rocksdb_sst_file_metadata_destroy(smallest_sst_meta);
//     rocksdb_sst_file_metadata_destroy(largest_sst_meta);

//     range.start_key_name = smallest_key;
//     range.end_key_name = largest_key;
//     range.start_key_name_size = smallest_key_name_size;
//     range.end_key_name_size = largest_key_name_size;

//     if (submitUtilTask(ROCKSDB_COMPACT_RANGE_TASK, NULL, NULL, NULL, NULL, &range)) {
//         serverLog(LL_NOTICE, "[rocksdb initiative compact range task set ok] ");
//     } else {
//         serverLog(LL_NOTICE, "[rocksdb initiative compact range task set failed] ");
//         free(range.end_key_name);
//         free(range.start_key_name);
//     }

//     zfree(file_index_arr);
//     zfree(exist_time_arr);

//     rocksdb_level_metadata_destroy(level_meta);
//     rocksdb_column_family_metadata_destroy(cf_meta);

//     serverRocksUnlock(rocks);
// }

// void submitTTLCompactionTask() {

//     collectSstInfo();

//     sstInfoSort();
    
//     submitTask();
// }

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
