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
    atomicGet(server.rocksdb_inflight_snapshot, inflight_snapshot);
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

#ifdef REDIS_TEST
static redisAtomic long long data_filter_call_count;
static redisAtomic long long data_blob_filter_call_count;
static redisAtomic long long score_filter_call_count;
static redisAtomic long long score_blob_filter_call_count;
#endif

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
#ifdef REDIS_TEST
    atomicIncr(data_filter_call_count, 1);
#endif
    return metaVersionFilterFilt(mvfilter, level, DATA_CF,rawkey, rawkey_length, decodeDataVersion);
}

static int dataFilterBlobByKey(void* mvfilter, int level, const char* rawkey,
                               size_t rawkey_length, char** new_value,
                               size_t* new_value_length, char** skip_until,
                               size_t* skip_until_length) {
    UNUSED(new_value);
    UNUSED(new_value_length);
    UNUSED(skip_until);
    UNUSED(skip_until_length);
#ifdef REDIS_TEST
    atomicIncr(data_blob_filter_call_count, 1);
#endif
    return metaVersionFilterFilt(mvfilter, level, DATA_CF,
            rawkey, rawkey_length, decodeDataVersion);
}

rocksdb_compactionfilter_t* createDataCfCompactionFilter(void *state, rocksdb_compactionfiltercontext_t *context) {
    metaVersionFilter *mvfilter = metaVersionFilterCreate();
    UNUSED(state), UNUSED(context);
    rocksdb_compactionfilter_t *cf = rocksdb_compactionfilter_create(mvfilter,
            metaVersionFilterDestroy, dataFilterFilter, dataFilterName);
    rocksdb_compactionfilter_set_filter_blob_by_key(cf, dataFilterBlobByKey);
    return cf;
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
#ifdef REDIS_TEST
    atomicIncr(score_filter_call_count, 1);
#endif
    return metaVersionFilterFilt(mvfilter, level, SCORE_CF,rawkey, rawkey_length, decodeScoreVersion);
}

static int scoreFilterBlobByKey(void* mvfilter, int level, const char* rawkey,
                                size_t rawkey_length, char** new_value,
                                size_t* new_value_length, char** skip_until,
                                size_t* skip_until_length) {
    UNUSED(new_value);
    UNUSED(new_value_length);
    UNUSED(skip_until);
    UNUSED(skip_until_length);
#ifdef REDIS_TEST
    atomicIncr(score_blob_filter_call_count, 1);
#endif
    return metaVersionFilterFilt(mvfilter, level, SCORE_CF,
            rawkey, rawkey_length, decodeScoreVersion);
}

rocksdb_compactionfilter_t* createScoreCfCompactionFilter(void *state, rocksdb_compactionfiltercontext_t *context) {
    metaVersionFilter *mvfilter = metaVersionFilterCreate();
    UNUSED(state), UNUSED(context);
    rocksdb_compactionfilter_t *cf = rocksdb_compactionfilter_create(mvfilter,
            metaVersionFilterDestroy, scoreFilterFilter, scoreFilterName);
    rocksdb_compactionfilter_set_filter_blob_by_key(cf, scoreFilterBlobByKey);
    return cf;
}

static const char* scoreFilterFactoryName(void* arg) {
  (void)arg;
  return "score_cf_filter_factory";
}

rocksdb_compactionfilterfactory_t* createScoreCfCompactionFilterFactory() {
    return rocksdb_compactionfilterfactory_create(NULL,filterFactoryDestructor,
            createScoreCfCompactionFilter,scoreFilterFactoryName);
}

/* 
 * for manual compact task 
 * 1.ttl compact task
 * 2.full compact task 
 */

cfIndexes *cfIndexesNew(uint num) {
    cfIndexes *idxes = zmalloc(sizeof(cfIndexes));
    idxes->num = num;
    idxes->index = zcalloc(sizeof(int) * num);
    return idxes;
}

void cfIndexesFree(cfIndexes *idxes) {
    zfree(idxes->index);
    zfree(idxes);
}

compactKeyRange *compactKeyRangeNew(uint cf_index, char *start_key, char *end_key, size_t start_key_size, size_t end_key_size) {
    serverAssert(cf_index < CF_COUNT);
    compactKeyRange *range = zcalloc(sizeof(compactKeyRange));
    range->cf_index = cf_index;
    range->start_key = start_key;
    range->end_key = end_key;
    range->start_key_size = start_key_size;
    range->end_key_size = end_key_size;
    return range;
}

void compactKeyRangeFree(compactKeyRange *range) {
    if (range->start_key) {
        zlibc_free(range->start_key);
    }
    if (range->end_key) {
        zlibc_free(range->end_key);
    }
    zfree(range);
}

compactTask *compactTaskNew(int compact_type) {
    compactTask *task = zcalloc(sizeof(compactTask));
    task->compact_type = compact_type;
    task->start_time = getMonotonicUs();
    task->count = 0;
    task->capacity = 1;
    task->key_range = zcalloc(sizeof(compactKeyRange *));

    return task;
}

void compactTaskFree(compactTask *task) {
    for (uint i = 0; i < task->count; i++) {
        if (task->key_range[i]) {
            compactKeyRangeFree(task->key_range[i]);
        }
    }
    zfree(task->key_range);
    zfree(task);
}

void compactTaskAppend(compactTask *task, compactKeyRange *key_range) {
    serverAssert(task->count <= task->capacity);
    if (task->count < task->capacity) {
        task->key_range[task->count] = key_range;
        task->count++;
        return;
    }

    task->capacity = task->capacity * 2;

    compactKeyRange **range_arr = zmalloc(task->capacity * sizeof(compactKeyRange *));
    memcpy(range_arr, task->key_range, task->count * sizeof(compactKeyRange *));
    zfree(task->key_range);
    task->key_range = range_arr;

    task->key_range[task->count] = key_range;
    task->count++;
}

static rocksdb_level_metadata_t* getHighestLevelMetaWithSST(rocksdb_column_family_metadata_t *default_meta) {

    size_t level_count = rocksdb_column_family_metadata_get_level_count(default_meta);

    rocksdb_level_metadata_t* level_meta = NULL;
    size_t highest_level_sst_num = 0;

    for (int i = level_count - 1; i >= 1; i--) {
        /* from bottom_most level */
        level_meta = rocksdb_column_family_metadata_get_level_metadata(default_meta, i);
        if (level_meta == NULL) {
            continue;
        }

        highest_level_sst_num = rocksdb_level_metadata_get_file_count(level_meta);
        if (highest_level_sst_num != 0) {
            break;
        }

        rocksdb_level_metadata_destroy(level_meta);
        level_meta = NULL;
    }
    return level_meta;
}

static int getExpiredSstInfo(rocksdb_level_metadata_t* level_meta, long long sst_age_limit, uint *sst_index_arr, uint64_t *sst_age_arr) {

    size_t level_sst_num = rocksdb_level_metadata_get_file_count(level_meta);
    int sst_recorded_num = 0;

    for (uint i = 0; i < level_sst_num; i++) {
        rocksdb_sst_file_metadata_t* sst_meta = rocksdb_level_metadata_get_sst_file_metadata(level_meta, i);
        if (sst_meta == NULL) {
            continue;
        }

        /* seconds */
        uint64_t create_time = rocksdb_sst_file_metadata_get_file_creation_time(sst_meta); 
        rocksdb_sst_file_metadata_destroy(sst_meta);
        long long nowtimestamp = server.mstime;
        
        long long exist_time = nowtimestamp - (long long)create_time * 1000;
        if (exist_time <= sst_age_limit) {
            continue;
        }

        sst_index_arr[sst_recorded_num] = i;
        sst_age_arr[sst_recorded_num] = exist_time;

        sst_recorded_num++;
    }

    return sst_recorded_num;
}

/*
 * sort the sst to get oldest sst range with the continous index(ascending or descending order).
 */
static uint sortExpiredSstInfo(uint *sst_index_arr, uint64_t *sst_age_arr, uint expired_sst_num, bool *is_ascending_order) {

    uint arranged_cursor = 0;

    /* pruning bubble algorithm */
    /* sort in place */
    for (uint i = 0; i < expired_sst_num - 1; i++) {
        for (uint j = expired_sst_num - 1; j > i; j--) {

            if (sst_age_arr[j] > sst_age_arr[j - 1]) {
                uint64_t tmp_exist_time;
                tmp_exist_time = sst_age_arr[j];
                sst_age_arr[j] = sst_age_arr[j - 1];
                sst_age_arr[j - 1] = tmp_exist_time;

                int tmp_sst_idx;
                tmp_sst_idx = sst_index_arr[j];
                sst_index_arr[j] = sst_index_arr[j - 1];
                sst_index_arr[j - 1] = tmp_sst_idx;
            }
        }

        if (i == 0) {
            arranged_cursor = 0;
            continue;
        }

        if (i == 1) { /* when there is two oldest sst arranged, order should be decided */
            if (sst_index_arr[i] == sst_index_arr[i - 1] + 1) {
                *is_ascending_order = true;
                arranged_cursor = 1;
                continue;
            } else if (sst_index_arr[i] + 1 == sst_index_arr[i - 1]) {
                *is_ascending_order = false;
                arranged_cursor = 1;
                continue;
            } else {
                break; /* continuity of sst index has been broken, no need to continue sorting */
            }
        }

        /* sort work will continue until the continuity of sst index is broken */
        /* i > 1 */
        if (((sst_index_arr[i - 1] + 1 == sst_index_arr[i]) && (*is_ascending_order)) || 
            ((sst_index_arr[i - 1] == sst_index_arr[i] + 1) && !(*is_ascending_order))) {
            arranged_cursor = i;
        } else {
            break;
        }
    }

    return arranged_cursor;
}

static compactTask *getTtlCompactTask(rocksdb_level_metadata_t *level_meta, uint *sst_index_arr, uint arranged_cursor, bool is_ascending_order) {

    rocksdb_sst_file_metadata_t* smallest_sst_meta;
    rocksdb_sst_file_metadata_t* largest_sst_meta;

    if (is_ascending_order) {
        smallest_sst_meta = rocksdb_level_metadata_get_sst_file_metadata(level_meta, sst_index_arr[0]);
        largest_sst_meta = rocksdb_level_metadata_get_sst_file_metadata(level_meta, sst_index_arr[arranged_cursor]);
    } else {
        smallest_sst_meta = rocksdb_level_metadata_get_sst_file_metadata(level_meta, sst_index_arr[arranged_cursor]);
        largest_sst_meta = rocksdb_level_metadata_get_sst_file_metadata(level_meta, sst_index_arr[0]);
    }

    serverAssert(smallest_sst_meta != NULL);
    serverAssert(largest_sst_meta != NULL);

    size_t smallest_key_name_size;
    size_t largest_key_name_size;
    char *smallest_key = rocksdb_sst_file_metadata_get_smallestkey(smallest_sst_meta, &smallest_key_name_size);
    char *largest_key = rocksdb_sst_file_metadata_get_largestkey(largest_sst_meta, &largest_key_name_size);

    rocksdb_sst_file_metadata_destroy(smallest_sst_meta);
    rocksdb_sst_file_metadata_destroy(largest_sst_meta);

    compactTask *task = compactTaskNew(TYPE_TTL_COMPACT);

    compactKeyRange *data_key_range = compactKeyRangeNew(DATA_CF, smallest_key, largest_key, smallest_key_name_size, largest_key_name_size);
    compactTaskAppend(task,data_key_range);

    return task;
}

void genServerTtlCompactTask(void *result, void *pd, int errcode) {
    UNUSED(errcode);
    cfIndexesFree(pd);
    cfMetas *metas = result;
    serverAssert(metas->num == 1);

    if (isRunningUtilTask(server.swap_util_task_manager, ROCKSDB_COMPACT_RANGE_TASK)) {
        cfMetasFree(metas);
        return;
    }

    long long sst_age_limit = server.swap_ttl_compact_ctx->expire_stats->sst_age_limit;
    if (!(sst_age_limit > LONG_LONG_MIN && sst_age_limit < LONG_LONG_MAX)) {
        /* illegal age limit for sst. */
        cfMetasFree(metas);
        return;
    }

    serverAssert(metas->cf_index[0] == DATA_CF);
    rocksdb_column_family_metadata_t *default_meta = metas->cf_meta[0];
    rocksdb_level_metadata_t *level_meta = getHighestLevelMetaWithSST(default_meta);
    if (level_meta == NULL) {
        cfMetasFree(metas);
        return;
    }

    size_t highest_level_sst_num = rocksdb_level_metadata_get_file_count(level_meta);
    serverAssert(highest_level_sst_num != 0);

    uint *sst_index_arr = zmalloc(sizeof(int) * highest_level_sst_num);
    memset(sst_index_arr, -1, sizeof(int) * highest_level_sst_num);

    uint64_t *sst_age_arr = zmalloc(sizeof(uint64_t) * highest_level_sst_num);
    memset(sst_age_arr, 0, sizeof(uint64_t) * highest_level_sst_num);

    uint expired_sst_num = getExpiredSstInfo(level_meta, sst_age_limit, sst_index_arr, sst_age_arr);
    if (expired_sst_num == 0) {
        goto end;
    }

    atomicSet(server.swap_ttl_compact_ctx->stat_expired_sst_count, expired_sst_num);

    bool is_ascending_order = true; /* record the order of sst index of compact range. */
    uint arranged_cursor = sortExpiredSstInfo(sst_index_arr, sst_age_arr, expired_sst_num, &is_ascending_order);

    if (server.swap_ttl_compact_ctx->task != NULL) {
        compactTaskFree(server.swap_ttl_compact_ctx->task);
    }
    server.swap_ttl_compact_ctx->task = getTtlCompactTask(level_meta, sst_index_arr, arranged_cursor, is_ascending_order);
    atomicIncr(server.swap_ttl_compact_ctx->stat_request_sst_count, arranged_cursor + 1);

end:
    zfree(sst_index_arr);
    zfree(sst_age_arr);
    rocksdb_level_metadata_destroy(level_meta);
    cfMetasFree(metas);
}

long long getServerMstime() {
    return server.mstime;
}

swapExpireStatus *swapExpireStatusNew() {

    swapExpireStatus *stats = zmalloc(sizeof(swapExpireStatus));
    stats->expire_wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS, getServerMstime);
    wtdigestSetWindow(stats->expire_wt, SWAP_TTL_COMPACT_DEFAULT_EXPIRE_WT_WINDOW);

    stats->sst_age_limit = SWAP_TTL_COMPACT_INVALID_EXPIRE;
    return stats;
}

void swapExpireStatusFree(swapExpireStatus *stats) {
    if (stats->expire_wt) {
        wtdigestDestroy(stats->expire_wt);
    }
    zfree(stats);
}

void swapExpireStatusReset(swapExpireStatus *stats) {
    wtdigestReset(stats->expire_wt);
    stats->sst_age_limit = SWAP_TTL_COMPACT_INVALID_EXPIRE; 
}

swapTtlCompactCtx *swapTtlCompactCtxNew() {
    swapTtlCompactCtx *ctx = zcalloc(sizeof(swapTtlCompactCtx));

    ctx->task = NULL;
    ctx->expire_stats = swapExpireStatusNew();
    ctx->stat_request_compact_times = 0;
    ctx->stat_request_sst_count = 0;
    ctx->stat_expired_sst_count = 0;
    ctx->stat_compacted_data_size = 0;
    ctx->stat_compact_took_us = 0;
    return ctx;
}

void swapTtlCompactCtxFree(swapTtlCompactCtx *ctx) {
    if (ctx->task) {
        compactTaskFree(ctx->task);
        ctx->task = NULL;
    }
    if (ctx->expire_stats) {
        swapExpireStatusFree(ctx->expire_stats);
    }
    zfree(ctx);
}

void swapTtlCompactCtxReset(swapTtlCompactCtx *ctx) {
    if (ctx->task) {
        compactTaskFree(ctx->task);
        ctx->task = NULL;
}
    if (ctx->expire_stats) {
        swapExpireStatusReset(ctx->expire_stats);
    }
}

swapFullCompactCtx *swapFullCompactCtxNew() {
    swapFullCompactCtx *ctx = zcalloc(sizeof(swapFullCompactCtx));
    ctx->task = NULL;
    return ctx;
}

void swapFullCompactCtxFree(swapFullCompactCtx *ctx) {
    if (ctx->task) {
        compactTaskFree(ctx->task);
        ctx->task = NULL;
    }
    zfree(ctx);
}

/* A whole range compaction rewrites every sst of the cf, so once it is through
 * the blob file set record is there and list based blob gc can finally be turned
 * on.
 *
 * A failed push leaves the cf pending, so the next cron round retries it. */
static void blobListRebuildClearPending(compactTask *task) {
    uint64_t took_us = elapsedUs(task->start_time);

    for (uint i = 0; i < task->count; i++) {
        int cf = (int)task->key_range[i]->cf_index;
        monotime pending_since = swapBlobListPendingSince(cf);
        const char *err = NULL;

        if (pending_since == 0) continue;

        if (task->start_time < pending_since) {
            serverLog(LL_NOTICE,
                    "[rocks] blob list of %s cf was asked for again while being "
                    "rebuilt, keeping it pending.", swapGetCFName(cf));
            continue;
        }

        if (!swapBlobListGcIntended(cf)) {
            swapBlobListSetPending(cf, 0);
            continue;
        }

        if (swapPushBlobListGcOption(cf, 1, &err)) {
            serverLog(LL_NOTICE,
                    "[rocks] blob list of %s cf rebuilt in %llu ms, turned on list blob gc.",
                    swapGetCFName(cf), (unsigned long long)(took_us / 1000));
        } else {
            serverLog(LL_WARNING,
                    "[rocks] turn on list blob gc of %s cf failed: %s, will retry.",
                    swapGetCFName(cf), err);
        }
    }
}

void rocksdbCompactRangeTaskDone(void *result, void *pd, int errcode) {
    UNUSED(result), UNUSED(errcode);

    compactTask *task = pd;
    if (task->compact_type == TYPE_FULL_COMPACT) blobListRebuildClearPending(task);
    compactTaskFree(task);
}

/* Returns 1 when blob list rebuild task submitted in this round or full compact task to be submitted in consume cron. */
int blobListRebuildProduceTask(void) {
    int cfs[CF_COUNT], cf_num = 0;

    if (server.swap_full_compact_ctx->task != NULL) return 1;

    for (int cf = 0; cf < CF_COUNT; cf++) {
        if (swapBlobListGcIntended(cf) && !swapBlobListGcApplied(cf))
            cfs[cf_num++] = cf;
    }
    if (cf_num == 0) return 0;

    cfIndexes *idxes = cfIndexesNew(cf_num);
    for (int i = 0; i < cf_num; i++) idxes->index[i] = cfs[i];

    if (!submitUtilTask(ROCKSDB_COLLECT_CF_META_TASK, idxes,
                genServerBlobListRebuildTask, idxes, NULL)) {
        serverLog(LL_NOTICE,
                "[rocksdb] collect cf meta task for full compact set failed.");
        cfIndexesFree(idxes);
    }
    return 1;
}

void genServerBlobListRebuildTask(void *result, void *pd, int errcode) {
    UNUSED(errcode);
    cfIndexesFree(pd);
    cfMetas *metas = result;

    if (isRunningUtilTask(server.swap_util_task_manager, ROCKSDB_COMPACT_RANGE_TASK)) {
        cfMetasFree(metas);
        return;
    }

    compactTask *task = compactTaskNew(TYPE_FULL_COMPACT);

    for (uint i = 0; i < metas->num; i++) {
        int cf = metas->cf_index[i];
        const char *err = NULL;

        if (cf < 0 || metas->cf_meta[i] == NULL) continue;

        if (!swapBlobListGcIntended(cf) || swapBlobListGcApplied(cf)) continue;

        if (cfMetaBlobListIncomplete(metas->cf_meta[i])) {
            compactTaskAppend(task, compactKeyRangeNew(cf, NULL, NULL, 0, 0));
            continue;
        }

        if (swapPushBlobListGcOption(cf, 1, &err)) {
            serverLog(LL_NOTICE,
                    "[rocks] blob list of %s cf already complete, turned on list blob gc.",
                    swapGetCFName(cf));
        } else {
            serverLog(LL_WARNING,
                    "[rocks] turn on list blob gc of %s cf failed: %s, will retry.",
                    swapGetCFName(cf), err);
        }
    }

    cfMetasFree(metas);

    if (task->count == 0) {
        compactTaskFree(task);
        return;
    }

    server.swap_full_compact_ctx->task = task;
    atomicIncr(server.swap_full_compact_ctx->stat_request_cf_count, task->count);
}

/* Returns 1 when a full compact was tried to be submitted to the util task. */
int blobListRebuildConsumeTask(void) {
    if (server.swap_full_compact_ctx->task == NULL) return 0;

    compactTask *task = server.swap_full_compact_ctx->task;

    for (uint i = 0; i < task->count; i++) {
        int cf = (int)task->key_range[i]->cf_index;

        if (swapBlobListGcIntended(cf) && !swapBlobListGcApplied(cf)) continue;

        serverLog(LL_NOTICE,
                "[rocksdb] %s cf no longer needs a blob list rebuild, dropping "
                "the full compact task covering it.", swapGetCFName(cf));
        compactTaskFree(task);
        server.swap_full_compact_ctx->task = NULL;
        return 0;
    }

    task->start_time = getMonotonicUs();

    if (!submitUtilTask(ROCKSDB_COMPACT_RANGE_TASK, task,
                rocksdbCompactRangeTaskDone, task, NULL)) {
        serverLog(LL_NOTICE,
                "[rocksdb] full compact task set failed, dropping it so the next "
                "round rebuilds it from fresh metadata.");
        compactTaskFree(task);
    } else {
        atomicIncr(server.swap_full_compact_ctx->stat_request_compact_times, 1);
    }

    server.swap_full_compact_ctx->task = NULL; /* task move to utilctx */
    return 1;
}

cfMetas *cfMetasNew(uint cf_num) {
    cfMetas *metas = zmalloc(sizeof(cfMetas));
    metas->num = cf_num;
    metas->cf_meta = zcalloc(sizeof(rocksdb_column_family_metadata_t*) * cf_num);
    metas->cf_index = zmalloc(sizeof(int) * cf_num);
    for (uint i = 0; i < cf_num; i++) metas->cf_index[i] = -1;
    return metas;
}

void cfMetasFree(cfMetas *metas) {
    for (uint i = 0; i < metas->num; i++) {
        if (metas->cf_meta[i]) {
            rocksdb_column_family_metadata_destroy(metas->cf_meta[i]);
        }
    }
    zfree(metas->cf_meta);
    zfree(metas->cf_index);
    zfree(metas);
}

/* When list based blob gc was last asked for but not yet pushed down, 0 meaning
 * there is nothing pending. */
static monotime swap_blob_list_pending[CF_COUNT];

bool swapBlobListGcApplied(int cf) {
    serverAssert(cf < CF_COUNT);
    return !swap_blob_list_pending[cf];
}

monotime swapBlobListPendingSince(int cf) {
    serverAssert(cf < CF_COUNT);
    return swap_blob_list_pending[cf];
}

void swapBlobListSetPending(int cf, bool pending) {
    serverAssert(cf < CF_COUNT);
    swap_blob_list_pending[cf] = pending ? getMonotonicUs() : 0;
}

void swapBlobListPendingInit(void) {
    for (int cf = 0; cf < CF_COUNT; cf++)
        swapBlobListSetPending(cf, swapBlobListGcIntended(cf));
}

int cfMetaBlobListIncomplete(rocksdb_column_family_metadata_t *cf_meta) {
    int incomplete = 0;

    if (cf_meta == NULL) return 0;

    size_t level_num = rocksdb_column_family_metadata_get_level_count(cf_meta);
    for (size_t l = 0; l < level_num && !incomplete; l++) {
        rocksdb_level_metadata_t *level_meta =
            rocksdb_column_family_metadata_get_level_metadata(cf_meta, l);
        if (level_meta == NULL) continue;

        size_t sst_num = rocksdb_level_metadata_get_file_count(level_meta);
        for (size_t f = 0; f < sst_num; f++) {
            rocksdb_sst_file_metadata_t *sst_meta =
                rocksdb_level_metadata_get_sst_file_metadata(level_meta, f);
            if (sst_meta == NULL) continue;

            if (rocksdb_sst_file_metadata_get_blob_file_set_count(sst_meta) == 0 &&
                rocksdb_sst_file_metadata_get_oldest_blob_file_number(sst_meta) != 0) {
                incomplete = 1;
            }

            rocksdb_sst_file_metadata_destroy(sst_meta);
            if (incomplete) break;
        }
        rocksdb_level_metadata_destroy(level_meta);
    }

    return incomplete;
}

uint64_t cfMetaOrphanBlobGarbageBytes(rocksdb_column_family_metadata_t *cf_meta) {
    uint64_t garbage_bytes = 0;

    if (cf_meta == NULL) return 0;

    size_t blob_num = rocksdb_column_family_metadata_get_blob_file_count(cf_meta);
    for (size_t i = 0; i < blob_num; i++) {
        rocksdb_blob_metadata_t *blob_meta =
            rocksdb_column_family_metadata_get_blob_metadata(cf_meta, i);
        if (blob_meta == NULL) continue;

        if (rocksdb_blob_metadata_get_full_linked_ssts_count(blob_meta) == 0) {
            garbage_bytes +=
                rocksdb_blob_metadata_get_garbage_blob_bytes(blob_meta);
        }

        rocksdb_blob_metadata_destroy(blob_meta);
    }

    return garbage_bytes;
}

sds genSwapFullCompactInfoString(sds info) {
    int pending_cf_count = 0;

    /* Names of the cf still waiting are deliberately left out, a comma separated
     * list does not fit an info field. Use "swap blob-list-pending" for that. */
    for (int cf = 0; cf < CF_COUNT; cf++) {
        if (swapBlobListGcIntended(cf) && !swapBlobListGcApplied(cf))
            pending_cf_count++;
    }

    info = sdscatprintf(info,
            "swap_full_compact:times=%llu,request_cf_count=%llu,"
            "compacted_data_size=%llu,compact_took_us=%llu,"
            "blob_list_pending_cf_count=%d\r\n",
            server.swap_full_compact_ctx->stat_request_compact_times,
            server.swap_full_compact_ctx->stat_request_cf_count,
            server.swap_full_compact_ctx->stat_compacted_data_size,
            server.swap_full_compact_ctx->stat_compact_took_us,
            pending_cf_count);
    return info;
}

sds genSwapTtlCompactInfoString(sds info) {
    info = sdscatprintf(info,
            "swap_ttl_compact:times=%llu,request_sst_count=%llu,"
            "expired_sst_count=%llu,compacted_data_size=%llu,"
            "compact_took_us=%llu,sst_age_limit=%lld\r\n",
            server.swap_ttl_compact_ctx->stat_request_compact_times,
            server.swap_ttl_compact_ctx->stat_request_sst_count,
            server.swap_ttl_compact_ctx->stat_expired_sst_count,
            server.swap_ttl_compact_ctx->stat_compacted_data_size,
            server.swap_ttl_compact_ctx->stat_compact_took_us,
            server.swap_ttl_compact_ctx->expire_stats->sst_age_limit);
    return info;
}

void ttlCompactRefreshSstAgeLimit() {
    if (!iAmMaster()) {
        /* slave get sst age limit in "swap.info" cmd propagated from master. */
        return;
    }

    if (server.swap_ttl_compact_enabled &&
                server.active_expire_enabled && !isImportingExpireDisabled()) {
            /* if expire is disabled, the sampling is stopped. */
            wtdigest *expire_wt = server.swap_ttl_compact_ctx->expire_stats->expire_wt;
            swapExpireStatus *expire_stats = server.swap_ttl_compact_ctx->expire_stats;
            long long keys_num = dbTotalServerKeyCount();
            long long sampled_size = wtdigestSize(expire_wt);

            if (sampled_size == 0) {
                expire_stats->sst_age_limit = 0;
            } else if (wtdigestGetRunnningTime(expire_wt) > wtdigestGetWindow(expire_wt) ||
                       sampled_size >= keys_num) {
                double percentile = (double)server.swap_ttl_compact_expire_percentile / 100;
                double res = wtdigestQuantile(expire_wt, percentile);
                if (res >= LLONG_MAX || res <= LLONG_MIN) {
                    /* maybe overflow happened, which is unexpected. */
                    expire_stats->sst_age_limit = SWAP_TTL_COMPACT_INVALID_EXPIRE;
                } else {
                    expire_stats->sst_age_limit = (long long)res;
                }
            } else {
                expire_stats->sst_age_limit = SWAP_TTL_COMPACT_INVALID_EXPIRE;
            }
    } else {
        swapExpireStatusReset(server.swap_ttl_compact_ctx->expire_stats);
    }
}

int ttlCompactProduceTask() {
    if (server.swap_ttl_compact_enabled && server.swap_ttl_compact_ctx->task == NULL &&
        (server.swap_ttl_compact_ctx->expire_stats->sst_age_limit != SWAP_TTL_COMPACT_INVALID_EXPIRE)) {
        cfIndexes *idxes = cfIndexesNew(1);
        idxes->index[0] = DATA_CF;
        if (!submitUtilTask(ROCKSDB_COLLECT_CF_META_TASK, idxes, genServerTtlCompactTask, idxes, NULL)) {
            serverLog(LL_NOTICE, "[rocksdb] collect cf meta task set failed.");
            cfIndexesFree(idxes);
        }
        return 1;
    }
    return 0;
}

int ttlCompactConsumeTask() {
    if (!server.swap_ttl_compact_enabled ||
            server.swap_ttl_compact_ctx->task == NULL)
        return 0;

    compactTask *task = server.swap_ttl_compact_ctx->task;
    task->start_time = getMonotonicUs();

    if (!submitUtilTask(ROCKSDB_COMPACT_RANGE_TASK, task,
                rocksdbCompactRangeTaskDone, task, NULL)) {
        serverLog(LL_NOTICE,
                "[rocksdb] ttl compact task set failed, dropping it so the next "
                "round rebuilds it from fresh metadata.");
        compactTaskFree(task);
    } else {
        atomicIncr(server.swap_ttl_compact_ctx->stat_request_compact_times, 1);
    }

    server.swap_ttl_compact_ctx->task = NULL; /* task move to utilctx */
    return 1;
}

void compactProduceTask(void) {
    if (isRunningUtilTask(server.swap_util_task_manager, ROCKSDB_COMPACT_RANGE_TASK))
        return;

    if (blobListRebuildProduceTask()) return;

    ttlCompactProduceTask();
}

void compactConsumeTask(void) {
    if (isRunningUtilTask(server.swap_util_task_manager, ROCKSDB_COMPACT_RANGE_TASK))
        goto end;

    if (blobListRebuildConsumeTask()) goto end;

    ttlCompactConsumeTask();

end:
    if (server.swap_full_compact_ctx->task != NULL) {
        compactTaskFree(server.swap_full_compact_ctx->task);
        server.swap_full_compact_ctx->task = NULL;
    }
    if (server.swap_ttl_compact_ctx->task != NULL) {
        compactTaskFree(server.swap_ttl_compact_ctx->task);
        server.swap_ttl_compact_ctx->task = NULL;
    }
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

compactTask *mockFullCompactTask() {

    compactTask *task = compactTaskNew(TYPE_FULL_COMPACT);

    compactKeyRange *meta_key_range = compactKeyRangeNew(META_CF, NULL, NULL, 0, 0);
    compactKeyRange *data_key_range = compactKeyRangeNew(DATA_CF, NULL, NULL, 0, 0);
    compactKeyRange *score_key_range = compactKeyRangeNew(SCORE_CF, NULL, NULL, 0, 0);
    
    compactTaskAppend(task,meta_key_range);
    compactTaskAppend(task,data_key_range);
    compactTaskAppend(task,score_key_range);
    return task;
}

compactTask *mockTtlCompactTask() {

    compactTask *task = compactTaskNew(TYPE_TTL_COMPACT);
    compactKeyRange *data_key_range = compactKeyRangeNew(DATA_CF, NULL, NULL, 0, 0);    
    compactTaskAppend(task,data_key_range);
    return task;
}

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
    long long blob_call_count, filter_call_count;
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

   TEST("exec: data compaction filter func (blob - FilterBlobByKey)") {
        int orig_enable_blob = server.rocksdb_data_enable_blob_files;
        unsigned long long orig_min_blob_size = server.rocksdb_data_min_blob_size;

        /* Enable blob files: min_blob_size=0 so any value is stored as blob */
        {
            char *cf_err = NULL;
            const char *blob_keys[] = {"enable_blob_files", "min_blob_size"};
            const char *blob_vals[] = {"true", "0"};
            rocksdb_set_options_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF],
                                   2, blob_keys, blob_vals, &cf_err);
            test_assert(cf_err == NULL);
        }

        /* test1 no-meta filter (blob): FilterBlobByKey returns kRemove */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            resetStatsSwap();
            atomicSet(data_filter_call_count, 0);
            atomicSet(data_blob_filter_call_count, 0);
            sds rawkey = rocksEncodeDataKey(db, key1->ptr, 1, subkey);
            rocksdbPut(DATA_CF, rawkey, val1->ptr, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            test_assert(val == NULL);
            sdsfree(rawkey);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].scan_count, scan_count);
            test_assert(filt_count == 1);
            test_assert(scan_count >= 1);
            atomicGet(data_blob_filter_call_count, blob_call_count);
            test_assert(blob_call_count >= 1);
            atomicGet(data_filter_call_count, filter_call_count);
            test_assert(filter_call_count == 0);
        }

        /* test2 meta_version > key_version (blob): FilterBlobByKey returns kRemove */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            resetStatsSwap();
            atomicSet(data_filter_call_count, 0);
            atomicSet(data_blob_filter_call_count, 0);
            sds rawkey = rocksEncodeDataKey(db, key1->ptr, 1, subkey);
            rocksdbPut(DATA_CF, rawkey, val1->ptr, &err);
            test_assert(err == NULL);
            sds rawmetakey = rocksEncodeMetaKey(db, key1->ptr);
            sds extend = rocksEncodeObjectMetaLen(1);
            sds rawmetaval = rocksEncodeMetaVal(OBJ_HASH, -1, 2, extend);
            rocksdbPut(META_CF, rawmetakey, rawmetaval, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            test_assert(val == NULL);
            rocksdbDelete(META_CF, rawmetakey, &err);
            test_assert(err == NULL);
            sdsfree(rawkey);
            sdsfree(rawmetakey);
            sdsfree(rawmetaval);
            sdsfree(extend);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].scan_count, scan_count);
            test_assert(filt_count == 1);
            test_assert(scan_count >= 1);
            atomicGet(data_blob_filter_call_count, blob_call_count);
            test_assert(blob_call_count >= 1);
            atomicGet(data_filter_call_count, filter_call_count);
            test_assert(filter_call_count == 0);
        }

        /* test3 meta_version <= key_version (blob): FilterBlobByKey returns kKeep */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            resetStatsSwap();
            atomicSet(data_filter_call_count, 0);
            atomicSet(data_blob_filter_call_count, 0);
            sds rawkey = rocksEncodeDataKey(db, key1->ptr, 1, subkey);
            rocksdbPut(DATA_CF, rawkey, val1->ptr, &err);
            test_assert(err == NULL);
            sds rawmetakey = rocksEncodeMetaKey(db, key1->ptr);
            sds extend = rocksEncodeObjectMetaLen(1);
            sds rawmetaval = rocksEncodeMetaVal(OBJ_HASH, -1, 1, extend);
            rocksdbPut(META_CF, rawmetakey, rawmetaval, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);

            /* key_version > meta_version → also kept */
            rawkey = rocksEncodeDataKey(db, key1->ptr, 2, subkey);
            rocksdbPut(DATA_CF, rawkey, val1->ptr, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF], NULL, 0, NULL, 0);
            val = rocksdbGet(server.rocks->ropts, DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);

            rocksdbDelete(META_CF, rawmetakey, &err);
            test_assert(err == NULL);
            rocksdbDelete(DATA_CF, rawkey, &err);
            test_assert(err == NULL);
            sdsfree(rawkey);
            sdsfree(rawmetakey);
            sdsfree(rawmetaval);
            sdsfree(extend);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[DATA_CF].scan_count, scan_count);
            test_assert(filt_count == 0);
            test_assert(scan_count >= 1);
            atomicGet(data_blob_filter_call_count, blob_call_count);
            test_assert(blob_call_count >= 1);
            atomicGet(data_filter_call_count, filter_call_count);
            test_assert(filter_call_count == 0);
        }

        /* Restore blob settings to original values */
        {
            char *cf_err = NULL;
            char min_blob_size_str[32];
            snprintf(min_blob_size_str, sizeof(min_blob_size_str), "%llu", orig_min_blob_size);
            const char *blob_keys[] = {"enable_blob_files", "min_blob_size"};
            const char *blob_vals[] = {orig_enable_blob ? "true" : "false", min_blob_size_str};
            rocksdb_set_options_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF],
                                   2, blob_keys, blob_vals, &cf_err);
            test_assert(cf_err == NULL);
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

   TEST("exec: score compaction filter func (blob - FilterBlobByKey)") {
        int orig_enable_blob = server.rocksdb_data_enable_blob_files;
        unsigned long long orig_min_blob_size = server.rocksdb_data_min_blob_size;

        /* Enable blob files on score cf: min_blob_size=0 so any value is
         * stored as blob. Score cf shares the rocksdb.data.* blob knobs with
         * data cf, see rocksOpen(). */
        {
            char *cf_err = NULL;
            const char *blob_keys[] = {"enable_blob_files", "min_blob_size"};
            const char *blob_vals[] = {"true", "0"};
            rocksdb_set_options_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF],
                                   2, blob_keys, blob_vals, &cf_err);
            test_assert(cf_err == NULL);
        }

        /* test1 no-meta filter (blob): FilterBlobByKey returns kRemove */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            resetStatsSwap();
            atomicSet(score_filter_call_count, 0);
            atomicSet(score_blob_filter_call_count, 0);
            sds rawscorekey = encodeScoreKey(db, key1->ptr, 1, 10, subkey);
            rocksdbPut(SCORE_CF, rawscorekey, val1->ptr, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, SCORE_CF, rawscorekey, &err);
            test_assert(err == NULL);
            test_assert(val == NULL);
            sdsfree(rawscorekey);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].scan_count, scan_count);
            test_assert(filt_count == 1);
            test_assert(scan_count == 1);
            atomicGet(score_blob_filter_call_count, blob_call_count);
            test_assert(blob_call_count >= 1);
            atomicGet(score_filter_call_count, filter_call_count);
            test_assert(filter_call_count == 0);
        }

        /* test2 meta_version > key_version (blob): FilterBlobByKey returns kRemove */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            resetStatsSwap();
            atomicSet(score_filter_call_count, 0);
            atomicSet(score_blob_filter_call_count, 0);
            sds rawscorekey = encodeScoreKey(db, key1->ptr, 1, 10, subkey);
            rocksdbPut(SCORE_CF, rawscorekey, val1->ptr, &err);
            test_assert(err == NULL);
            sds rawmetakey = rocksEncodeMetaKey(db, key1->ptr);
            sds extend = rocksEncodeObjectMetaLen(1);
            sds rawmetaval = rocksEncodeMetaVal(OBJ_ZSET, -1, 2, extend);
            rocksdbPut(META_CF, rawmetakey, rawmetaval, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, SCORE_CF, rawscorekey, &err);
            test_assert(err == NULL);
            test_assert(val == NULL);
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
            atomicGet(score_blob_filter_call_count, blob_call_count);
            test_assert(blob_call_count >= 1);
            atomicGet(score_filter_call_count, filter_call_count);
            test_assert(filter_call_count == 0);
        }

        /* test3 meta_version <= key_version (blob): FilterBlobByKey returns kKeep */
        {
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            resetStatsSwap();
            atomicSet(score_filter_call_count, 0);
            atomicSet(score_blob_filter_call_count, 0);
            sds rawscorekey1 = encodeScoreKey(db, key1->ptr, 1, 10, subkey);
            rocksdbPut(SCORE_CF, rawscorekey1, val1->ptr, &err);
            test_assert(err == NULL);
            sds rawmetakey = rocksEncodeMetaKey(db, key1->ptr);
            sds extend = rocksEncodeObjectMetaLen(1);
            sds rawmetaval = rocksEncodeMetaVal(OBJ_ZSET, -1, 1, extend);
            rocksdbPut(META_CF, rawmetakey, rawmetaval, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            sds val = rocksdbGet(server.rocks->ropts, SCORE_CF, rawscorekey1, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);

            /* key_version > meta_version => also kept */
            sds rawscorekey2 = encodeScoreKey(db, key1->ptr, 2, 10, subkey);
            rocksdbPut(SCORE_CF, rawscorekey2, val1->ptr, &err);
            test_assert(err == NULL);
            rocksdb_compact_range_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF], NULL, 0, NULL, 0);
            val = rocksdbGet(server.rocks->ropts, SCORE_CF, rawscorekey2, &err);
            test_assert(err == NULL);
            test_assert(val != NULL);
            sdsfree(val);

            /* clean */
            rocksdbDelete(META_CF, rawmetakey, &err);
            test_assert(err == NULL);
            rocksdbDelete(SCORE_CF, rawscorekey1, &err);
            test_assert(err == NULL);
            rocksdbDelete(SCORE_CF, rawscorekey2, &err);
            test_assert(err == NULL);
            sdsfree(rawscorekey1);
            sdsfree(rawscorekey2);
            sdsfree(rawmetakey);
            sdsfree(rawmetaval);
            sdsfree(extend);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].filt_count, filt_count);
            atomicGet(server.ror_stats->compaction_filter_stats[SCORE_CF].scan_count, scan_count);
            test_assert(filt_count == 0);
            test_assert(scan_count >= 2);
            atomicGet(score_blob_filter_call_count, blob_call_count);
            test_assert(blob_call_count >= 2);
            atomicGet(score_filter_call_count, filter_call_count);
            test_assert(filter_call_count == 0);
        }

        /* Restore blob settings to original values */
        {
            char *cf_err = NULL;
            char min_blob_size_str[32];
            snprintf(min_blob_size_str, sizeof(min_blob_size_str), "%llu", orig_min_blob_size);
            const char *blob_keys[] = {"enable_blob_files", "min_blob_size"};
            const char *blob_vals[] = {orig_enable_blob ? "true" : "false", min_blob_size_str};
            rocksdb_set_options_cf(server.rocks->db, server.rocks->cf_handles[SCORE_CF],
                                   2, blob_keys, blob_vals, &cf_err);
            test_assert(cf_err == NULL);
        }
   }

    TEST("compactTask - new free") {
        compactTask *task1 = mockFullCompactTask();
        test_assert(task1->count == 3);
        test_assert(task1->capacity == 4);
        compactTaskFree(task1);

        compactTask *task2 = mockTtlCompactTask();
        test_assert(task2->count == 1);
        test_assert(task2->capacity == 1);
        compactTaskFree(task2);
    }

    TEST("blob list gc - pending state machine") {
        int orig_data_intended = server.rocksdb_data_enable_blob_list_garbage_collection;
        int orig_meta_intended = server.rocksdb_meta_enable_blob_list_garbage_collection;
        const char *cf_err = NULL;

        /* Pushing the option down must work at all, otherwise every assertion
         * below that expects pending to be cleared is meaningless. */
        test_assert(swapPushBlobListGcOption(DATA_CF, 1, &cf_err));
        test_assert(swapPushBlobListGcOption(DATA_CF, 0, &cf_err));

        /* gc not asked for: nothing pending. Note GcApplied only means "no push
         * outstanding", it reads true for a cf that has gc turned off. */
        server.rocksdb_data_enable_blob_list_garbage_collection = 0;
        server.rocksdb_meta_enable_blob_list_garbage_collection = 0;
        swapBlobListPendingInit();
        for (int cf = 0; cf < CF_COUNT; cf++) {
            test_assert(!swapBlobListGcIntended(cf));
            test_assert(swapBlobListPendingSince(cf) == 0);
            test_assert(swapBlobListGcApplied(cf));
        }

        /* gc asked for: every cf starts out pending, nothing pushed down yet */
        server.rocksdb_data_enable_blob_list_garbage_collection = 1;
        server.rocksdb_meta_enable_blob_list_garbage_collection = 1;
        swapBlobListPendingInit();
        for (int cf = 0; cf < CF_COUNT; cf++) {
            test_assert(swapBlobListGcIntended(cf));
            test_assert(swapBlobListPendingSince(cf) != 0);
            test_assert(!swapBlobListGcApplied(cf));
        }

        /* the data knob covers data and score cf, the meta knob only meta cf */
        server.rocksdb_data_enable_blob_list_garbage_collection = 0;
        test_assert(!swapBlobListGcIntended(DATA_CF));
        test_assert(!swapBlobListGcIntended(SCORE_CF));
        test_assert(swapBlobListGcIntended(META_CF));
        server.rocksdb_meta_enable_blob_list_garbage_collection = 0;
        server.rocksdb_data_enable_blob_list_garbage_collection = 1;
        test_assert(swapBlobListGcIntended(DATA_CF));
        test_assert(swapBlobListGcIntended(SCORE_CF));
        test_assert(!swapBlobListGcIntended(META_CF));
        server.rocksdb_meta_enable_blob_list_garbage_collection = 1;

        /* set / clear round trip */
        swapBlobListSetPending(DATA_CF, 0);
        test_assert(swapBlobListPendingSince(DATA_CF) == 0);
        test_assert(swapBlobListGcApplied(DATA_CF));
        swapBlobListSetPending(DATA_CF, 1);
        test_assert(swapBlobListPendingSince(DATA_CF) != 0);
        test_assert(!swapBlobListGcApplied(DATA_CF));

        /* rebuild finished after pending was recorded => option pushed down */
        {
            swapBlobListSetPending(DATA_CF, 1);
            monotime pending_since = swapBlobListPendingSince(DATA_CF);
            usleep(1000);
            compactTask *task = compactTaskNew(TYPE_FULL_COMPACT);
            compactTaskAppend(task, compactKeyRangeNew(DATA_CF, NULL, NULL, 0, 0));
            test_assert(task->start_time > pending_since);
            blobListRebuildClearPending(task);
            test_assert(swapBlobListGcApplied(DATA_CF));
            compactTaskFree(task);
        }

        /* asked for again while being rebuilt => stays pending, the rebuild that
         * just finished did not cover the newer request */
        {
            compactTask *task = compactTaskNew(TYPE_FULL_COMPACT);
            compactTaskAppend(task, compactKeyRangeNew(DATA_CF, NULL, NULL, 0, 0));
            usleep(1000);
            swapBlobListSetPending(DATA_CF, 1);
            test_assert(swapBlobListPendingSince(DATA_CF) > task->start_time);
            blobListRebuildClearPending(task);
            test_assert(!swapBlobListGcApplied(DATA_CF));
            compactTaskFree(task);
        }

        /* a task covering several cf clears them all */
        {
            for (int cf = 0; cf < CF_COUNT; cf++) swapBlobListSetPending(cf, 1);
            usleep(1000);
            compactTask *task = compactTaskNew(TYPE_FULL_COMPACT);
            for (int cf = 0; cf < CF_COUNT; cf++)
                compactTaskAppend(task, compactKeyRangeNew(cf, NULL, NULL, 0, 0));
            blobListRebuildClearPending(task);
            for (int cf = 0; cf < CF_COUNT; cf++)
                test_assert(swapBlobListGcApplied(cf));
            compactTaskFree(task);
        }

        /* restore: turn list gc back off in rocksdb and reset the intent */
        for (int cf = 0; cf < CF_COUNT; cf++)
            test_assert(swapPushBlobListGcOption(cf, 0, &cf_err));
        server.rocksdb_data_enable_blob_list_garbage_collection = orig_data_intended;
        server.rocksdb_meta_enable_blob_list_garbage_collection = orig_meta_intended;
        swapBlobListPendingInit();
    }

    TEST("server ttl compact task - during no sst") {
    
        server.swap_ttl_compact_ctx = swapTtlCompactCtxNew();

        cfIndexes *idxes = cfIndexesNew(1);

        /* mock result of collect meta task */
        cfMetas *cf_metas = cfMetasNew(1);
        cf_metas->cf_index[0] = DATA_CF;
        cf_metas->cf_meta[0] = rocksdb_get_column_family_metadata_cf(server.rocks->db, server.rocks->cf_handles[DATA_CF]);

        genServerTtlCompactTask(cf_metas, idxes, 0);

        if (server.swap_ttl_compact_ctx->task)
            compactTaskFree(server.swap_ttl_compact_ctx->task);
    }

    TEST("swapTtlCompactCtx - new & reset & free") {
        
        server.swap_ttl_compact_ctx = swapTtlCompactCtxNew();

        /* mock server operation 
         * add expire, get sst_age_limit and task */
        wtdigestAdd(server.swap_ttl_compact_ctx->expire_stats->expire_wt, 10, 1);
        server.swap_ttl_compact_ctx->expire_stats->sst_age_limit = 10;
        server.swap_ttl_compact_ctx->task = compactTaskNew(TYPE_TTL_COMPACT);

        swapTtlCompactCtxReset(server.swap_ttl_compact_ctx);

        test_assert(server.swap_ttl_compact_ctx->task == NULL);
        test_assert(server.swap_ttl_compact_ctx->expire_stats->sst_age_limit == SWAP_TTL_COMPACT_INVALID_EXPIRE);
        test_assert(wtdigestSize(server.swap_ttl_compact_ctx->expire_stats->expire_wt) == 0);

        swapTtlCompactCtxFree(server.swap_ttl_compact_ctx); 
        server.swap_ttl_compact_ctx = NULL;
    }

    TEST("sortExpiredSstInfo - test 0") {
        
        /* mock sst info */
        size_t highest_level_sst_num = 5;

        uint sst_index_arr[5] = {1, 2, 5, 30, 31};
        uint64_t sst_age_arr[5] = {10, 20, 99, 88, 199};

        bool is_ascending_order = true; /* record the order of sst index of compact range. */
        uint arranged_cursor = sortExpiredSstInfo(sst_index_arr, sst_age_arr, highest_level_sst_num, &is_ascending_order);

        test_assert(arranged_cursor == 0);

        test_assert(sst_index_arr[0] == 31);
        test_assert(sst_index_arr[1] == 5);

        test_assert(sst_age_arr[0] == 199);
        test_assert(sst_age_arr[1] == 99);
    }

    TEST("sortExpiredSstInfo - test 1") {
        
        /* mock sst info */
        size_t highest_level_sst_num = 5;

        uint sst_index_arr[5] = {1, 2, 30, 31, 50};
        uint64_t sst_age_arr[5] = {10, 20, 188, 199, 99};

        bool is_ascending_order = true; /* record the order of sst index of compact range. */
        uint arranged_cursor = sortExpiredSstInfo(sst_index_arr, sst_age_arr, highest_level_sst_num, &is_ascending_order);

        test_assert(arranged_cursor == 1);
        test_assert(is_ascending_order == false);

        test_assert(sst_index_arr[0] == 31);
        test_assert(sst_index_arr[1] == 30);
        test_assert(sst_index_arr[2] == 50);

        test_assert(sst_age_arr[0] == 199);
        test_assert(sst_age_arr[1] == 188);
        test_assert(sst_age_arr[2] == 99);
    }

    TEST("sortExpiredSstInfo - test 2") {
        
        /* mock sst info */
        size_t highest_level_sst_num = 5;

        uint sst_index_arr[5] = {1, 2, 30, 31, 50};
        uint64_t sst_age_arr[5] = {10, 20, 199, 188, 99};

        bool is_ascending_order = true; /* record the order of sst index of compact range. */
        uint arranged_cursor = sortExpiredSstInfo(sst_index_arr, sst_age_arr, highest_level_sst_num, &is_ascending_order);

        test_assert(arranged_cursor == 1);
        test_assert(is_ascending_order == true);

        test_assert(sst_index_arr[0] == 30);
        test_assert(sst_index_arr[1] == 31);
        test_assert(sst_index_arr[2] == 50);

        test_assert(sst_age_arr[0] == 199);
        test_assert(sst_age_arr[1] == 188);
        test_assert(sst_age_arr[2] == 99);
    }

    TEST("sortExpiredSstInfo - test 3") {
        
        /* mock sst info */
        size_t highest_level_sst_num = 6;

        uint sst_index_arr[6] = {1, 2, 30, 31, 50, 32};
        uint64_t sst_age_arr[6] = {10, 20, 188, 199, 99, 299};

        bool is_ascending_order = true; /* record the order of sst index of compact range. */
        uint arranged_cursor = sortExpiredSstInfo(sst_index_arr, sst_age_arr, highest_level_sst_num, &is_ascending_order);

        test_assert(arranged_cursor == 2);
        test_assert(is_ascending_order == false);

        test_assert(sst_index_arr[0] == 32);
        test_assert(sst_index_arr[1] == 31);
        test_assert(sst_index_arr[2] == 30);
        test_assert(sst_index_arr[3] == 50);

        test_assert(sst_age_arr[0] == 299);
        test_assert(sst_age_arr[1] == 199);
        test_assert(sst_age_arr[2] == 188);
        test_assert(sst_age_arr[3] == 99);

    }

    TEST("sortExpiredSstInfo - test 4") {
        
        /* mock sst info */
        size_t highest_level_sst_num = 6;

        uint sst_index_arr[6] = {1, 2, 30, 31, 50, 32};
        uint64_t sst_age_arr[6] = {10, 20, 299, 199, 99, 188};

        bool is_ascending_order = true; /* record the order of sst index of compact range. */
        uint arranged_cursor = sortExpiredSstInfo(sst_index_arr, sst_age_arr, highest_level_sst_num, &is_ascending_order);

        test_assert(arranged_cursor == 2);
        test_assert(is_ascending_order == true);

        test_assert(sst_index_arr[0] == 30);
        test_assert(sst_index_arr[1] == 31);
        test_assert(sst_index_arr[2] == 32);
        test_assert(sst_index_arr[3] == 50);

        test_assert(sst_age_arr[0] == 299);
        test_assert(sst_age_arr[1] == 199);
        test_assert(sst_age_arr[2] == 188);
        test_assert(sst_age_arr[3] == 99);

    }

    return error;

}

#endif
