/* Swap configuration enums and helpers for config.c */
#ifndef CTRIP_SWAP_CONFIG_H
#define CTRIP_SWAP_CONFIG_H

#ifdef ENABLE_SWAP
#include "server.h"
#include "ctrip_swap.h"
#include "rocksdb/c.h"

configEnum rocksdb_compression_enum[] = {
    {"optimized_for_compaction", -1},
    {"no", rocksdb_no_compression},
    {"snappy", rocksdb_snappy_compression},
    {"zlib", rocksdb_zlib_compression},
    {"lz4", rocksdb_lz4_compression},
    {"zstd", rocksdb_zstd_compression},
    {NULL, 0}};

configEnum cuckoo_filter_bit_type_enum[] = {
    {"8", CUCKOO_FILTER_BITS_PER_TAG_8},
    {"12", CUCKOO_FILTER_BITS_PER_TAG_12},
    {"16", CUCKOO_FILTER_BITS_PER_TAG_16},
    {"32", CUCKOO_FILTER_BITS_PER_TAG_32},
    {NULL, 0}};

configEnum swap_ratelimit_policy_enum[] = {
    {"pause", SWAP_RATELIMIT_POLICY_PAUSE},
    {"reject_oom", SWAP_RATELIMIT_POLICY_REJECT_OOM},
    {"reject_all", SWAP_RATELIMIT_POLICY_REJECT_ALL},
    {"disabled", SWAP_RATELIMIT_POLICY_DISABLED},
    {NULL, 0}};

configEnum swap_info_supported_enum[] = {
    {"yes", SWAP_INFO_SUPPORTED_YES},
    {"no", SWAP_INFO_SUPPORTED_NO},
    {"auto", SWAP_INFO_SUPPORTED_AUTO},
    {NULL, 0}};

configEnum swap_info_propagate_mode_enum[] = {
    {"ping", SWAP_INFO_PROPAGATE_BY_PING},
    {"swap.info", SWAP_INFO_PROPAGATE_BY_SWAP_INFO},
    {NULL, 0}};

static int isValidSwapCuckooFilterEnabled(int val, const char **err) {
    if (!reading_config_file && val) {
        *err = "Couldn't enable cuckoo filter dynamically";
        return 0;
    }
    return 1;
}

static int updateSwapCuckooFilterEnabled(const char **err) {
    UNUSED(err);
    if (!server.swap_cuckoo_filter_enabled) {
        for (int i = 0; i < server.dbnum; i++) {
            redisDb *db = server.db ? server.db[i] : NULL;
            if (!db || db->cold_filter->filter == NULL) continue;
            cuckooFilterFree(db->cold_filter->filter);
            db->cold_filter->filter = NULL;
        }
        serverLog(LL_WARNING, "swap cuckoo filter disabled.");
    }
    return 1;
}

static int updateSwapAbsentCacheEnabled(const char **err) {
    UNUSED(err);
    if (server.swap_absent_cache_enabled) {
        serverLog(LL_WARNING, "absent cache enabled with capacity(%llu).", server.swap_absent_cache_capacity);
        for (int i = 0; i < server.dbnum; i++) {
            redisDb *db = server.db ? server.db[i] : NULL;
            if (!db) continue;
            if (db->cold_filter->absents == NULL)
                db->cold_filter->absents = absentCacheNew(server.swap_absent_cache_capacity);
        }
    } else {
        serverLog(LL_WARNING, "absent cache disabled.");
        for (int i = 0; i < server.dbnum; i++) {
            redisDb *db = server.db ? server.db[i] : NULL;
            if (!db) continue;
            if (db->cold_filter->absents != NULL) {
                absentCacheFree(db->cold_filter->absents);
                db->cold_filter->absents = NULL;
            }
        }
    }
    return 1;
}

static int updateSwapAbsentCacheCapacity(const char **err) {
    UNUSED(err);
    if (server.swap_absent_cache_capacity) {
        for (int i = 0; i < server.dbnum; i++) {
            redisDb *db = server.db ? server.db[i] : NULL;
            if (db && db->cold_filter->absents)
                absentCacheSetCapacity(db->cold_filter->absents, server.swap_absent_cache_capacity);
        }
    }
    return 1;
}

static int updateRocksdbCFOptionNumber(int cf, char *key, long long val, const char **err) {
    sds val_sds = sdsfromlonglong(val);
    int res = updateRocksdbCFOption(cf, key, val_sds, err);
    sdsfree(val_sds);
    return res;
}

static int updateRocksdbCFOptionPercent(int cf, char *key, int val, const char **err) {
    char val_str[16];
    snprintf(val_str, sizeof(val_str), "%.2f", val / 100.0);
    return updateRocksdbCFOption(cf, key, val_str, err);
}

static const char *rocksdbCompressionTypeName(int val) {
    if (val == rocksdb_no_compression) return "kNoCompression";
    if (val == rocksdb_snappy_compression) return "kSnappyCompression";
    if (val == rocksdb_zlib_compression) return "kZlibCompression";
    if (val == rocksdb_lz4_compression) return "kLZ4Compression";
    if (val == rocksdb_zstd_compression) return "kZSTD";
    return "unknown";
}

static const char *rocksdbBlobCompressionTypeName(int val) {
    if (val < 0) val = rocksdb_snappy_compression;
    return rocksdbCompressionTypeName(val);
}

static int updateRocksdbDataCompactPeriod(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "periodic_compaction_seconds", server.rocksdb_data_periodic_compaction_seconds, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "periodic_compaction_seconds", server.rocksdb_data_periodic_compaction_seconds, err);
}

static int updateRocksdbMetaCompactPeriod(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "periodic_compaction_seconds", server.rocksdb_meta_periodic_compaction_seconds, err);
}

static int updateRocksdbDataMinBlobSize(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "min_blob_size", server.rocksdb_data_min_blob_size, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "min_blob_size", server.rocksdb_data_min_blob_size, err);
}

static int updateRocksdbMetaMinBlobSize(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "min_blob_size", server.rocksdb_meta_min_blob_size, err);
}

static int updateRocksdbDataBlobFileSize(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "blob_file_size", server.rocksdb_data_blob_file_size, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "blob_file_size", server.rocksdb_data_blob_file_size, err);
}

static int updateRocksdbMetaBlobFileSize(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "blob_file_size", server.rocksdb_meta_blob_file_size, err);
}

static int updateRocksdbDataDisableAutoCompactions(const char **err) {
    return updateRocksdbCFOptionBoolean(DATA_CF, "disable_auto_compactions", server.rocksdb_data_disable_auto_compactions, err) &&
           updateRocksdbCFOptionBoolean(SCORE_CF, "disable_auto_compactions", server.rocksdb_data_disable_auto_compactions, err);
}

static int updateRocksdbMetaDisableAutoCompactions(const char **err) {
    return updateRocksdbCFOptionBoolean(META_CF, "disable_auto_compactions", server.rocksdb_meta_disable_auto_compactions, err);
}

static int updateRocksdbDataEnableBlobFiles(const char **err) {
    return updateRocksdbCFOptionBoolean(DATA_CF, "enable_blob_files", server.rocksdb_data_enable_blob_files, err) &&
           updateRocksdbCFOptionBoolean(SCORE_CF, "enable_blob_files", server.rocksdb_data_enable_blob_files, err);
}

static int updateRocksdbMetaEnableBlobFiles(const char **err) {
    return updateRocksdbCFOptionBoolean(META_CF, "enable_blob_files", server.rocksdb_meta_enable_blob_files, err);
}

static int updateRocksdbDataEnableBlobGarbageCollection(const char **err) {
    return updateRocksdbCFOptionBoolean(DATA_CF, "enable_blob_garbage_collection", server.rocksdb_data_enable_blob_garbage_collection, err) &&
           updateRocksdbCFOptionBoolean(SCORE_CF, "enable_blob_garbage_collection", server.rocksdb_data_enable_blob_garbage_collection, err);
}

static int updateRocksdbMetaEnableBlobGarbageCollection(const char **err) {
    return updateRocksdbCFOptionBoolean(META_CF, "enable_blob_garbage_collection", server.rocksdb_meta_enable_blob_garbage_collection, err);
}

static int updateRocksdbDataBlobGarbageCollectionAgeCutoffPercentage(const char **err) {
    return updateRocksdbCFOptionPercent(DATA_CF, "blob_garbage_collection_age_cutoff", server.rocksdb_data_blob_garbage_collection_age_cutoff_percentage, err) &&
           updateRocksdbCFOptionPercent(SCORE_CF, "blob_garbage_collection_age_cutoff", server.rocksdb_data_blob_garbage_collection_age_cutoff_percentage, err);
}

static int updateRocksdbMetaBlobGarbageCollectionAgeCutoffPercentage(const char **err) {
    return updateRocksdbCFOptionPercent(META_CF, "blob_garbage_collection_age_cutoff", server.rocksdb_meta_blob_garbage_collection_age_cutoff_percentage, err);
}

static int updateRocksdbDataBlobGarbageCollectionForceThresholdPercentage(const char **err) {
    return updateRocksdbCFOptionPercent(DATA_CF, "blob_garbage_collection_force_threshold", server.rocksdb_data_blob_garbage_collection_force_threshold_percentage, err) &&
           updateRocksdbCFOptionPercent(SCORE_CF, "blob_garbage_collection_force_threshold", server.rocksdb_data_blob_garbage_collection_force_threshold_percentage, err);
}

static int updateRocksdbMetaBlobGarbageCollectionForceThresholdPercentage(const char **err) {
    return updateRocksdbCFOptionPercent(META_CF, "blob_garbage_collection_force_threshold", server.rocksdb_meta_blob_garbage_collection_force_threshold_percentage, err);
}

static int updateRocksdbDataLevel0FileNumCompactionTrigger(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "level0_file_num_compaction_trigger", server.rocksdb_data_level0_file_num_compaction_trigger, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "level0_file_num_compaction_trigger", server.rocksdb_data_level0_file_num_compaction_trigger, err);
}

static int updateRocksdbMetaLevel0FileNumCompactionTrigger(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "level0_file_num_compaction_trigger", server.rocksdb_meta_level0_file_num_compaction_trigger, err);
}

static int updateRocksdbCFCompression(int cf, int val, const char **err) {
    if (val < 0) {
        return updateRocksdbCFOption(cf, "compression_per_level",
                                     "kNoCompression:kNoCompression:kSnappyCompression:kSnappyCompression:kSnappyCompression:kSnappyCompression:kSnappyCompression",
                                     err);
    }
    return updateRocksdbCFOption(cf, "compression", (char *)rocksdbCompressionTypeName(val), err);
}

static int updateRocksdbDataCompression(const char **err) {
    return updateRocksdbCFCompression(DATA_CF, server.rocksdb_data_compression, err) &&
           updateRocksdbCFCompression(SCORE_CF, server.rocksdb_data_compression, err);
}

static int updateRocksdbMetaCompression(const char **err) {
    return updateRocksdbCFCompression(META_CF, server.rocksdb_meta_compression, err);
}

static int updateRocksdbDataBlobCompression(const char **err) {
    char *val_str = (char *)rocksdbBlobCompressionTypeName(server.rocksdb_data_blob_compression);
    return updateRocksdbCFOption(DATA_CF, "blob_compression_type", val_str, err) &&
           updateRocksdbCFOption(SCORE_CF, "blob_compression_type", val_str, err);
}

static int updateRocksdbMetaBlobCompression(const char **err) {
    char *val_str = (char *)rocksdbBlobCompressionTypeName(server.rocksdb_meta_blob_compression);
    return updateRocksdbCFOption(META_CF, "blob_compression_type", val_str, err);
}

static int updateRocksdbDataEnableBlobFileSetRecord(const char **err) {
    return updateRocksdbCFOptionBoolean(DATA_CF, "enable_blob_file_set_record", server.rocksdb_data_enable_blob_file_set_record, err) &&
           updateRocksdbCFOptionBoolean(SCORE_CF, "enable_blob_file_set_record", server.rocksdb_data_enable_blob_file_set_record, err);
}

static int updateRocksdbMetaEnableBlobFileSetRecord(const char **err) {
    return updateRocksdbCFOptionBoolean(META_CF, "enable_blob_file_set_record", server.rocksdb_meta_enable_blob_file_set_record, err);
}

static int isValidRocksdbDataEnableBlobListGarbageCollection(int val, const char **err) {
    if (val && !server.rocksdb_data_enable_blob_file_set_record) {
        *err = "rocksdb.data.enable_blob_file_set_record must be enabled first";
        return 0;
    }
    return 1;
}

static int isValidRocksdbMetaEnableBlobListGarbageCollection(int val, const char **err) {
    if (val && !server.rocksdb_meta_enable_blob_file_set_record) {
        *err = "rocksdb.meta.enable_blob_file_set_record must be enabled first";
        return 0;
    }
    return 1;
}

static int isValidRocksdbDataEnableBlobFileSetRecord(int val, const char **err) {
    if (!val && server.rocksdb_data_enable_blob_list_garbage_collection) {
        *err = "rocksdb.data.enable_blob_list_garbage_collection must be disabled first";
        return 0;
    }
    return 1;
}

static int isValidRocksdbMetaEnableBlobFileSetRecord(int val, const char **err) {
    if (!val && server.rocksdb_meta_enable_blob_list_garbage_collection) {
        *err = "rocksdb.meta.enable_blob_list_garbage_collection must be disabled first";
        return 0;
    }
    return 1;
}

static int updateRocksdbDataEnableBlobListGarbageCollection(const char **err) {
    if (!server.rocksdb_data_enable_blob_list_garbage_collection) {
        return swapPushBlobListGcOption(DATA_CF, 0, err) &&
               swapPushBlobListGcOption(SCORE_CF, 0, err);
    }
    swapBlobListSetPending(DATA_CF, 1);
    swapBlobListSetPending(SCORE_CF, 1);
    return 1;
}

static int updateRocksdbMetaEnableBlobListGarbageCollection(const char **err) {
    if (!server.rocksdb_meta_enable_blob_list_garbage_collection) {
        return swapPushBlobListGcOption(META_CF, 0, err);
    }
    swapBlobListSetPending(META_CF, 1);
    return 1;
}

static int updateRocksdbDataBlobListGarbageOverallGarbageRatioLow(const char **err) {
    return updateRocksdbCFOptionPercent(DATA_CF, "blob_list_garbage_overall_garbage_ratio_low", server.rocksdb_data_blob_list_garbage_overall_garbage_ratio_low, err) &&
           updateRocksdbCFOptionPercent(SCORE_CF, "blob_list_garbage_overall_garbage_ratio_low", server.rocksdb_data_blob_list_garbage_overall_garbage_ratio_low, err);
}

static int updateRocksdbMetaBlobListGarbageOverallGarbageRatioLow(const char **err) {
    return updateRocksdbCFOptionPercent(META_CF, "blob_list_garbage_overall_garbage_ratio_low", server.rocksdb_meta_blob_list_garbage_overall_garbage_ratio_low, err);
}

static int updateRocksdbDataBlobListGarbageOverallGarbageRatioMiddle(const char **err) {
    return updateRocksdbCFOptionPercent(DATA_CF, "blob_list_garbage_overall_garbage_ratio_middle", server.rocksdb_data_blob_list_garbage_overall_garbage_ratio_middle, err) &&
           updateRocksdbCFOptionPercent(SCORE_CF, "blob_list_garbage_overall_garbage_ratio_middle", server.rocksdb_data_blob_list_garbage_overall_garbage_ratio_middle, err);
}

static int updateRocksdbMetaBlobListGarbageOverallGarbageRatioMiddle(const char **err) {
    return updateRocksdbCFOptionPercent(META_CF, "blob_list_garbage_overall_garbage_ratio_middle", server.rocksdb_meta_blob_list_garbage_overall_garbage_ratio_middle, err);
}

static int updateRocksdbDataBlobListGarbageOverallGcGarbageRatioHigh(const char **err) {
    return updateRocksdbCFOptionPercent(DATA_CF, "blob_list_garbage_overall_gc_garbage_ratio_high", server.rocksdb_data_blob_list_garbage_overall_gc_garbage_ratio_high, err) &&
           updateRocksdbCFOptionPercent(SCORE_CF, "blob_list_garbage_overall_gc_garbage_ratio_high", server.rocksdb_data_blob_list_garbage_overall_gc_garbage_ratio_high, err);
}

static int updateRocksdbMetaBlobListGarbageOverallGcGarbageRatioHigh(const char **err) {
    return updateRocksdbCFOptionPercent(META_CF, "blob_list_garbage_overall_gc_garbage_ratio_high", server.rocksdb_meta_blob_list_garbage_overall_gc_garbage_ratio_high, err);
}

static int updateRocksdbDataBlobListGarbageGcGarbageRatio(const char **err) {
    return updateRocksdbCFOptionPercent(DATA_CF, "blob_list_garbage_gc_garbage_ratio", server.rocksdb_data_blob_list_garbage_gc_garbage_ratio, err) &&
           updateRocksdbCFOptionPercent(SCORE_CF, "blob_list_garbage_gc_garbage_ratio", server.rocksdb_data_blob_list_garbage_gc_garbage_ratio, err);
}

static int updateRocksdbMetaBlobListGarbageGcGarbageRatio(const char **err) {
    return updateRocksdbCFOptionPercent(META_CF, "blob_list_garbage_gc_garbage_ratio", server.rocksdb_meta_blob_list_garbage_gc_garbage_ratio, err);
}

static int updateRocksdbDataBlobListGarbageHardGcGarbageRatio(const char **err) {
    return updateRocksdbCFOptionPercent(DATA_CF, "blob_list_garbage_hard_gc_garbage_ratio", server.rocksdb_data_blob_list_garbage_hard_gc_garbage_ratio, err) &&
           updateRocksdbCFOptionPercent(SCORE_CF, "blob_list_garbage_hard_gc_garbage_ratio", server.rocksdb_data_blob_list_garbage_hard_gc_garbage_ratio, err);
}

static int updateRocksdbMetaBlobListGarbageHardGcGarbageRatio(const char **err) {
    return updateRocksdbCFOptionPercent(META_CF, "blob_list_garbage_hard_gc_garbage_ratio", server.rocksdb_meta_blob_list_garbage_hard_gc_garbage_ratio, err);
}

static int updateRocksdbDataBlobListGarbageMaxBlobCandidatePerRound(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "blob_list_garbage_max_blob_candidate_per_round", server.rocksdb_data_blob_list_garbage_max_blob_candidate_per_round, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "blob_list_garbage_max_blob_candidate_per_round", server.rocksdb_data_blob_list_garbage_max_blob_candidate_per_round, err);
}

static int updateRocksdbMetaBlobListGarbageMaxBlobCandidatePerRound(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "blob_list_garbage_max_blob_candidate_per_round", server.rocksdb_meta_blob_list_garbage_max_blob_candidate_per_round, err);
}

static int updateRocksdbDataBlobListGarbageMaxBlobPerCompaction(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "blob_list_garbage_max_blob_per_compaction", server.rocksdb_data_blob_list_garbage_max_blob_per_compaction, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "blob_list_garbage_max_blob_per_compaction", server.rocksdb_data_blob_list_garbage_max_blob_per_compaction, err);
}

static int updateRocksdbMetaBlobListGarbageMaxBlobPerCompaction(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "blob_list_garbage_max_blob_per_compaction", server.rocksdb_meta_blob_list_garbage_max_blob_per_compaction, err);
}

static int updateRocksdbDataBlobListGarbageMaxSstCandidatePerRound(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "blob_list_garbage_max_sst_candidate_per_round", server.rocksdb_data_blob_list_garbage_max_sst_candidate_per_round, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "blob_list_garbage_max_sst_candidate_per_round", server.rocksdb_data_blob_list_garbage_max_sst_candidate_per_round, err);
}

static int updateRocksdbMetaBlobListGarbageMaxSstCandidatePerRound(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "blob_list_garbage_max_sst_candidate_per_round", server.rocksdb_meta_blob_list_garbage_max_sst_candidate_per_round, err);
}

static int updateRocksdbDataBlobListGarbageSstRewriteGarbageBytesRatioThreshold(const char **err) {
    return updateRocksdbCFOptionPercent(DATA_CF, "blob_list_garbage_sst_rewrite_garbage_bytes_ratio_threshold", server.rocksdb_data_blob_list_garbage_sst_rewrite_garbage_bytes_ratio_threshold, err) &&
           updateRocksdbCFOptionPercent(SCORE_CF, "blob_list_garbage_sst_rewrite_garbage_bytes_ratio_threshold", server.rocksdb_data_blob_list_garbage_sst_rewrite_garbage_bytes_ratio_threshold, err);
}

static int updateRocksdbMetaBlobListGarbageSstRewriteGarbageBytesRatioThreshold(const char **err) {
    return updateRocksdbCFOptionPercent(META_CF, "blob_list_garbage_sst_rewrite_garbage_bytes_ratio_threshold", server.rocksdb_meta_blob_list_garbage_sst_rewrite_garbage_bytes_ratio_threshold, err);
}

static int updateRocksdbDataBlobListGarbageHardSstRewriteGarbageBytesRatioThreshold(const char **err) {
    return updateRocksdbCFOptionPercent(DATA_CF, "blob_list_garbage_hard_sst_rewrite_garbage_bytes_ratio_threshold", server.rocksdb_data_blob_list_garbage_hard_sst_rewrite_garbage_bytes_ratio_threshold, err) &&
           updateRocksdbCFOptionPercent(SCORE_CF, "blob_list_garbage_hard_sst_rewrite_garbage_bytes_ratio_threshold", server.rocksdb_data_blob_list_garbage_hard_sst_rewrite_garbage_bytes_ratio_threshold, err);
}

static int updateRocksdbMetaBlobListGarbageHardSstRewriteGarbageBytesRatioThreshold(const char **err) {
    return updateRocksdbCFOptionPercent(META_CF, "blob_list_garbage_hard_sst_rewrite_garbage_bytes_ratio_threshold", server.rocksdb_meta_blob_list_garbage_hard_sst_rewrite_garbage_bytes_ratio_threshold, err);
}

static int updateRocksdbDataMaxWriteBufferNumber(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "max_write_buffer_number", server.rocksdb_data_max_write_buffer_number, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "max_write_buffer_number", server.rocksdb_data_max_write_buffer_number, err);
}

static int updateRocksdbMetaMaxWriteBufferNumber(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "max_write_buffer_number", server.rocksdb_meta_max_write_buffer_number, err);
}

static int updateRocksdbDataLevel0SlowdownWritesTrigger(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "level0_slowdown_writes_trigger", server.rocksdb_data_level0_slowdown_writes_trigger, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "level0_slowdown_writes_trigger", server.rocksdb_data_level0_slowdown_writes_trigger, err);
}

static int updateRocksdbMetaLevel0SlowdownWritesTrigger(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "level0_slowdown_writes_trigger", server.rocksdb_meta_level0_slowdown_writes_trigger, err);
}

static int updateRocksdbDataMaxBytesForLevelMultiplier(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "max_bytes_for_level_multiplier", server.rocksdb_data_max_bytes_for_level_multiplier, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "max_bytes_for_level_multiplier", server.rocksdb_data_max_bytes_for_level_multiplier, err);
}

static int updateRocksdbMetaMaxBytesForLevelMultiplier(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "max_bytes_for_level_multiplier", server.rocksdb_meta_max_bytes_for_level_multiplier, err);
}

static int updateRocksdbDataWriteBufferSize(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "write_buffer_size", server.rocksdb_data_write_buffer_size, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "write_buffer_size", server.rocksdb_data_write_buffer_size, err);
}

static int updateRocksdbMetaWriteBufferSize(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "write_buffer_size", server.rocksdb_meta_write_buffer_size, err);
}

static int updateRocksdbDataTargetFileSizeBase(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "target_file_size_base", server.rocksdb_data_target_file_size_base, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "target_file_size_base", server.rocksdb_data_target_file_size_base, err);
}

static int updateRocksdbMetaTargetFileSizeBase(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "target_file_size_base", server.rocksdb_meta_target_file_size_base, err);
}

static int updateRocksdbDataMaxBytesForLevelBase(const char **err) {
    return updateRocksdbCFOptionNumber(DATA_CF, "max_bytes_for_level_base", server.rocksdb_data_max_bytes_for_level_base, err) &&
           updateRocksdbCFOptionNumber(SCORE_CF, "max_bytes_for_level_base", server.rocksdb_data_max_bytes_for_level_base, err);
}

static int updateRocksdbMetaMaxBytesForLevelBase(const char **err) {
    return updateRocksdbCFOptionNumber(META_CF, "max_bytes_for_level_base", server.rocksdb_meta_max_bytes_for_level_base, err);
}

static int isValidSwapThreadsAutoScaleMin(long long val, const char **err) {
    if (val > server.swap_threads_auto_scale_max) {
        *err = "the new swap-threads-auto-scale-min value is bigger than the current swap-threads-auto-scale-max";
        return 0;
    }
    return 1;
}

struct standardConfig;
struct rewriteConfigState;
int rewriteConfigRewriteLine(struct rewriteConfigState *state, const char *option, sds line, int force);

static int setConfigSwapBatchLimitOption(struct standardConfig *config, sds *argv, int argc, const char **err) {
    UNUSED(config);
    if (argc % 3) {
        if (err) *err = "Wrong number of arguments in swap-batch-limit configuration.";
        return 0;
    }

    for (int j = 0; j < argc; j += 3) {
        int intention = getSwapIntentionByName(argv[j]);
        if (intention <= 0 || intention == SWAP_UTILS) {
            if (err) *err = "Unrecognized or unsupported swap intention name.";
            return 0;
        }
        int mem_err = 0;
        char *endptr = NULL;
        long long count = strtoll(argv[j + 1], &endptr, 10);
        memtoull(argv[j + 2], &mem_err);
        if (count <= 0 || *endptr != '\0' || mem_err) {
            if (err) *err = "Error in count or memory setting in swap-batch-limit.";
            return 0;
        }
    }

    for (int j = 0; j < argc; j += 3) {
        int intention = getSwapIntentionByName(argv[j]);
        int count = atoi(argv[j + 1]);
        unsigned long long mem = memtoull(argv[j + 2], NULL);
        server.swap_batch_limits[intention].count = count;
        server.swap_batch_limits[intention].mem = mem;
    }
    return 1;
}

static sds getConfigSwapBatchLimitOption(struct standardConfig *config) {
    UNUSED(config);
    sds buf = sdsempty();
    for (int j = 1; j < SWAP_UTILS; j++) {
        buf = sdscatprintf(buf, "%s %d %llu",
                           swapIntentionName(j),
                           server.swap_batch_limits[j].count,
                           server.swap_batch_limits[j].mem);
        if (j != SWAP_UTILS - 1)
            buf = sdscatlen(buf, " ", 1);
    }
    return buf;
}

static void rewriteConfigSwapBatchLimitOption(struct standardConfig *config, const char *name, struct rewriteConfigState *state) {
    UNUSED(config);
    sds line = sdsnew(name);
    for (int j = 1; j < SWAP_UTILS; j++) {
        line = sdscatprintf(line, " %s %d %llu",
                            swapIntentionName(j),
                            server.swap_batch_limits[j].count,
                            server.swap_batch_limits[j].mem);
    }
    rewriteConfigRewriteLine(state, name, line, 1);
}

#endif /* ENABLE_SWAP */
#endif /* CTRIP_SWAP_CONFIG_H */
