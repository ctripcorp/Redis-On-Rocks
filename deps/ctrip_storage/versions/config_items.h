/*
 * config_items.h - 存储引擎配置项列表（X-macro，版本无关）
 *
 * 通过 X-macro 模式分离"配置项元数据"与"版本相关的宏展开"：
 *   - 此文件只写配置项本身，不含任何版本相关宏
 *   - 版本文件在 #include 此文件前定义好展开宏
 *
 * 宏格式（刻意精简，只保留真正有差异的参数）：
 *
 *   SCFG_BOOL(name, field, default, apply)
 *   SCFG_BOOL_H(name, field, default, apply)        ← HIDDEN（debug 用）
 *
 *   SCFG_INT(name, field, lower, upper, default, apply)
 *   SCFG_INT_H(name, field, lower, upper, default, apply)  ← HIDDEN
 *
 *   SCFG_MEM(name, field, lower, upper, default, apply)    ← ULongLong，MEMORY 单位（mb/kb）
 *
 *   SCFG_ULL(name, field, lower, upper, default, apply)    ← ULongLong，纯整数
 *
 *   SCFG_SIZET(name, field, lower, upper, default, apply)
 *
 * 省略的参数（版本宏内统一处理）：
 *   alias      → NULL（storage 配置无别名）
 *   flags      → MODIFIABLE_CONFIG（_H 变体加 HIDDEN_CONFIG）
 *   mem_flag   → SCFG_MEM 用 MEMORY_CONFIG，其余用 INTEGER_CONFIG
 *   is_valid   → NULL（依赖 apply 做校验）
 *   field 路径 → 版本宏内统一加 server.storage. 前缀
 *
 * 新增配置项：在此文件末尾追加一行，无需修改版本文件。
 */

/* ===== Bool 配置 ===== */
SCFG_BOOL("swap-absent-cache-enabled",          swap_absent_cache_enabled,          0, NULL)
SCFG_BOOL("swap-absent-cache-include-subkey",   swap_absent_cache_include_subkey,   0, NULL)
SCFG_BOOL("swap-persist-enabled",               swap_persist_enabled,               0, NULL)
SCFG_BOOL("swap-dirty-subkeys-enabled",         swap_dirty_subkeys_enabled,         0, NULL)
SCFG_BOOL("swap-cuckoo-filter-enabled",         swap_cuckoo_filter_enabled,         0, NULL)
SCFG_BOOL("swap-rdb-bitmap-encode-enabled",     swap_rdb_bitmap_encode_enabled,     0, NULL)
SCFG_BOOL("swap-bitmap-subkeys-enabled",        swap_bitmap_subkeys_enabled,        0, NULL)

/* ===== Int 配置 ===== */
SCFG_INT("swap-threads-auto-scale-max",              swap_threads_auto_scale_max,              1, 128,     4,   NULL)
SCFG_INT("swap-threads-auto-scale-min",              swap_threads_auto_scale_min,              1, 128,     1,   updateSwapThreadsAutoScaleMin)
SCFG_INT("swap-threads-auto-scale-up-threshold",     swap_threads_auto_scale_up_threshold,     1, INT_MAX, 100, NULL)
SCFG_INT("swap-threads-auto-scale-down-idle-seconds",swap_threads_auto_scale_down_idle_seconds,1, INT_MAX, 30,  NULL)
SCFG_INT("swap-repl-workers",                        swap_repl_workers,                        1, 64,      1,   NULL)
SCFG_INT("swap-scan-session-bits",                   swap_scan_session_bits,                   0, 16,      4,   NULL)
SCFG_INT("swap-scan-session-max-idle-seconds",       swap_scan_session_max_idle_seconds,       1, INT_MAX, 60,  NULL)
SCFG_INT("swap-ratelimit-maxmemory-percentage",      swap_ratelimit_maxmemory_percentage,      0, 100,     80,  NULL)
SCFG_INT("swap-ratelimit-maxmemory-pause-growth-rate",swap_ratelimit_maxmemory_pause_growth_rate,1,INT_MAX,10, NULL)
SCFG_INT("swap-ratelimit-policy",                    swap_ratelimit_policy,                    0, 3,       0,   NULL)
SCFG_INT("swap-ratelimit-persist-lag",               swap_ratelimit_persist_lag,               0, INT_MAX, 0,   NULL)
SCFG_INT("swap-ratelimit-persist-pause-growth-rate", swap_ratelimit_persist_pause_growth_rate, 1, INT_MAX, 10,  NULL)
SCFG_INT("swap-cuckoo-filter-bit-type",              swap_cuckoo_filter_bit_type,              0, 3,       0,   NULL)
SCFG_INT("swap-slow-expire-effort",                  swap_slow_expire_effort,                  -10, 10,     -5,   NULL)
/* ===== Debug Int 配置（HIDDEN） ===== */
SCFG_INT_H("swap-debug-rio-delay-micro",                  swap_debug_rio_delay_micro,                  0, INT_MAX, 0, NULL)
SCFG_INT_H("swap-debug-swapout-notify-delay-micro",       swap_debug_swapout_notify_delay_micro,       0, INT_MAX, 0, NULL)
SCFG_INT_H("swap-debug-before-exec-swap-delay-micro",     swap_debug_before_exec_swap_delay_micro,     0, INT_MAX, 0, NULL)
SCFG_INT_H("swap-debug-init-rocksdb-delay-micro",         swap_debug_init_rocksdb_delay_micro,         0, INT_MAX, 0, NULL)
SCFG_INT_H("swap-debug-rio-error",                        swap_debug_rio_error,                        0, INT_MAX, 0, NULL)
SCFG_INT_H("swap-debug-rio-error-action",                 swap_debug_rio_error_action,                 0, INT_MAX, 0, NULL)
SCFG_INT_H("swap-debug-trace-latency",                    swap_debug_trace_latency,                    0, 1,       0, NULL)
SCFG_INT_H("swap-debug-bgsave-metalen-addition",          swap_debug_bgsave_metalen_addition,          0, INT_MAX, 0, NULL)
SCFG_INT_H("swap-debug-compaction-filter-delay-micro",    swap_debug_compaction_filter_delay_micro,    0, INT_MAX, 0, NULL)
SCFG_INT_H("swap-debug-rdb-key-save-delay-micro",         swap_debug_rdb_key_save_delay_micro,         0, INT_MAX, 0, NULL)

/* ===== ULongLong（内存单位，mb/kb 前缀） ===== */
SCFG_MEM("swap-absent-cache-capacity",          swap_absent_cache_capacity,          0, ULLONG_MAX, 1024*1024, NULL)
SCFG_MEM("swap-maxmemory-scale-from",           maxmemory_scale_from,                0, ULLONG_MAX, 0,         NULL)
SCFG_MEM("swap-maxmemory-scaledown-rate",       maxmemory_scaledown_rate,            0, ULLONG_MAX, 0,         NULL)

/* ===== ULongLong（纯整数） ===== */
SCFG_ULL("swap-cuckoo-filter-estimated-keys",   swap_cuckoo_filter_estimated_keys,   0, ULLONG_MAX, 1000000,   NULL)

/* ===== SizeT ===== */
SCFG_SIZET("swap-bitmap-subkey-size",           swap_bitmap_subkey_size,             1, (size_t)-1, 4096,      NULL)


/* ===== Enum 配置 ===== */
SCFG_ENUM("storage-type",                    storage_types,      type,                         STORAGE_TYPE_NONE , NULL)