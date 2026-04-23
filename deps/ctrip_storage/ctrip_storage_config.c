/*
 * ctrip_storage_config.c - 存储引擎配置描述符表
 *
 * 定义所有需要通过 CONFIG SET/GET 管理的 server.storage 字段。
 * 每个配置项对应 StorageServerNamespace 中的一个字段。
 *
 * 新增配置项操作步骤：
 *   Step 1: ctrip_storage_types.h 中给 StorageServerNamespace 添加新字段
 *   Step 2: 在下方 storage_config_descriptors[] 中追加一行描述符
 *   （src/config.c 无需修改）
 */

/* 通过 server.h 间接获得 StorageServerNamespace 完整定义（含 redisAtomic 等依赖类型） */
#include "server.h"
#include "ctrip_storage_config.h"
#include <limits.h>  /* INT_MAX / LLONG_MAX / ULLONG_MAX */

/* 计算 StorageServerNamespace 字段偏移量的便捷宏 */
#define SSN_OFFSET(field) offsetof(StorageServerNamespace, field)

/*
 * storage_config_descriptors - 存储引擎全量配置描述符表
 *
 * 按类型分组，flags 含义：
 *   STORAGE_CONF_MODIFIABLE  运行时可通过 CONFIG SET 修改
 *   STORAGE_CONF_IMMUTABLE   仅启动时生效，运行时只读
 *   STORAGE_CONF_HIDDEN      不在 CONFIG GET * 中显示（调试配置）
 */
static const StorageConfigDesc storage_config_descriptors[] = {

    /* ===== Bool 配置（CONFIG SET yes/no） ===== */

    /* absent cache：是否在内存中缓存不存在的 key，减少无谓的 rocksdb 查询 */
    { "swap-absent-cache-enabled", NULL,
      STORAGE_CONF_BOOL, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_absent_cache_enabled), .def.bval = {0} },

    /* absent cache subkey：absent cache 是否同时缓存 hash/zset 等的 subkey */
    { "swap-absent-cache-include-subkey", NULL,
      STORAGE_CONF_BOOL, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_absent_cache_include_subkey), .def.bval = {0} },

    /* persist：是否开启 swap 持久化（将 swap 操作写入持久化日志） */
    { "swap-persist-enabled", NULL,
      STORAGE_CONF_BOOL, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_persist_enabled), .def.bval = {0} },

    /* dirty subkeys：是否追踪脏子键用于增量持久化 */
    { "swap-dirty-subkeys-enabled", NULL,
      STORAGE_CONF_BOOL, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_dirty_subkeys_enabled), .def.bval = {0} },

    /* cuckoo filter：是否使用 cuckoo filter 过滤不存在的冷 key 查询 */
    { "swap-cuckoo-filter-enabled", NULL,
      STORAGE_CONF_BOOL, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_cuckoo_filter_enabled), .def.bval = {0} },

    /* rdb bitmap encode：RDB 保存时是否对 bitmap 使用特殊编码格式 */
    { "swap-rdb-bitmap-encode-enabled", NULL,
      STORAGE_CONF_BOOL, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_rdb_bitmap_encode_enabled), .def.bval = {0} },

    /* bitmap subkeys：是否将大 bitmap 拆分为 subkeys 存入 rocksdb */
    { "swap-bitmap-subkeys-enabled", NULL,
      STORAGE_CONF_BOOL, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_bitmap_subkeys_enabled), .def.bval = {0} },

    /* ===== Int 配置 ===== */

    /* 线程池自动扩容上限（最大线程数） */
    { "swap-threads-auto-scale-max", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_threads_auto_scale_max), .def.ival = {4, 1, 128} },

    /* 线程池自动缩容下限（最小线程数） */
    { "swap-threads-auto-scale-min", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_threads_auto_scale_min), .def.ival = {1, 1, 128} },

    /* 触发线程池扩容的请求数阈值 */
    { "swap-threads-auto-scale-up-threshold", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_threads_auto_scale_up_threshold), .def.ival = {100, 1, INT_MAX} },

    /* 线程空闲多少秒后触发缩容 */
    { "swap-threads-auto-scale-down-idle-seconds", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_threads_auto_scale_down_idle_seconds), .def.ival = {30, 1, INT_MAX} },

    /* repl worker 客户端数量（用于 slave 同步期间并行 swap） */
    { "swap-repl-workers", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_repl_workers), .def.ival = {1, 1, 64} },

    /* scan session 使用的 bits 数，决定最大并发 scan 会话数（2^bits） */
    { "swap-scan-session-bits", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_scan_session_bits), .def.ival = {4, 0, 16} },

    /* scan session 最大空闲秒数，超时后自动释放 */
    { "swap-scan-session-max-idle-seconds", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_scan_session_max_idle_seconds), .def.ival = {60, 1, INT_MAX} },

    /* rate limit：maxmemory 使用率超过此百分比时开始限速写入 */
    { "swap-ratelimit-maxmemory-percentage", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_ratelimit_maxmemory_percentage), .def.ival = {80, 0, 100} },

    /* rate limit：内存限速时每次 pause 的增长倍率 */
    { "swap-ratelimit-maxmemory-pause-growth-rate", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_ratelimit_maxmemory_pause_growth_rate), .def.ival = {10, 1, INT_MAX} },

    /* rate limit 策略（0=关闭，1=拒绝，2=暂停，3=混合） */
    { "swap-ratelimit-policy", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_ratelimit_policy), .def.ival = {0, 0, 3} },

    /* persist 落后量超过此值（字节）时触发持久化限速 */
    { "swap-ratelimit-persist-lag", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_ratelimit_persist_lag), .def.ival = {0, 0, INT_MAX} },

    /* persist 限速时 pause 增长倍率 */
    { "swap-ratelimit-persist-pause-growth-rate", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_ratelimit_persist_pause_growth_rate), .def.ival = {10, 1, INT_MAX} },

    /* cuckoo filter bit 精度类型（影响内存占用和误判率） */
    { "swap-cuckoo-filter-bit-type", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_cuckoo_filter_bit_type), .def.ival = {0, 0, 3} },

    /* ===== Debug Int 配置（HIDDEN，不在 CONFIG GET * 中显示） ===== */

    /* 模拟 SSD 延迟：每次 RIO 操作后 sleep 的微秒数 */
    { "swap-debug-rio-delay-micro", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE | STORAGE_CONF_HIDDEN,
      SSN_OFFSET(swap_debug_rio_delay_micro), .def.ival = {0, 0, INT_MAX} },

    /* 模拟 swapout 通知延迟：通知完成队列后额外 sleep 的微秒数 */
    { "swap-debug-swapout-notify-delay-micro", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE | STORAGE_CONF_HIDDEN,
      SSN_OFFSET(swap_debug_swapout_notify_delay_micro), .def.ival = {0, 0, INT_MAX} },

    /* 模拟 exec swap 前延迟 */
    { "swap-debug-before-exec-swap-delay-micro", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE | STORAGE_CONF_HIDDEN,
      SSN_OFFSET(swap_debug_before_exec_swap_delay_micro), .def.ival = {0, 0, INT_MAX} },

    /* 模拟 rocksdb 初始化延迟 */
    { "swap-debug-init-rocksdb-delay-micro", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE | STORAGE_CONF_HIDDEN,
      SSN_OFFSET(swap_debug_init_rocksdb_delay_micro), .def.ival = {0, 0, INT_MAX} },

    /* 注入 RIO 错误（非 0 表示触发错误） */
    { "swap-debug-rio-error", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE | STORAGE_CONF_HIDDEN,
      SSN_OFFSET(swap_debug_rio_error), .def.ival = {0, 0, INT_MAX} },

    /* RIO 错误触发时的动作码 */
    { "swap-debug-rio-error-action", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE | STORAGE_CONF_HIDDEN,
      SSN_OFFSET(swap_debug_rio_error_action), .def.ival = {0, 0, INT_MAX} },

    /* 是否开启异步完成队列延迟调试日志（0=关闭，1=开启） */
    { "swap-debug-trace-latency", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE | STORAGE_CONF_HIDDEN,
      SSN_OFFSET(swap_debug_trace_latency), .def.ival = {0, 0, 1} },

    /* bgsave 时 meta 长度额外增加量（用于测试边界） */
    { "swap-debug-bgsave-metalen-addition", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE | STORAGE_CONF_HIDDEN,
      SSN_OFFSET(swap_debug_bgsave_metalen_addition), .def.ival = {0, 0, INT_MAX} },

    /* compaction filter 模拟延迟 */
    { "swap-debug-compaction-filter-delay-micro", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE | STORAGE_CONF_HIDDEN,
      SSN_OFFSET(swap_debug_compaction_filter_delay_micro), .def.ival = {0, 0, INT_MAX} },

    /* RDB key 保存模拟延迟 */
    { "swap-debug-rdb-key-save-delay-micro", NULL,
      STORAGE_CONF_INT, STORAGE_CONF_MODIFIABLE | STORAGE_CONF_HIDDEN,
      SSN_OFFSET(swap_debug_rdb_key_save_delay_micro), .def.ival = {0, 0, INT_MAX} },

    /* ===== ULongLong 配置 ===== */

    /* absent cache 最大容量（字节，超过后 LRU 淘汰） */
    { "swap-absent-cache-capacity", NULL,
      STORAGE_CONF_ULONG_LONG, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_absent_cache_capacity),
      .def.ullval = {1024*1024, 0, ULLONG_MAX} },

    /* maxmemory 缩放起始值（字节，0 表示不缩放） */
    { "swap-maxmemory-scale-from", NULL,
      STORAGE_CONF_ULONG_LONG, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(maxmemory_scale_from),
      .def.ullval = {0, 0, ULLONG_MAX} },

    /* maxmemory 每秒缩减速率（字节，0 表示不限速） */
    { "swap-maxmemory-scaledown-rate", NULL,
      STORAGE_CONF_ULONG_LONG, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(maxmemory_scaledown_rate),
      .def.ullval = {0, 0, ULLONG_MAX} },

    /* cuckoo filter 预估存储的 key 数量（影响初始内存分配） */
    { "swap-cuckoo-filter-estimated-keys", NULL,
      STORAGE_CONF_ULONG_LONG, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_cuckoo_filter_estimated_keys),
      .def.ullval = {1000000, 0, ULLONG_MAX} },

    /* ===== SizeT 配置 ===== */

    /* bitmap subkey 大小（字节，每个 subkey 对应 rocksdb 中的一条记录） */
    { "swap-bitmap-subkey-size", NULL,
      STORAGE_CONF_SIZE_T, STORAGE_CONF_MODIFIABLE,
      SSN_OFFSET(swap_bitmap_subkey_size),
      .def.stval = {4096, 1, (size_t)-1} },
};

/*
 * storageGetConfigDescriptors - 返回全量存储配置描述符数组
 *
 * 输入:  count - 输出参数，写入数组元素个数
 * 输出:  指向静态描述符数组的指针（进程生命周期内有效，调用方不得释放）
 */
const StorageConfigDesc *storageGetConfigDescriptors(int *count) {
    *count = sizeof(storage_config_descriptors) / sizeof(storage_config_descriptors[0]);
    return storage_config_descriptors;
}
