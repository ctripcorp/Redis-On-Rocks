#ifndef MEMORY_STORAGE_ENGINE_H
#define MEMORY_STORAGE_ENGINE_H

#include "server.h"
#include "ctrip_storage_utils.h"

/* ========== 内存存储引擎 - 跳表实现 ==========
 *
 * 功能：提供与 RocksDB 接口兼容的纯内存 KV 存储引擎。
 * 数据按字典序（字节序）排列，支持 put/get/del/iterate 操作。
 *
 * 数据结构选择：跳表（Skip List）
 * - 天然有序，范围查询 O(log n + k)
 * - put/get/del 均为 O(log n)
 * - 实现简单，无需平衡旋转
 *
 * CF 隔离：每个 CF 独立一个跳表（sl[cf]），rawkey 直接作为跳表 key，
 * 无需 cf 前缀编码，iterate 时天然只扫描目标 CF 的数据。
 */

#define MSE_SKIPLIST_MAXLEVEL 32   /* 跳表最大层数 */
#define MSE_SKIPLIST_P        0.25 /* 跳表概率因子 */

/* 跳表节点 */
typedef struct mseNode {
    sds key;                    /* rawkey（字节序排序） */
    sds val;                    /* 值（rawval） */
    struct mseNode **forward;   /* 前向指针数组，长度为 level */
} mseNode;

/* 跳表 */
typedef struct mseSkipList {
    mseNode *header;    /* 哨兵头节点 */
    int level;          /* 当前最高层数 */
    long length;        /* 节点数量 */
} mseSkipList;

/* 内存存储引擎上下文
 * 每个 CF 独立一个跳表，sl[cf] 存储该 CF 的所有 kv，CF 间完全隔离 */
typedef struct memoryStorageEngineCtx {
    mseSkipList **sl;   /* 跳表数组，长度为 CF_COUNT */
} memoryStorageEngineCtx;

/* ===== 公开接口 ===== */

/* 初始化内存存储引擎，返回 memoryStorageEngine* */
void freeMemoryStorageEngine(void *engine);



/* StorageEngine 回调接口（由 RIO 框架调用）
 * 使用 struct RIO* 前向声明，与 ctrip_storage_types.h 中 StorageEngine 定义保持一致 */
int mseEnginePut(void *context, struct RIO *rio);
int mseEngineGet(void *context, struct RIO *rio);
int mseEngineDel(void *context, struct RIO *rio);
int mseEngineIterate(void *context, struct RIO *rio);

typedef struct memoryRdbSaveCtx {
    long key_count;
    int rdbflags;
    struct rio *rdb;
} memoryRdbSaveCtx;

#ifdef REDIS_TEST
/* 单元测试入口（REDIS_TEST 模式下注册到 ctripStorageTest） */
int memoryStorageEngineTest(int argc, char **argv, int accurate);
#endif

#endif /* MEMORY_STORAGE_ENGINE_H */
