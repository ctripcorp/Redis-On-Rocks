

#ifndef __CTRIP_STORAGE_HELP_H__
#define __CTRIP_STORAGE_HELP_H__ 
#define IN        /* Input parameter */
#define OUT       /* Output parameter */
#define INOUT     /* Input/Output parameter */
#define MOVE      /* Moved ownership */

/* next of OBJ_STREAM */
#define OBJ_BITMAP  OBJ_TYPE_MAX

#if defined(OBJ_TYPE_MAX) && (OBJ_TYPE_MAX != 7)
#error OBJ_TYPE_MAX is not equal to 7
#endif
#undef OBJ_TYPE_MAX
#define OBJ_TYPE_MAX 8

#define SWAP_TYPE_STRING    OBJ_STRING
#define SWAP_TYPE_LIST      OBJ_LIST
#define SWAP_TYPE_SET       OBJ_SET
#define SWAP_TYPE_ZSET      OBJ_ZSET
#define SWAP_TYPE_HASH      OBJ_HASH
#define SWAP_TYPE_STREAM    OBJ_STREAM
#define SWAP_TYPE_BITMAP    OBJ_BITMAP

typedef unsigned int keylen_t;

/* Encode version in BE order, so that numeric order matches alphabatic. */
#define rocksEncodeVersion(version) htonu64(version)
#define rocksDecodeVersion(version) ntohu64(version)

/* swapRequestBatch 接口定义 */
#define SWAP_BATCH_DEFAULT_SIZE 16
#define SWAP_BATCH_LINEAR_SIZE  4096

/* engine task type */
#define ROCKSDB_COMPACT_RANGE_TASK 0
#define ROCKSDB_GET_STATS_TASK 1
#define ROCKSDB_FLUSH_TASK 2
#define ROCKSDB_EXCLUSIVE_TASK_COUNT 3
#define ROCKSDB_CREATE_CHECKPOINT 3
#define ROCKSDB_COLLECT_CF_META_TASK 4
#endif