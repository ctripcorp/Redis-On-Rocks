
#ifndef __CTRIP_STORAGE_COMMAND_H__
#define __CTRIP_STORAGE_COMMAND_H__
#include <stdint.h>
/* --- cmd intention flags --- */
/* Delete key in rocksdb when swap in. */
#define SWAP_IN_DEL (1U<<0)
/* Only need to swap meta for hash/set/zset/list/bitmap  */
#define SWAP_IN_META (1U<<1)
/* Delete key in rocksdb and mock value needed to be swapped in. */
#define SWAP_IN_DEL_MOCK_VALUE (1U<<2)
/* Data swap in will be overwritten by fun dbOverwrite
 * same as SWAP_IN_DEL for collection type(SET, ZSET, LISH, HASH...), same as SWAP_IN for STRING */
#define SWAP_IN_OVERWRITE (1U<<3)
/* When swap finished, meta will be deleted(so that key will turn pure hot).*/
#define SWAP_IN_FORCE_HOT (1U<<4)
/* whether to expire Keys with generated in writtable slave is decided
 * before submitExpireClientRequest and should not skip expire even
 * if current role is slave. */
#define SWAP_EXPIRE_FORCE (1U<<5)
/* If oom would happen during RIO, swap will abort. */
#define SWAP_OOM_CHECK (1U<<6)
/* This is a metascan request for scan command. */
#define SWAP_METASCAN_SCAN (1U<<7)
/* This is a metascan request for randomkey command. */
#define SWAP_METASCAN_RANDOMKEY (1U<<8)
/* This is a metascan request for active-expire. */
#define SWAP_METASCAN_EXPIRE (1U<<9)
/* This is a persist requset. */
#define SWAP_OUT_PERSIST (1U<<10)
/* Keep data in memory because memory is sufficient. */
#define SWAP_OUT_KEEP_DATA (1U<<11)

/* 获取key请求的函数指针类型
 * 输入: dbid - 数据库ID, cmd - redis命令, argv - 参数数组, argc - 参数个数
 * 输出: result - key请求结果，返回0表示成功 */
typedef int (*redisGetKeyRequestsProc)(int dbid, struct redisCommand *cmd,
        robj **argv, int argc, struct getKeyRequestsResult *result);

/* swap命令定义，描述每个Redis命令的冷热交换行为 */
typedef struct {
    const char *name;                              /* 命令全名（小写），与 redisCommand.fullname 格式一致
                                                    * 顶级命令: "get", 子命令: "config|get" */
    redisGetKeyRequestsProc getkeyrequests_proc;   /* 获取key请求的函数指针，NULL则走默认逻辑 */
    int intention;                                 /* swap操作类型: SWAP_NOP/IN/OUT/DEL/UTILS */
    uint32_t intention_flags;                      /* swap意图标志位: SWAP_IN_DEL/SWAP_IN_META等 */
    uint64_t cmd_swap_flags;                       /* 命令支持的数据类型标志: CMD_SWAP_DATATYPE_* */
} swapCommand;

/* 通过命令名查找对应的 swapCommand（不区分大小写）
 * 输入: name - 命令名，支持 fullname 格式（如 "get" 或 "config|get"）
 * 输出: 找到返回 swapCommand 指针，找不到返回 NULL
 * 用法: lookupSwapCommand(cmd->fullname) */
swapCommand *lookupSwapCommand(const char *name);

/* swap命令表（由 generate_swap_command_def.py 自动生成） */
extern swapCommand swapCommandTable[];

#endif /* __CTRIP_STORAGE_COMMAND_H__ */
