#include "xredis_cmdparse.h"
#include "server.h"

/* ================================================================
 * 辅助函数：sds 转大写（server.commands 的 key 为大写）
 * ================================================================ */
static sds sdsdupupper(sds s) {
    sds result = sdsdup(s);
    for (size_t i = 0; i < sdslen(result); i++) {
        result[i] = toupper((unsigned char)result[i]);
    }
    return result;
}

/* ================================================================
 * 各命令的 count / parse 实现
 * ================================================================ */

/* --- del / unlink：多个 key --- */
static int cmdCountDel(robj **argv, int argc) {
    UNUSED(argv);
    return argc > 1 ? argc - 1 : 0;
}
static void cmdParseDel(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    for (int i = 1; i < argc; i++) {
        on_key(ctx, OBJ_UNKNOWN, i, 0, 0, 0, NULL);
    }
}

/* --- 单个 key，无 subkeys（通过命令名推断类型）--- */
static int cmdCountSingleKey(robj **argv, int argc) {
    UNUSED(argv); UNUSED(argc);
    return 1;
}
static int cmdKeyTypeFromCommand(struct redisCommand *cmd) {
    if (cmd->flags & CMD_CATEGORY_STRING) return OBJ_STRING;
    if (cmd->flags & CMD_CATEGORY_LIST) return OBJ_LIST;
    if (cmd->flags & CMD_CATEGORY_HASH) return OBJ_HASH;
    if (cmd->flags & CMD_CATEGORY_SET) return OBJ_SET;
    if (cmd->flags & CMD_CATEGORY_SORTEDSET) return OBJ_ZSET;
    if (cmd->flags & CMD_CATEGORY_BITMAP) return OBJ_STRING;
    return OBJ_UNKNOWN;
}
static void cmdParseSingleKey(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(argc);
    on_key(ctx, cmdKeyTypeFromCommand(cmd), 1, 0, 0, 0, NULL);
}

/* --- mset / msetnx：多个 key，间隔出现 --- */
static int cmdCountMset(robj **argv, int argc) {
    UNUSED(argv);
    return argc > 1 ? (argc - 1) / 2 : 0;
}
static void cmdParseMset(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(argv);
    for (int i = 1; i < argc; i += 2) {
        on_key(ctx, OBJ_STRING, i, 0, 0, 0, NULL);
    }
}

/* --- hset / hmset：单个 key + subkeys（field）, 步长为2, 从argv[2]开始 --- */
static void cmdParseHset(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = (argc - 2) / 2;
    on_key(ctx, OBJ_HASH, 1, subkeys_count, 2, 2, NULL);
}

/* --- hdel：单个 key + 多个 subkeys, 步长为1, 从argv[2]开始 --- */
static void cmdParseHdel(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = argc - 2;
    on_key(ctx, OBJ_HASH, 1, subkeys_count, 2, 1, NULL);
}

/* --- hsetnx / hincrby / hincrbyfloat：单个 key + 1 个 subkey --- */
static void cmdParseHsetnx(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    if (argc >= 3) {
        on_key(ctx, OBJ_HASH, 1, 1, 2, 1, NULL);
    } else {
        on_key(ctx, OBJ_HASH, 1, 0, 0, 0, NULL);
    }
}

/* --- sadd / srem / spop：单个 key + 多个 subkeys, 步长为1, 从argv[2]开始 --- */
static void cmdParseSadd(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = argc - 2;
    on_key(ctx, OBJ_SET, 1, subkeys_count, 2, 1, NULL);
}

/* --- smove：2 个 key，各带 1 个 subkey（member=argv[3]）--- */
static int cmdCountRename(robj **argv, int argc) {
    UNUSED(argv);
    return argc >= 3 ? 2 : (argc >= 2 ? 1 : 0);
}
static void cmdParseSmove(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(argv);
    if (argc >= 4) {
        on_key(ctx, OBJ_SET, 1, 1, 3, 1, NULL);
        on_key(ctx, OBJ_SET, 2, 1, 3, 1, NULL);
    } else if (argc >= 3) {
        on_key(ctx, OBJ_SET, 1, 0, 0, 0, NULL);
        on_key(ctx, OBJ_SET, 2, 0, 0, 0, NULL);
    } else if (argc >= 2) {
        on_key(ctx, OBJ_SET, 1, 0, 0, 0, NULL);
    }
}

/* --- zadd：单个 key + member subkeys, 步长为2 --- */
static void cmdParseZadd(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int i = 2;
    while (i < argc) {
        sds arg = (sds)argv[i]->ptr;
        if (!strcasecmp(arg, "nx") || !strcasecmp(arg, "xx") ||
            !strcasecmp(arg, "ch") || !strcasecmp(arg, "incr") ||
            !strcasecmp(arg, "gt") || !strcasecmp(arg, "lt")) {
            i++;
        } else {
            break;
        }
    }
    int subkeys_count = (argc - i) / 2;
    on_key(ctx, OBJ_ZSET, 1, subkeys_count, i + 1, 2, NULL);
}

/* --- zrem：单个 key + 多个 subkeys, 步长为1, 从argv[2]开始 --- */
static void cmdParseZrem(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = argc - 2;
    on_key(ctx, OBJ_ZSET, 1, subkeys_count, 2, 1, NULL);
}

/* --- zincrby：单个 key + 1 个 subkey --- */
static void cmdParseZincrby(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    if (argc >= 4) {
        on_key(ctx, OBJ_ZSET, 1, 1, 3, 1, NULL);
    } else {
        on_key(ctx, OBJ_ZSET, 1, 0, 0, 0, NULL);
    }
}

/* --- rename / renamenx：2 个 key --- */
static void cmdParseRename(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    if (argc >= 3) {
        on_key(ctx, OBJ_UNKNOWN, 1, 0, 0, 0, NULL);
        on_key(ctx, OBJ_UNKNOWN, 2, 0, 0, 0, NULL);
    } else if (argc >= 2) {
        on_key(ctx, OBJ_UNKNOWN, 1, 0, 0, 0, NULL);
    }
}

/* --- bitop：argv[1]=操作符, argv[2+]=key（参考 server 表 firstkey=2）--- */
static int cmdCountBitOp(robj **argv, int argc) {
    UNUSED(argv);
    return argc > 2 ? argc - 2 : 0;
}
static void cmdParseBitOp(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(argv);
    for (int i = 2; i < argc; i++) {
        on_key(ctx, OBJ_STRING, i, 0, 0, 0, NULL);
    }
}

/* --- zunionstore / zinterstore / zdiffstore：dest + numkeys 个 key --- */
static int cmdCountZstore(robj **argv, int argc) {
    UNUSED(argv);
    if (argc < 4) return argc >= 2 ? 1 : 0;
    long long numkeys = 0;
    if (getLongLongFromObject(argv[2], &numkeys) != C_OK || numkeys < 1) {
        return 1; /* dest only */
    }
    return 1 + (int)numkeys;
}
static void cmdParseZstore(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid); UNUSED(argv);
    if (argc < 2) return;
    on_key(ctx, OBJ_ZSET, 1, 0, 0, 0, NULL); /* dest */
    if (argc < 4) return;
    long long numkeys = 0;
    if (getLongLongFromObject(argv[2], &numkeys) != C_OK || numkeys < 1) return;
    int max_keys = argc - 3;
    if (numkeys > max_keys) numkeys = max_keys;
    for (long long i = 0; i < numkeys; i++) {
        on_key(ctx, OBJ_ZSET, 3 + (int)i, 0, 0, 0, NULL);
    }
}

/* ================================================================
 * 引入由 generate_cmdparse_commands.py 自动生成的命令注册表
 * ================================================================ */
#include "xredis_commands.def"

/* ================================================================
 * 公共接口：通过 server.commands 查找
 * ================================================================ */

/* 计算命令中包含的 key 条目数（通过 lookupCommand 从 server.commands 取） */
int cmdParseCountKeys(robj **argv, int argc) {
    if (argc < 1) return 0;
    sds cmd_upper = sdsdupupper((sds)argv[0]->ptr);
    struct redisCommand *cmd = lookupCommand(cmd_upper);
    sdsfree(cmd_upper);
    if (cmd != NULL && cmd->cmdparse_count != NULL) {
        return cmd->cmdparse_count(argv, argc);
    }
    /* unknown command fallback */
    return argc >= 2 ? 1 : 0;
}

/* 解析命令，通过回调通知每个 key 的位置描述（通过 lookupCommand 从 server.commands 取） */
void cmdParseKeys(int dbid, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    if (argc < 1) return;
    sds cmd_upper = sdsdupupper((sds)argv[0]->ptr);
    struct redisCommand *cmd = lookupCommand(cmd_upper);
    sdsfree(cmd_upper);
    if (cmd != NULL && cmd->cmdparse_parse != NULL) {
        cmd->cmdparse_parse(dbid, cmd, argv, argc, ctx, on_key);
    } else if (argc >= 2) {
        /* unknown command fallback */
        serverLog(LL_WARNING, "Unknown command '%s' for key propagation", (sds)argv[0]->ptr);
        // on_key(ctx, OBJ_UNKNOWN, 1, 0, 0, 0, NULL);
        serverPanic("unknown command fallback");
    }
}

/* 绑定 cmdparse 函数到 server.commands 中的所有命令 */
void cmdParseBindToCommands(void) {
    int i;
    for (i = 0; cmd_parse_commands[i].name != NULL; i++) {
        sds name = sdsnew(cmd_parse_commands[i].name);
        for (size_t j = 0; j < sdslen(name); j++) {
            name[j] = toupper((unsigned char)name[j]);
        }
        struct redisCommand *cmd = lookupCommand(name);
        sdsfree(name);
        if (cmd != NULL) {
            cmd->cmdparse_count = cmd_parse_commands[i].count;
            cmd->cmdparse_parse = cmd_parse_commands[i].parse;
        }
    }
}

/* 通过命令名查找 count 函数（从 server.commands 取） */
int (*cmdParseGetCountFunc(const char *cmd_name))(robj **argv, int argc) {
    if (cmd_name == NULL) return NULL;
    struct redisCommand *cmd = lookupCommandByCString(cmd_name);
    if (cmd != NULL) {
        return cmd->cmdparse_count;
    }
    return NULL;
}

/* 通过命令名查找 parse 函数（从 server.commands 取） */
void (*cmdParseGetParseFunc(const char *cmd_name))(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    if (cmd_name == NULL) return NULL;
    struct redisCommand *cmd = lookupCommandByCString(cmd_name);
    if (cmd != NULL) {
        return cmd->cmdparse_parse;
    }
    return NULL;
}
