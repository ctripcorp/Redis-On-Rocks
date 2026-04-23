/*
 * config_6_x.c - Redis 6.x 存储引擎配置注册（X-macro 版）
 *
 * 通过 #include 嵌入 6.x src/config.c 编译单元。
 * 嵌入位置：configs[] 数组体内、原生 {NULL} 终结符之前。
 *
 *   standardConfig configs[] = {
 *       createBoolConfig("rdbchecksum", ...),
 *       ...
 *       #include "../deps/ctrip_storage/versions/config_6_x.c"
 *       {NULL}
 *   };
 *
 * 6.x 注册机制：configs[] 是静态全局数组，此文件展开为数组初始化元素。
 *
 * 6.x 与 8.x 的差异处理：
 *   1. apply 签名不兼容
 *      8.x: int apply(const char **err)           无参，CONFIG SET 后触发
 *      6.x: int update_fn(val, prev, const char **err)  携带新旧值，放在 data 内
 *      处理: update 位置传 NULL（交叉校验的 min/max 约束在写入前通过 is_valid 处理，
 *            或接受 6.x 无此校验的限制）
 *
 *   2. HIDDEN_CONFIG 在 6.x 不存在
 *      处理: 定义为 0，使 MODIFIABLE_CONFIG|HIDDEN_CONFIG == MODIFIABLE_CONFIG
 *            debug 配置功能正常，但会出现在 CONFIG GET * 中（可接受）
 *
 *   3. 无 storageStaticConfigs[] 独立数组
 *      原因: 6.x configGetCommand/configSetCommand/initConfigValues 均只遍历 configs[]，
 *            必须物理并入该数组，不能像 8.x 那样单独持有一个数组再动态注册
 */

/* server.h 等已由 config.c 在前面 include，无需重复 */

/* 6.x 没有 HIDDEN_CONFIG，定义为 0 使 flags 运算正确 */
#ifndef HIDDEN_CONFIG
#define HIDDEN_CONFIG 0
#define STORAGE_UNDEF_HIDDEN_CONFIG
#endif

/*============================================================================
 * X-macro 展开宏（6.x 版本）
 *
 * server.storage. 前缀统一在此处添加，config_items.h 只写字段名。
 * update（apply）位置传 NULL：6.x update_fn 签名携带 val/prev，与 apply 不兼容。
 *===========================================================================*/

#define SCFG_BOOL(name, field, default, apply) \
    createBoolConfig(name, NULL, MODIFIABLE_CONFIG, \
        server.storage.field, default, NULL, NULL),

#define SCFG_BOOL_H(name, field, default, apply) \
    createBoolConfig(name, NULL, MODIFIABLE_CONFIG | HIDDEN_CONFIG, \
        server.storage.field, default, NULL, NULL),

#define SCFG_INT(name, field, lower, upper, default, apply) \
    createIntConfig(name, NULL, MODIFIABLE_CONFIG, lower, upper, \
        server.storage.field, default, INTEGER_CONFIG, NULL, NULL),

#define SCFG_INT_H(name, field, lower, upper, default, apply) \
    createIntConfig(name, NULL, MODIFIABLE_CONFIG | HIDDEN_CONFIG, lower, upper, \
        server.storage.field, default, INTEGER_CONFIG, NULL, NULL),

#define SCFG_MEM(name, field, lower, upper, default, apply) \
    createULongLongConfig(name, NULL, MODIFIABLE_CONFIG, lower, upper, \
        server.storage.field, default, MEMORY_CONFIG, NULL, NULL),

#define SCFG_ULL(name, field, lower, upper, default, apply) \
    createULongLongConfig(name, NULL, MODIFIABLE_CONFIG, lower, upper, \
        server.storage.field, default, INTEGER_CONFIG, NULL, NULL),

#define SCFG_SIZET(name, field, lower, upper, default, apply) \
    createSizeTConfig(name, NULL, MODIFIABLE_CONFIG, lower, upper, \
        server.storage.field, default, INTEGER_CONFIG, NULL, NULL),


//6.x 未适配置项
static standardConfig storageStaticConfigs[] = {
#include "config_items.h"
    {NULL}
};

#undef SCFG_BOOL
#undef SCFG_BOOL_H
#undef SCFG_INT
#undef SCFG_INT_H
#undef SCFG_MEM
#undef SCFG_ULL
#undef SCFG_SIZET

#ifdef STORAGE_UNDEF_HIDDEN_CONFIG
#undef HIDDEN_CONFIG
#undef STORAGE_UNDEF_HIDDEN_CONFIG
#endif
