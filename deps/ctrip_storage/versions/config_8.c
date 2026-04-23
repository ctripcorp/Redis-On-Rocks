/*
 * config_8_6.c - Redis 8.x 存储引擎配置注册（X-macro 版）
 *
 * 通过 #include 嵌入 src/config.c 编译单元，访问内部宏和类型。
 * 嵌入位置：static_configs[] 末尾 {NULL} 之后（registerConfigValue 可见）。
 *
 * 8.x 注册机制：动态注册到 configs 字典（registerConfigValue）。
 * apply 回调：独立函数指针，CONFIG SET 写入后触发（无 val/prev 参数）。
 */

/* server.h 等已由 config.c 在前面 include，无需重复 */

/* 前向声明：registerConfigValue() 定义在本文件之后的 config.c 中 */
int registerConfigValue(const char *name, const standardConfig *config, int alias);

/* apply 回调函数（与 6.x 共用） */
#include "config_apply.h"

/*============================================================================
 * X-macro 展开宏（8.x 版本）
 *
 * server.storage. 前缀统一在此处添加，config_items.h 只写字段名。
 * HIDDEN_CONFIG 在 8.x 原生支持，_H 变体直接使用。
 * apply 参数直接透传给 createXxxConfig 宏的最后一个参数。
 *===========================================================================*/

#define SCFG_BOOL(name, field, default, apply) \
    createBoolConfig(name, NULL, MODIFIABLE_CONFIG, \
        server.storage.field, default, NULL, apply),

#define SCFG_BOOL_H(name, field, default, apply) \
    createBoolConfig(name, NULL, MODIFIABLE_CONFIG | HIDDEN_CONFIG, \
        server.storage.field, default, NULL, apply),

#define SCFG_INT(name, field, lower, upper, default, apply) \
    createIntConfig(name, NULL, MODIFIABLE_CONFIG, lower, upper, \
        server.storage.field, default, INTEGER_CONFIG, NULL, apply),

#define SCFG_INT_H(name, field, lower, upper, default, apply) \
    createIntConfig(name, NULL, MODIFIABLE_CONFIG | HIDDEN_CONFIG, lower, upper, \
        server.storage.field, default, INTEGER_CONFIG, NULL, apply),

/* MEMORY_CONFIG：支持 mb/kb 等单位前缀 */
#define SCFG_MEM(name, field, lower, upper, default, apply) \
    createULongLongConfig(name, NULL, MODIFIABLE_CONFIG, lower, upper, \
        server.storage.field, default, MEMORY_CONFIG, NULL, apply),

#define SCFG_ULL(name, field, lower, upper, default, apply) \
    createULongLongConfig(name, NULL, MODIFIABLE_CONFIG, lower, upper, \
        server.storage.field, default, INTEGER_CONFIG, NULL, apply),

#define SCFG_SIZET(name, field, lower, upper, default, apply) \
    createSizeTConfig(name, NULL, MODIFIABLE_CONFIG, lower, upper, \
        server.storage.field, default, INTEGER_CONFIG, NULL, apply),

/* functions */

static int updateSwapThreadsAutoScaleMin(const char **err) {
    if (server.storage.swap_threads_auto_scale_min >
        server.storage.swap_threads_auto_scale_max) {
        *err = "swap-threads-auto-scale-min must be <= swap-threads-auto-scale-max";
        return 0;
    }
    return 1;
}




/* storageStaticConfigs - 存储引擎配置项数组，格式与 static_configs[] 完全一致 */
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

/*
 * storageRegisterConfigs - 将 storageStaticConfigs[] 注册到 configs 字典
 *
 * 与 initConfigValues() 遍历 static_configs[] 的逻辑完全一致。
 * 在 initConfigValues() 末尾调用（由 src/config.c 负责）。
 * 输入/输出: 无（修改全局 configs 字典）
 */
static void storageRegisterConfigs(void) {
    for (standardConfig *config = storageStaticConfigs; config->name != NULL; config++) {
        if (config->interface.init) config->interface.init(config);
        int ret = registerConfigValue(config->name, config, 0);
        serverAssert(ret);
        if (config->alias) {
            ret = registerConfigValue(config->alias, config, ALIAS_CONFIG);
            serverAssert(ret);
        }
    }
}
