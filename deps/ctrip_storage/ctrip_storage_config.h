/*
 * ctrip_storage_config.h - 存储引擎配置描述符定义
 *
 * 定义轻量级配置描述符 StorageConfigDesc，不依赖任何 Redis 内部类型，
 * 跨 Redis 版本（6.x / 8.x）通用。
 *
 * ctrip_storage 通过 storageGetConfigDescriptors() 向外暴露所有配置项的
 * 元信息（名称、字段偏移、类型、默认值），由 versions/config_register_*.c
 * 负责翻译为对应 Redis 版本的 standardConfig 并注册。
 *
 * 新增配置只需修改 ctrip_storage_config.c 中的描述符表，无需改动 src/config.c。
 */

#ifndef __CTRIP_STORAGE_CONFIG_H__
#define __CTRIP_STORAGE_CONFIG_H__

#include <stddef.h>  /* offsetof, size_t */

/* 配置值类型枚举，对应 StorageConfigDesc.def 中的 union 分支 */
typedef enum {
    STORAGE_CONF_BOOL,       /* int，0 或 1，CONFIG SET 接受 yes/no */
    STORAGE_CONF_INT,        /* int，有上下界 */
    STORAGE_CONF_UINT,       /* unsigned int，有上下界 */
    STORAGE_CONF_LONG_LONG,  /* long long，有上下界 */
    STORAGE_CONF_ULONG_LONG, /* unsigned long long，有上下界 */
    STORAGE_CONF_SIZE_T,     /* size_t，有上下界 */
} StorageConfType;

/* 配置项标志位，与 config.c 内部 flags 隔离，由注册函数负责翻译 */
#define STORAGE_CONF_IMMUTABLE   (1 << 0)  /* 启动后不可修改 */
#define STORAGE_CONF_MODIFIABLE  (1 << 1)  /* 运行时可通过 CONFIG SET 修改 */
#define STORAGE_CONF_HIDDEN      (1 << 2)  /* CONFIG GET * 不显示（调试用） */

/*
 * StorageConfigDesc - 存储配置项描述符
 *
 * 编译期静态数组的元素类型。记录每个配置项的名称、类型、
 * StorageServerNamespace 中的字段偏移（offsetof）和默认值。
 * 不含任何 Redis 内部类型，版本无关。
 */
typedef struct StorageConfigDesc {
    const char *name;       /* CONFIG GET/SET 中使用的名称，如 "swap-absent-cache-enabled" */
    const char *alias;      /* 配置别名，可为 NULL */
    StorageConfType type;   /* 字段的 C 类型 */
    int flags;              /* STORAGE_CONF_xxx 标志组合 */
    size_t offset;          /* offsetof(StorageServerNamespace, field) */
    union {
        struct { int            default_val;                                        } bval;   /* BOOL */
        struct { int            default_val; int            lower; int            upper; } ival;   /* INT */
        struct { unsigned int   default_val; unsigned int   lower; unsigned int   upper; } uival;  /* UINT */
        struct { long long      default_val; long long      lower; long long      upper; } llval;  /* LONG_LONG */
        struct { unsigned long long default_val; unsigned long long lower; unsigned long long upper; } ullval; /* ULONG_LONG */
        struct { size_t         default_val; size_t         lower; size_t         upper; } stval;  /* SIZE_T */
    } def;
} StorageConfigDesc;

/*
 * storageGetConfigDescriptors - 获取所有存储配置描述符
 *
 * 输入:  count - 输出参数，写入描述符数组长度
 * 输出:  指向静态描述符数组的指针（进程生命周期内有效）
 */
const StorageConfigDesc *storageGetConfigDescriptors(int *count);

#endif /* __CTRIP_STORAGE_CONFIG_H__ */
