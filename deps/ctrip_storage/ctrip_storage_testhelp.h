/* ctrip_storage_testhelp.h - ctrip_storage 测试辅助宏和函数声明
 *
 * 在 #ifdef REDIS_TEST 块中 include 此文件，提供：
 * - test_assert: 断言宏，失败时打印错误并计数
 * - UNUSED: 抑制未使用变量警告
 * - TEST: 打印测试名称并接代码块
 * - initTestRedisServer: stub（ctrip_storage 单测不需要完整 server 初始化）
 */
#ifndef CTRIP_STORAGE_TESTHELP_H
#define CTRIP_STORAGE_TESTHELP_H

#include "testhelp.h"

/* test_assert: 断言宏，失败时打印文件/行号并递增失败计数 */
#ifndef test_assert
#define test_assert(e) do { \
    __test_num++; \
    if (!(e)) { \
        printf("FAILED: %s (line %d)\n", #e, __LINE__); \
        __failed_tests++; \
    } \
} while(0)
#endif

/* UNUSED: 抑制未使用变量警告 */
#ifndef UNUSED
#define UNUSED(x) ((void)(x))
#endif

/* TEST: 打印测试描述并接代码块 */
#ifndef TEST
#define TEST(name) printf("    [%s]\n", name);
#endif

/* initTestRedisServer stub：
 * swapDataTest 需要完整 server 初始化（server.db 等），此处提供空 stub 使其能 link。
 * 实际上 ctripStorageTest 不调用 swapDataTest，此 stub 不会被执行。
 */
static inline int initTestRedisServer(void) { return 0; }

#endif /* CTRIP_STORAGE_TESTHELP_H */
