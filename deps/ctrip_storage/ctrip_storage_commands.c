/* 本文件由 generate_swap_command_def.py 自动生成，请勿手动修改 */

#include "ctrip_storage_data.h"      /* SWAP_NOP/IN/OUT/DEL/UTILS */
#include "ctrip_storage_objects.h"    /* CMD_SWAP_DATATYPE_* */
#include "ctrip_storage_commands.h"   /* swapCommand 结构体 */
#include <string.h>
#include <strings.h>

/********** swap命令定义表 ********************/
/* 每个条目: {name, getkeyrequests_proc, intention, intention_flags, cmd_swap_flags} */
swapCommand swapCommandTable[] = {
    {"get", NULL, SWAP_IN, 0, CMD_SWAP_DATATYPE_STRING},
    {NULL, NULL, 0, 0, 0}  /* 哨兵结尾 */
};

/* swap命令总数（不含哨兵） */
static const int swapCommandTableSize =
    sizeof(swapCommandTable) / sizeof(swapCommandTable[0]) - 1;

/* 通过命令名查找对应的 swapCommand
 * 输入: name - 命令名字符串（不区分大小写）
 * 输出: 找到返回 swapCommand 指针，找不到返回 NULL
 * 说明: 线性扫描，命令数量较少时性能足够 */
swapCommand *lookupSwapCommand(const char *name) {
    if (name == NULL) return NULL;
    for (int i = 0; i < swapCommandTableSize; i++) {
        if (strcasecmp(swapCommandTable[i].name, name) == 0) {
            return &swapCommandTable[i];
        }
    }
    return NULL;
}
