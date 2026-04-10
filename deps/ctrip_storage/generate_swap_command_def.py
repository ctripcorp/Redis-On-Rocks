#!/usr/bin/env python3
"""
从 commands/ 目录下的 JSON 文件生成 ctrip_storage_commands.c

参考 Redis 的 utils/generate-command-code.py 设计，
为 ctrip_storage 冷热交换引擎生成命令定义表和查找函数。

用法: python3 generate_swap_command_def.py [commands_dir] [output_file]
  默认: commands_dir = ./commands, output_file = ./ctrip_storage_commands.c

JSON 格式示例 (commands/get.json):
{
    "GET": {
        "intention": "SWAP_IN",
        "intention_flags": 0,
        "cmd_swap_flags": ["SWAP_DATATYPE_STRING"],
        "getkeyrequests_proc": null
    }
}

命令名规则:
  - JSON 中的 key 不区分大小写，脚本会自动转小写
  - 顶级命令: "GET" → "get"
  - 子命令: 使用 "|" 分隔，如 "CONFIG|GET" → "config|get"
  - 生成的 name 与 redisCommand.fullname 格式一致，可直接用 cmd->fullname 查找
"""

import os
import sys
import glob
import json

# === 映射表 ===

# 意图类型: JSON字符串 → C宏名
INTENTION_MAP = {
    "SWAP_NOP":   "SWAP_NOP",
    "SWAP_IN":    "SWAP_IN",
    "SWAP_OUT":   "SWAP_OUT",
    "SWAP_DEL":   "SWAP_DEL",
    "SWAP_UTILS": "SWAP_UTILS",
}

# 数据类型标志: JSON字符串 → C宏名 (uint64_t, 定义在 ctrip_storage_objects.h)
CMD_SWAP_FLAGS_MAP = {
    "SWAP_DATATYPE_KEYSPACE": "CMD_SWAP_DATATYPE_KEYSPACE",
    "SWAP_DATATYPE_STRING":   "CMD_SWAP_DATATYPE_STRING",
    "SWAP_DATATYPE_HASH":     "CMD_SWAP_DATATYPE_HASH",
    "SWAP_DATATYPE_SET":      "CMD_SWAP_DATATYPE_SET",
    "SWAP_DATATYPE_ZSET":     "CMD_SWAP_DATATYPE_ZSET",
    "SWAP_DATATYPE_LIST":     "CMD_SWAP_DATATYPE_LIST",
    "SWAP_DATATYPE_BITMAP":   "CMD_SWAP_DATATYPE_BITMAP",
}

# 意图标志位: JSON字符串 → C宏名 (uint32_t, 定义在 ctrip_storage_request.h)
INTENTION_FLAGS_MAP = {
    "SWAP_IN_DEL":             "SWAP_IN_DEL",
    "SWAP_IN_META":            "SWAP_IN_META",
    "SWAP_IN_DEL_MOCK_VALUE":  "SWAP_IN_DEL_MOCK_VALUE",
    "SWAP_IN_OVERWRITE":       "SWAP_IN_OVERWRITE",
    "SWAP_IN_FORCE_HOT":       "SWAP_IN_FORCE_HOT",
    "SWAP_EXPIRE_FORCE":       "SWAP_EXPIRE_FORCE",
    "SWAP_OOM_CHECK":          "SWAP_OOM_CHECK",
    "SWAP_METASCAN_SCAN":      "SWAP_METASCAN_SCAN",
    "SWAP_METASCAN_RANDOMKEY": "SWAP_METASCAN_RANDOMKEY",
    "SWAP_METASCAN_EXPIRE":    "SWAP_METASCAN_EXPIRE",
    "SWAP_OUT_PERSIST":        "SWAP_OUT_PERSIST",
    "SWAP_OUT_KEEP_DATA":      "SWAP_OUT_KEEP_DATA",
}


def parse_commands(commands_dir):
    """读取 commands/ 目录下所有 JSON 文件，解析命令定义
    输入: commands_dir - JSON文件所在目录路径
    输出: 命令定义列表，按命令名排序"""
    commands = []
    json_files = sorted(glob.glob(os.path.join(commands_dir, "*.json")))

    if not json_files:
        print(f"警告: {commands_dir} 下未找到 JSON 文件", file=sys.stderr)
        return commands

    for filepath in json_files:
        with open(filepath, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"错误: 解析 {filepath} 失败: {e}", file=sys.stderr)
                sys.exit(1)

        for cmd_name, cmd_info in data.items():
            cmd = {
                "name": cmd_name.lower(),
                "intention": cmd_info.get("intention", "SWAP_NOP"),
                "intention_flags": cmd_info.get("intention_flags", 0),
                "cmd_swap_flags": cmd_info.get("cmd_swap_flags", []),
                "getkeyrequests_proc": cmd_info.get("getkeyrequests_proc", None),
            }
            # 验证 intention 合法性
            if cmd["intention"] not in INTENTION_MAP:
                print(f"错误: 命令 {cmd_name} 的 intention '{cmd['intention']}' 不合法，"
                      f"可选值: {list(INTENTION_MAP.keys())}", file=sys.stderr)
                sys.exit(1)
            # 验证 cmd_swap_flags 合法性
            for flag in cmd["cmd_swap_flags"]:
                if flag not in CMD_SWAP_FLAGS_MAP:
                    print(f"错误: 命令 {cmd_name} 的 cmd_swap_flags '{flag}' 不合法，"
                          f"可选值: {list(CMD_SWAP_FLAGS_MAP.keys())}", file=sys.stderr)
                    sys.exit(1)
            # 验证 intention_flags 合法性（如果是列表）
            if isinstance(cmd["intention_flags"], list):
                for flag in cmd["intention_flags"]:
                    if flag not in INTENTION_FLAGS_MAP:
                        print(f"错误: 命令 {cmd_name} 的 intention_flags '{flag}' 不合法，"
                              f"可选值: {list(INTENTION_FLAGS_MAP.keys())}", file=sys.stderr)
                        sys.exit(1)
            commands.append(cmd)

    # 按命令名排序，保证生成结果稳定
    commands.sort(key=lambda c: c["name"])
    return commands


def format_intention(intention_str):
    """将意图字符串转为C宏名"""
    return INTENTION_MAP[intention_str]


def format_intention_flags(flags):
    """将意图标志转为C表达式
    支持整数(如0)或字符串列表(如["SWAP_IN_DEL", "SWAP_IN_META"])"""
    if isinstance(flags, int):
        return str(flags)
    if isinstance(flags, list):
        if not flags:
            return "0"
        return "|".join(INTENTION_FLAGS_MAP[f] for f in flags)
    return str(flags)


def format_cmd_swap_flags(flags):
    """将数据类型标志数组转为C表达式
    输入: ["SWAP_DATATYPE_STRING", "SWAP_DATATYPE_HASH"]
    输出: "CMD_SWAP_DATATYPE_STRING|CMD_SWAP_DATATYPE_HASH" """
    if not flags:
        return "0"
    return "|".join(CMD_SWAP_FLAGS_MAP[f] for f in flags)


def generate_c_file(commands, output_path):
    """生成 ctrip_storage_commands.c
    输入: commands - 命令定义列表, output_path - 输出文件路径"""
    lines = []

    # 文件头
    lines.append("/* 本文件由 generate_swap_command_def.py 自动生成，请勿手动修改 */")
    lines.append("")
    lines.append('#include "ctrip_storage_data.h"      /* SWAP_NOP/IN/OUT/DEL/UTILS */')
    lines.append('#include "ctrip_storage_request.h"    /* SWAP_IN_DEL 等意图标志位 */')
    lines.append('#include "ctrip_storage_objects.h"    /* CMD_SWAP_DATATYPE_* */')
    lines.append('#include "ctrip_storage_commands.h"   /* swapCommand 结构体 */')
    lines.append('#include <string.h>')
    lines.append('#include <strings.h>')
    lines.append("")

    # 收集需要 extern 声明的 getkeyrequests_proc 函数
    extern_procs = set()
    for cmd in commands:
        proc = cmd["getkeyrequests_proc"]
        if proc:
            extern_procs.add(proc)

    if extern_procs:
        lines.append("/* 外部 getkeyrequests_proc 函数声明 */")
        for proc in sorted(extern_procs):
            lines.append(f"extern int {proc}(int dbid, struct redisCommand *cmd,")
            lines.append(f"        robj **argv, int argc, struct getKeyRequestsResult *result);")
        lines.append("")

    # 生成命令定义表
    lines.append("/********** swap命令定义表 ********************/")
    lines.append("/* 每个条目: {name, getkeyrequests_proc, intention, intention_flags, cmd_swap_flags} */")
    lines.append("swapCommand swapCommandTable[] = {")

    for cmd in commands:
        name = cmd["name"]
        proc = cmd["getkeyrequests_proc"] or "NULL"
        intention = format_intention(cmd["intention"])
        intention_flags = format_intention_flags(cmd["intention_flags"])
        cmd_swap_flags = format_cmd_swap_flags(cmd["cmd_swap_flags"])

        lines.append(f'    {{"{name}", {proc}, {intention}, {intention_flags}, {cmd_swap_flags}}},')

    lines.append('    {NULL, NULL, 0, 0, 0}  /* 哨兵结尾 */')
    lines.append("};")
    lines.append("")

    # 命令总数
    lines.append("/* swap命令总数（不含哨兵） */")
    lines.append("static const int swapCommandTableSize =")
    lines.append("    sizeof(swapCommandTable) / sizeof(swapCommandTable[0]) - 1;")
    lines.append("")

    # lookupSwapCommand 查找函数
    lines.append("/* 通过命令名查找对应的 swapCommand")
    lines.append(" * 输入: name - 命令名字符串（不区分大小写）")
    lines.append(" * 输出: 找到返回 swapCommand 指针，找不到返回 NULL")
    lines.append(" * 说明: 线性扫描，命令数量较少时性能足够 */")
    lines.append("swapCommand *lookupSwapCommand(const char *name) {")
    lines.append("    if (name == NULL) return NULL;")
    lines.append("    for (int i = 0; i < swapCommandTableSize; i++) {")
    lines.append('        if (strcasecmp(swapCommandTable[i].name, name) == 0) {')
    lines.append("            return &swapCommandTable[i];")
    lines.append("        }")
    lines.append("    }")
    lines.append("    return NULL;")
    lines.append("}")
    lines.append("")

    with open(output_path, 'w') as f:
        f.write("\n".join(lines))

    print(f"已生成 {output_path}，共 {len(commands)} 个命令定义")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # 支持命令行参数覆盖默认路径
    if len(sys.argv) >= 2:
        commands_dir = sys.argv[1]
    else:
        commands_dir = os.path.join(script_dir, "commands")

    if len(sys.argv) >= 3:
        output_path = sys.argv[2]
    else:
        output_path = os.path.join(script_dir, "ctrip_storage_commands.c")

    if not os.path.isdir(commands_dir):
        print(f"错误: commands 目录不存在: {commands_dir}", file=sys.stderr)
        sys.exit(1)

    commands = parse_commands(commands_dir)
    generate_c_file(commands, output_path)


if __name__ == "__main__":
    main()
