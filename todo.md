# Valkey-On-Rocks (VOR) 升级迁移与打磨计划

## 目标
将 Redis-On-Rocks (基于 Redis 6.2) 平滑升级并合并至最新版本的 Valkey (https://github.com/valkey-io/valkey.git)，保留完整提交历史，尊重 Redis-On-Rocks 的核心分层存储与 RocksDB Swap 设计思想，适配 Valkey 的数据结构演进与 C++ 标准，消除编译报警，并确保通过测试套件。

---

## 阶段规划与进展

### 阶段一：分支准备与 Git 历史合并基线 (Git History & Baseline)
- [x] 在当前 PR 分支 (`feature/multiplatform-ci-and-deps`) 基础上创建独立新分支 `feature/valkey-on-rocks`（不影响原有 PR #461）。
- [x] 解除浅克隆 (`unshallow`) 并引入本地 Valkey 完整提交历史。
- [x] 确立两路历史融合的 Merge Commit 骨架，确保 `feature/multiplatform-ci-and-deps` 与 `valkey/unstable` 历史完整保留（共 15,038 commits，双 Parent 合并）。
- [x] 验证 Valkey 基础环境构建及基础测试套件通过。

### 阶段二：架构差异分析与核心模块适配 (Core Data Structures & Engine Adaptation)
- [x] **Dict 与存储结构适配**：
  - 对齐 Valkey 8/9 新版 `kvstore` 与 slot-based dict、`dictEntry` 内存布局与迭代器。
  - 适配 `ctrip_swap_*.c` 对 kvstore/dict/db 内部结构的调用（`swap_string`, `swap_hash`, `swap_list`, `swap_set`, `swap_zset`, `swap_bitmap` 等数据类型全量支持）。
- [x] **Server / DB / Client 结构体扩展**：
  - 迁移 Redis-On-Rocks 在 `server.h` 中的 `swapData`, `swapCtx`, `swapRequest` 等关键字段到 Valkey `server.h`。
  - 适配 `client`、`serverDb` 及 `robj` (valkeyObject) 对象的冷热标记及元数据（dirty_meta, dirty_data, persistent, persist_keep）。
  - 修复 `objectSetKeyAndExpire` 中的引用计数与字符串复制逻辑，杜绝内存越界与崩溃。
- [x] **命令分发与拦截钩子 (Dispatch & Swap Interception)**：
  - 适配 Valkey 新版命令标志系统（64 位 bitmask 及 `commands.def` 生成机制）。
  - 在 `call()`, `lookupKeyRead`, `lookupKeyWrite`, `processCommand` 中植入 Swap 拦截逻辑。
  - 适配 `selectCommand` 脚本限制、`gtidCommand` 调度执行与 `gtid-enabled` 配置项。
  - 适配 `commandlog` (原 slowlog) 中的 swap traces、swap count 与 swap duration 记录与统计。
  - 拦截不支持 swap 的数据类型命令（`swap-soft-block-cmd-enabled` 机制，覆盖普通执行、MULTI、Lua 脚本与主从复制路径）。
- [x] **后台换出与淘汰 (Eviction & Compaction & Expiry)**：
  - 适配 Valkey 新版内存驱逐（`evict.c`）与过期键扫描（`expire.c`，引入 `scanExpireDbCycle`）。
  - 支持 TTL 自动压缩（`ttl_compact`）及主从之间通过 ping/swap.info 传递 `SST-AGE-LIMIT`。
  - 支持 `swap-batch-limit` 动态配置与参数校验，修复线程缩容（scale-down）竞争下的 crash bug。
- [x] **持久化与复制 (RDB, AOF & Replication)**：
  - 适配 `rdb.c`, `aof.c`, `replication.c`，保留 RocksDB 快照保存、`rordb` 复制与 GTID 增量同步逻辑。
  - 在复制握手（Handshake）中广播 `swap_info` 与 `rordb` 能力集。

### 阶段三：构建系统、RocksDB 升级与 C++20 标准打磨 (Build System, RocksDB Upgrade & Modern C++20)
- [x] **RocksDB 子模块 Fork 与最新版本同步 (`webc-fork/rocksdb`)**：
  - 将 `ctripcorp/rocksdb` Fork 至 `https://github.com/webc-fork/rocksdb.git`。
  - 同步官方最新 `meta/rocksdb` `main` 分支（v11.12+），并在新分支 `ctrip-main` 上完整迁移合并 Ctrip 定制补丁（`LocalTimeR` 死锁修复、`rocksdb_sst_file_metadata_get_file_create_time`、`GetInfoLogFiles` 日志前缀修复、`covering_the_whole_range` picker 修复、`blob_list_gc` 全量支持及 C API 暴露）。
  - 推送合并后的 `ctrip-main` 至 `webc-fork/rocksdb` 的 `ctrip-main` 与 `main` 分支，更新 `.gitmodules` 与 `deps/rocksdb` 子模块指向。
- [x] **Makefile 与 C++20 升级**：
  - 支持 `valkey-server` 与 SWAP 引擎 (`make SWAP=yes`) 联合编译。
  - 升级 C++ 标准至最新 RocksDB 所使用的 **C++20**（`-std=c++20` / `CMAKE_CXX_STANDARD 20`）。
- [x] **消除编译警告**：
  - 开启 `-Werror` 严格编译，消除 `deps/rocksdb` 与 `Redis-On-Rocks` 所有 Clang / GCC 报警，达到零报警构建。

### 阶段四：文档与配置更新 (Documentation & Configuration)
- [x] **README.md 改造**：
  - 在 Redis-On-Rocks 架构文档与图表基础上，更新为 Valkey-On-Rocks 架构说明。
  - 说明与 Valkey 最新版本的兼容性、C++20 升级及新增特性。
- [x] **配置文件适配**：
  - 合并 `ror.conf` 与 `valkey.conf`，提供兼容 Valkey 和 RocksDB 存储层的配置模板。

### 阶段五：测试验证与逐步推进 (Testing & Verification)
- [x] 基础内存模式单元测试验证。
- [x] **RocksDB Swap 单元测试（`tests/swap/unit/*` 共 39 个测试套件，100% 全部通过）**：
  - `swap/unit/swap_out+in` (通过)
  - `swap/unit/del` (通过)
  - `swap/unit/dbsize` (通过)
  - `swap/unit/swap_mode` (通过)
  - `swap/unit/hash` (通过)
  - `swap/unit/absent_cache` (通过)
  - `swap/unit/lock` (通过)
  - `swap/unit/dirty` (通过)
  - `swap/unit/expire` (通过)
  - `swap/unit/metascan` (通过)
  - `swap/unit/metascan_multidb` (通过)
  - `swap/unit/list` (通过)
  - `swap/unit/zset` (通过)
  - `swap/unit/bitmap` (通过)
  - `swap/unit/rdb` (通过)
  - `swap/unit/save_rdb` (通过)
  - `swap/unit/load_rdb` (通过)
  - `swap/unit/big_hash` (通过)
  - `swap/unit/big_set` (通过)
  - `swap/unit/blob_list_gc` (通过)
  - `swap/unit/client` (通过)
  - `swap/unit/debug` (通过)
  - `swap/unit/geo` (通过)
  - `swap/unit/import_mode` (通过)
  - `swap/unit/info` (通过)
  - `swap/unit/lazydel` (通过)
  - `swap/unit/ltrim_check` (通过)
  - `swap/unit/monitor` (通过)
  - `swap/unit/multi` (通过)
  - `swap/unit/scripting` (通过)
  - `swap/unit/select` (通过)
  - `swap/unit/slowlog` (通过)
  - `swap/unit/swap_error` (通过)
  - `swap/unit/swap_info` (通过)
  - `swap/unit/swap_out+del` (通过)
  - `swap/unit/swap_unsupport_cmd` (通过)
  - `swap/unit/ttl_compact` (通过)
  - `swap/unit/swap_thread` (通过)
  - `swap/unit/swap_thread_race_scaledown` (通过)
- [x] **RocksDB Swap 集成测试（`tests/swap/integration/*` 共 19 个测试套件，100% 全部通过）**：
  - `bgsave` (通过), `client_rate_limit_bug` (通过), `compact_range` (通过), `complex_data` (通过), `concurrency` (通过), `concurrent` (通过), `expire_evict` (通过), `info_rocksdb_stats` (通过), `multi_bighash` (通过), `overwrite` (通过), `persist` (通过), `pipeline` (通过), `repl_self_connect` (通过), `replication` (通过), `rocksdb_log_rotate` (通过), `rordb` (通过), `swap_load` (通过), `type_error` (通过), `zset` (通过)
- [x] 多线程并发与基准测试验证（带有安全 timeout 控制的 `valkey-benchmark`，含热内存模式与持续高频 Swap-In/Out 混合负载验证）。
- [x] 整体构建与代码规范复查，确保工作区干净，推送至远程。
