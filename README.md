# Valkey-On-Rocks (VOR / ROR)

## Introduction

**Valkey-On-Rocks (VOR)** — evolved from **Redis-On-Rocks (ROR)** — is a cost-effective, high-performance tiered storage engine built on the latest **Valkey** codebase (`valkey-io/valkey`) with **RocksDB** as the persistent cold-storage engine, saving roughly 2/3 of memory and infrastructure cost.

Valkey-On-Rocks extends the **SWAP** hot/cold tiered storage architecture onto Valkey, maintaining full compatibility with Valkey/Redis commands (including Lua scripting and `MULTI`/`EXEC` transactions) and replication protocols (`RDB`, `RORDB` SST-based replication, `GTID` command envelope & parallel replication worker dispatch, and `RESP2`/`RESP3`).

### Key Highlights in Valkey-On-Rocks
- **Upgraded to Latest Valkey Core**: Adapted to Valkey's modern `hashtable` (SwissTable-inspired slot-based dictionary), `kvstore`, embedded `robj` (`valkeyObject` with embedded key/expire), unified `LISTPACK` encodings for `Hash` / `Set` / `ZSet`, 64-bit command flags (`commands.def`), and `COMMANDLOG`.
- **Upgraded RocksDB & Modern C++20**: Synchronized `deps/rocksdb` (`webc-fork/rocksdb` v11.14.0) with upstream RocksDB and upgraded the C++ build standard to **C++20** (`-std=c++20`), preserving all custom RocksDB enhancements (`blob_list_gc`, custom compaction filters, table properties collectors, and C API bindings).
- **Zero-Warning Strict Build**: Clean compilation across C11 (`src/`) and C++20 (`deps/rocksdb`) components.
- **Full Data Type & Feature Coverage**: Supports tiered `SWAP IN` / `SWAP OUT` / `SWAP DEL` for `String`, `Hash`, `List`, `Set`, `ZSet`, and `Bitmap` (subkey-level swapping), alongside Cuckoo Filter, Absent Cache, async `Persist` mode, `RORDB` checkpoint replication, and `GTID` command envelope dispatch (`GTID <uuid:gno> <dbid> <cmd>`).

---

## Architecture

### SWAP

Valkey-On-Rocks stores hot and cold data in Valkey memory and RocksDB respectively, and exchanges hot and cold data automatically:

- **SWAP IN**: If a key (or subkey) is cold, Valkey-On-Rocks asynchronously reads the cold data from RocksDB into Valkey memory via swap threads and then executes the command.
- **SWAP OUT**: When `maxmemory` is reached (or when `swap-persist-enabled` is active), Valkey-On-Rocks selects least frequently used keys (LFU) or dirty keys/subkeys and swaps them out to RocksDB.

![SWAP](docs/images/ROR.png)

### Replication

Standard Valkey/Redis instances can act as replicas of Valkey-On-Rocks by executing `REPLICAOF` / `SLAVEOF`. The replication process is compatible with standard Valkey replication:
- **RDB Replication**: A RocksDB `CHECKPOINT` is created first, and cold data is scanned and encoded into standard RDB format alongside in-memory hot data.
- **RORDB Replication**: When `swap-repl-rordb-sync yes` is enabled between Valkey-On-Rocks nodes, cold data is transferred directly as RocksDB SST files inside the `rordb` stream for significantly faster full resynchronization.

![SWAP-REPL](docs/images/ROR-REPL.png)

---

## Getting Started with Valkey-On-Rocks

### 1. Running with Docker

```sh
git clone https://github.com/webc-fork/Redis-On-Rocks.git
cd Redis-On-Rocks
# Build Docker image
docker build -t ror .
# Run container interactively
docker run -it ror /bin/bash
```

### 2. Compilation from Source

1. **Supported Platforms**
    * Linux (Ubuntu / Debian / CentOS / RHEL, x86_64 & ARM64)
    * macOS (Darwin, Apple Silicon ARM64 & x86_64)

2. **Required Dependencies**
    * C11 & C++20 capable compiler (GCC 10+/11+/13+ or Clang 14+)
    * GNU Make (`make SWAP=yes` builds the Valkey-On-Rocks SWAP engine; CMake 3.24+ is required only on Linux if building Folly for RocksDB or building upstream standard Valkey)
    * `zlib` (`zlib1g-dev` / `zlib-devel`)
    * `gflags` (`libgflags-dev` / `gflags-devel`)

3. **Compilation Process**
    * **3.1 Install Dependencies**
    ```bash
    # Ubuntu / Debian
    sudo apt update
    sudo apt install -y build-essential cmake pkg-config zlib1g-dev libgflags-dev liburing-dev tcl

    # CentOS / RHEL
    sudo yum install -y cmake zlib-devel gflags-devel jemalloc-devel procps-ng tcl

    # macOS (Homebrew)
    brew install pkg-config zlib gflags tcl-tk
    ```
    * **3.2 Clone Source Code & Submodules**
    ```bash
    git clone https://github.com/webc-fork/Redis-On-Rocks.git
    cd Redis-On-Rocks
    git submodule update --init --recursive
    ```
    * **3.3 Build Valkey-On-Rocks (with RocksDB SWAP Engine)**
    ```bash
    make SWAP=yes -j$(nproc)
    ```
    * **3.4 Start Server**
    ```bash
    ./src/valkey-server ./ror.conf
    # Or using valkey.conf:
    ./src/valkey-server ./valkey.conf
    ```

---

## Migration

### From Valkey / Redis
1. **RDB Compatibility**: Valkey-On-Rocks is compatible with Valkey and Redis RDB files. Place `dump.rdb` in the configured `dir` and start `valkey-server` to load existing data directly.
2. **Online Replication**: Use `REPLICAOF <host> <port>` to replicate directly from an upstream Valkey/Redis primary into Valkey-On-Rocks, or from Valkey-On-Rocks to a downstream Valkey/Redis replica.

---

## Benchmark Performance

### Performance
1. Two scenarios of performance testing
    1. `100thd`: 100 concurrency, no speed limit
    2. `10Kqps`: 1000 concurrency, speed limit to 10000 QPS

2. Public cloud performance
   1. Cold refers to conf `maxmemory`'s size. Hot refers to data size within `maxmemory` conf size.
   2. SIN-AWS & FRA-AWS (`r6gd.4xlarge`)
    <table>
        <tr>
            <th style="font-weight: normal" colspan="3">Scenario\Program</th>
            <th style="font-weight: normal">xredis</th>
            <th style="font-weight: normal">ror / vor</th>
        </tr>
        <tr>
            <th style="font-weight: normal" rowspan="6">Hot</th>
            <th style="font-weight: normal" rowspan="2">Get</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">QPS=100344<br/>Latency(mean,p99): 952 1804(us)</th>
            <th style="font-weight: normal">QPS=85566<br/>Latency(mean,p99):1131 2135(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">QPS=9962<br/>Latency(mean,p99): 92 249(us)</th>
            <th style="font-weight: normal">QPS=9960<br/>Latency(mean,p99):111 320(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal" rowspan="2">HGETALL</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">QPS=81024<br/>Latency(mean,p99):1190 2383(us)</th>
            <th style="font-weight: normal">QPS=70468<br/>Latency(mean,p99):1366 2592(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">QPS=9967<br/>Latency(mean,p99): 119 404(us)</th>
            <th style="font-weight: normal">QPS=9965<br/>Latency(mean,p99):125 362(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal" rowspan="2">HGET</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">QPS=94653<br/>Latency(mean,p99): 998 1937(us)</th>
            <th style="font-weight: normal">QPS=80700<br/>Latency(mean,p99):1186 2272(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">QPS=9965<br/>Latency(mean,p99):95 191(us)</th>
            <th style="font-weight: normal">QPS=9967<br/>Latency(mean,p99):97 228(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal" rowspan="6">Cold</th>
            <th style="font-weight: normal" rowspan="2">Get</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=54457<br/>Latency(mean,p99):1760 6814(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=9959<br/>Latency(mean,p99):111 356(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal" rowspan="2">HGETALL</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=32565 Latency(mean,p99):2968 5732(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=9965<br/>Latency(mean,p99):163 524(us)</th>
        </tr><tr>
            <th style="font-weight: normal" rowspan="2">HGET</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=43512<br/>Latency(mean,p99):2263 12751(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=9966 Latency(mean,p99):118 472(us)</th>
        </tr>
    </table>
    3. ALI (`ecs.si3r.4xlarge`)
    <table>
        <tr>
            <th style="font-weight: normal" colspan="3">Scenario\Program</th>
            <th style="font-weight: normal">xredis</th>
            <th style="font-weight: normal">ror / vor</th>
        </tr>
        <tr>
            <th style="font-weight: normal" rowspan="6">Hot</th>
            <th style="font-weight: normal" rowspan="2">Get</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">QPS=96679<br/>Latency(mean,p99):1006 1267(us)</th>
            <th style="font-weight: normal">QPS=82631<br/>Latency(mean,p99):1177 1651(us)
        </th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">QPS=9983<br/>Latency(mean,p99):92 204(us)</th>
            <th style="font-weight: normal">QPS=9983<br/>Latency(mean,p99):112 250(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal" rowspan="2">HGETALL</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">QPS=76308<br/>Latency(mean,p99):1283 1917(us)</th>
            <th style="font-weight: normal">QPS=66645<br/>Latency(mean,p99):1468 2146(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">QPS=9988<br/>Latency(mean,p99):122 279(us)</th>
            <th style="font-weight: normal">QPS=9988<br/>Latency(mean,p99):139 294(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal" rowspan="2">HGET</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">QPS=86230<br/>Latency(mean,p99):1122 1610(us)</th>
            <th style="font-weight: normal">QPS=77459<br/>Latency(mean,p99):1249 1716(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">QPS=9995<br/>Latency(mean,p99):114 269(us)</th>
            <th style="font-weight: normal">QPS=9988<br/>Latency(mean,p99):126 259(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal" rowspan="6">Cold</th>
            <th style="font-weight: normal" rowspan="2">Get</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=56408<br/>Latency(mean,p99):1743 2710(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=9983<br/>Latency(mean,p99):200 706(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal" rowspan="2">HGETALL</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=38082<br/>Latency(mean,p99):2599 3791(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=9991<br/>Latency(mean,p99):148 442(us)</th>
        </tr><tr>
            <th style="font-weight: normal" rowspan="2">HGET</th>
            <th style="font-weight: normal">100thd</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=47821<br/>Latency(mean,p99):2045 3202(us)</th>
        </tr>
        <tr>
            <th style="font-weight: normal">10Kqps</th>
            <th style="font-weight: normal">-</th>
            <th style="font-weight: normal">QPS=9996<br/>Latency(mean,p99):177 891(us)</th>
        </tr>
    </table>

---

## Configuration

### Core Options
1. `maxmemory`
   * Sets a memory usage limit to the specified amount of bytes. When the memory limit is reached, Valkey-On-Rocks swaps cold/warm data out to RocksDB according to the eviction policy (see `maxmemory-policy`, LFU by default).
2. `swap-max-db-size`
   * Maximum size of RocksDB disk usage allowed. If disk usage exceeds the limit, the server will reject `DENYOOM` commands. Default is `0` (unlimited).
3. `ror.conf` & `valkey.conf`
   * `ror.conf` and `valkey.conf` have been unified to include both standard Valkey 9 configuration directives and the `SWAP` and `ROCKSDB` storage/replication sections (note: replication uses Valkey's native `PSYNC2` + `RORDB` SST checkpoint sync and `GTID` command envelope dispatch; legacy Redis 6.2 `XSYNC`/gaplog is retired).

### Detailed Configuration Reference
* [Detailed Confs](docs/ror.conf.md)

---

## Observability

1. **Keyspace**
    ```
    # Keyspace
    db0:keys=2,evicts=20000,metas=0,expires=0,avg_ttl=0
    ```
    * `db0`: current db id
    * `evicts`: cold keys count in current db (decremented when data turns warm or is deleted; incremented when data turns cold)
    * `metas`: metadata entries for warm/cold keys tracked in memory
    * `expires`: keys with an expiration TTL
    * `avg_ttl`: average TTL of key objects

2. **CPU**
    ```
    # CPU
    used_cpu_sys:0.045921
    used_cpu_user:0.187862
    used_cpu_sys_children:0.001902
    used_cpu_user_children:0.000000
    used_cpu_sys_main_thread:0.043579
    used_cpu_user_main_thread:0.183033
    swap_main_thread_cpu_usage:1.60%
    swap_swap_threads_cpu_usage:0.00%
    swap_other_threads_cpu_usage:0.00%
    ```
    * `swap_main_thread_cpu_usage`: main thread CPU usage
    * `swap_swap_threads_cpu_usage`: swap worker threads CPU usage
    * `swap_other_threads_cpu_usage`: remaining background threads CPU usage
