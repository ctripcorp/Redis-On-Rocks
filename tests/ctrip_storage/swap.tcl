# start_server {tags {"swap"}} {

#     test {set - get command} {
#         r set x foobar
#         r get x
#     } {foobar}

#     test {set - get command} {
#         catch {r CTRIP_STORAGE evict x} error
#         assert_equal $error "ERR Storage SPI is not enabled"

#     }
# }

start_server {tags {"swap"} overrides { storage-type memory }} {
    test {ping} {
        r ping
    } {PONG}
    test {set - get command} {
        r set x foobar
        r CTRIP_STORAGE EVICT x
        assert_equal [r get x] "foobar"
        r set x foobar1
        r CTRIP_STORAGE EVICT x
        assert_equal [r dbsize] 0
        r get x
    } {foobar1}

    test {expire command} {
        r expire x 1
        r CTRIP_STORAGE EVICT x
        after 1100
        r get x
    } {}

    test {wait for async operations} {
        # Wait for all async operations to complete before leak check
        r CTRIP_STORAGE WAIT
    } {OK}

    # Skip leak check for storage tests - buffered allocators intentionally
    # pre-allocate memory pools that appear as "leaks" to the leaks tool
    set srv [lindex $::servers end]
    dict set srv "skipleaks" 1
    lset ::servers end $srv
}

# Test full sync when all data is in memory storage
start_server {tags {"swap"} overrides { storage-type memory }} {
    # Create multiple keys and evict them to memory storage
    test {prepare data in memory storage} {
        for {set i 0} {$i < 10} {incr i} {
            r set key$i value$i
            r CTRIP_STORAGE EVICT key$i
        }
        # Verify all keys are evicted (dbsize should be 0)
        assert_equal [r dbsize] 0
    }

    test {get data from memory storage} {
        # Verify we can still get all keys
        for {set i 0} {$i < 10} {incr i} {
            assert_equal [r get key$i] value$i
        }
    }

    test {full sync from memory storage} {
        # Start a replica server
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        start_server {overrides { storage-type memory }} {
            # Configure as replica
            r replicaof $master_host $master_port

            # Wait for sync to complete
            wait_for_sync r

            # Verify all keys are replicated
            for {set i 0} {$i < 10} {incr i} {
                assert_equal [r get key$i] value$i
            }

            # Verify dbsize on replica
            assert_equal [r dbsize] 10
        }
    }

    test {wait for async operations} {
        r CTRIP_STORAGE WAIT
    } {OK}

    set srv [lindex $::servers end]
    dict set srv "skipleaks" 1
    lset ::servers end $srv
}

# 测试：RDB SAVE + RESTART，验证数据全部加载到 memory storage 中
# 原理：SET → EVICT（写入跳表）→ SAVE（冷键路径写入RDB）→ 重启 → RDB 加载直写存储层 → GET 触发 SWAP_IN
start_server {tags {"swap"} overrides { storage-type memory }} {
    test {RDB load: prepare data and evict to storage} {
        for {set i 0} {$i < 10} {incr i} {
            r set loadkey$i loadval$i
            r CTRIP_STORAGE EVICT loadkey$i
        }
        # EVICT 后主字典为空，数据在 memory storage 跳表中
        assert_equal [r dbsize] 0
    }

    test {RDB load: SAVE and verify cold storage after restart} {
        r save
        # 重启服务器，重新加载 RDB，数据应全部进入 memory storage 跳表
        restart_server 0 true false
        # GET 触发 SWAP_IN 从存储层加载数据
        for {set i 0} {$i < 10} {incr i} {
            assert_equal [r get loadkey$i] loadval$i
        }
    }

    test {wait for async operations} {
        r CTRIP_STORAGE WAIT
    } {OK}

    set srv [lindex $::servers end]
    dict set srv "skipleaks" 1
    lset ::servers end $srv
}