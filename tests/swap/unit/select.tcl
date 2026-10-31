start_server {overrides {save "" swap-debug-evict-keys -1} tags {"swap" "select"}} {
    test {multi-db basics} {
        r select 0
        r set key1 db0
        wait_key_cold r key1
        r select 1
        r set key1 db1
        wait_key_cold r key1

        r select 0
        assert_equal [r get key1] db0
        r select 1
        assert_equal [r get key1] db1

        r select 0
        r del key1
        assert_equal [r get key1] {}
        r select 1
        assert_equal [r get key1] db1

        r del key1
        assert_equal [r get key1] {}
        r select 0
        assert_equal [r get key1] {}
    }
    
    test {multi-db expire} {
        r select 0
        r set key2 db0
        r select 1
        r set key2 db1

        r select 0
        assert_equal [r get key2] db0
        r pexpire key2 1
        r select 1
        assert_equal [r get key2] db1
        r pexpire key2 1

        after 1000

        r select 0
        assert_equal [r get key2] {}
        r select 1
        assert_equal [r get key2] {}
    }

    test {multi-db multi/exec} {
        r select 0
        r set key3 db0
        r hmset myhash f1 v1 f2 v2 f3 v3
        r select 1
        r set key3 db1
        r rpush mylist 1 2 3 4 5

        wait_keyspace_cold r

        r select 0

        r multi
        r select 1
        r lindex mylist 3
        r hmget myhash f1 f2
        r select 0
        r get key3
        r hmget myhash f1 f2
        r select 1
        r get key3

        assert_equal [r exec] {OK 4 {{} {}} OK db0 {v1 v2} OK db1}

        r flushall
    }

    test {multi-db eval: select not allowed} {
        catch {r eval {redis.call('select', '0')} 0} e
        assert_match {*select command is not allowed from scripts in disk mode*} $e
    }

    test {multi-db metascan} {
    }

    test {multi-db scanexpire} {
        r select 0
        r setex key5 1 db0
        r select 1
        r setex key5 1 db1

        wait_keyspace_cold r

        after 2000

        r select 0
        assert_equal [rio_get_meta r key5] {}
        assert_equal [r get key5] {}
        r select 1
        assert_equal [rio_get_meta r key5] {}
        assert_equal [r get key5] {}
    }

    test {multi-db gtid} {
        r select 3
        r gtid A:1 1 set key6 db1
        r gtid A:2 0 set key6 db0 
        r select 0
        r hmset myhash6 f1 db0 f2 db0
        r gtid A:3 1 rpush mylist6 db1-1 db1-2 db1-3 db1-4
        r gtid A:4 0 rpush mylist6 db0-1 db0-2 db0-3 db0-4
        r select 1
        r hmset myhash6 f1 db1 f2 db1

        # wait_keyspace_cold r #TODO uncomment when list ready
        r select 0
        wait_key_cold r key6
        wait_key_cold r myhash6
        r select 1
        wait_key_cold r key6
        wait_key_cold r myhash6

        r select 0
        assert_equal [r get key6] db0
        assert_equal [r lindex mylist6 2] db0-3
        assert_equal [r hmget myhash6 f1 f2] {db0 db0}

        r select 1
        assert_equal [r get key6] db1
        assert_equal [r lindex mylist6 2] db1-3
        assert_equal [r hmget myhash6 f1 f2] {db1 db1}

        r flushall
    }

    test {gtid validation before swap and selectDb} {
        r select 0
        r set db0_marker ok
        r select 1
        r set cold_secret secret_val
        wait_key_cold r cold_secret
        r select 0

        # Non-string/numeric command name in GTID must not crash or switch DB
        assert_error {*unknown command*} {r gtid A:10 1 12345 cold_secret}
        assert_equal [r get db0_marker] ok

        # Out-of-range DB index must not switch DB
        assert_error {*DB index is out of range*} {r gtid A:11 999 get cold_secret}
        assert_equal [r get db0_marker] ok

        # Wrong arity inner command must not switch DB or swap in cold key
        assert_error {*wrong number of arguments*} {r gtid A:12 1 set cold_secret}
        assert_equal [r get db0_marker] ok
        r select 1
        assert_equal [object_is_cold r cold_secret] 1
        r select 0

        # ACL-denied inner command must not switch DB or swap in cold key
        r acl setuser gtid_ro on nopass +gtid +get -set ~*
        set rc [redis [srv 0 host] [srv 0 port] 0 $::tls]
        $rc auth gtid_ro ignored
        assert_error {*NOPERM*} {$rc gtid A:13 1 set cold_secret hacked}
        assert_equal [$rc get db0_marker] ok
        $rc close
        r acl deluser gtid_ro

        # Target-DB-specific ACL check: user with DB0-only write permission must be rejected on DB1
        r acl setuser gtid_db0 on nopass +gtid +get %RW~db0_*
        set rc2 [redis [srv 0 host] [srv 0 port] 0 $::tls]
        $rc2 auth gtid_db0 ignored
        assert_error {*NOPERM*} {$rc2 gtid A:14 1 get cold_secret}
        assert_equal [$rc2 get db0_marker] ok
        $rc2 close
        r acl deluser gtid_db0

        r select 1
        assert_equal [object_is_cold r cold_secret] 1
        assert_equal [r get cold_secret] secret_val
        r flushall
    }

    test {hash field ttl and new subkey commands on cold/warm keys} {
        r select 0
        r config set swap-debug-evict-keys 0
        r hset hfe f1 v1 f2 v2 f3 v3 f4 v4
        r swap.evict hfe
        wait_key_cold r hfe

        # HEXPIRE on cold field swaps in only f1 and deletes cold copy of f1 in RocksDB
        assert_equal [r hexpire hfe 100 FIELDS 1 f1] {1}
        assert {[lindex [r httl hfe FIELDS 2 f1 f2] 0] > 0}
        assert_equal [lindex [r httl hfe FIELDS 2 f1 f2] 1] -1

        # Modify volatile field value while preserving TTL, then SAVE + DEBUG RELOAD on warm hash
        assert_equal [r hsetex hfe KEEPTTL FIELDS 1 f1 v1_updated] 1
        r swap.evict hfe
        r save
        r debug reload
        # Verify cold fields stayed cold in RocksDB (len=3 for f2/f3/f4) and f1 kept updated value + TTL
        assert_match {*hot_meta:*len=3*cold_meta:*len=3*} [r swap object hfe]
        assert {[lindex [r httl hfe FIELDS 2 f1 f3] 0] > 0}
        assert_equal [r hmget hfe f1 f2 f3 f4] {v1_updated v2 v3 v4}

        # HPERSIST removes field TTL and marks field dirty so subsequent SWAP.EVICT turns hash fully cold
        assert_equal [r hpersist hfe FIELDS 1 f1] {1}
        r swap.evict hfe
        wait_key_cold r hfe
        assert_equal [object_is_cold r hfe] 1
        assert_equal [r hmget hfe f1 f2 f3 f4] {v1_updated v2 v3 v4}

        # Short-lived volatile field on cold hash must not resurrect from RocksDB after expiry
        r swap.evict hfe
        wait_key_cold r hfe
        r hpexpire hfe 50 FIELDS 1 f2
        after 150
        assert_equal [r hget hfe f2] {}
        assert_equal [r hgetdel hfe FIELDS 2 f3 f4] {v3 v4}
        assert_equal [r hlen hfe] 1

        # LMPOP on cold list swaps in only popped elements
        r rpush coldlist e1 e2 e3 e4
        r swap.evict coldlist
        wait_key_cold r coldlist
        assert_equal [r lmpop 1 coldlist LEFT COUNT 2] {coldlist {e1 e2}}
        assert_equal [r lrange coldlist 0 -1] {e3 e4}

        # ZMPOP and BZMPOP on single and multiple cold zsets
        r zadd coldzset 10 m1 20 m2 30 m3 40 m4
        r zadd coldzset2 100 a1 200 a2 300 a3
        r swap.evict coldzset
        r swap.evict coldzset2
        wait_key_cold r coldzset
        wait_key_cold r coldzset2
        assert_equal [r zmpop 2 coldzset coldzset2 MIN COUNT 2] {coldzset {{m1 10} {m2 20}}}
        assert_equal [object_is_hot r coldzset2] 1
        assert_equal [r zrange coldzset2 0 -1 WITHSCORES] {a1 100 a2 200 a3 300}
        r swap.evict coldzset
        r swap.evict coldzset2
        wait_key_cold r coldzset
        wait_key_cold r coldzset2
        assert_equal [object_is_cold r coldzset2] 1
        r debug reload
        assert_equal [r zrange coldzset2 0 -1 WITHSCORES] {a1 100 a2 200 a3 300}
        r swap.evict coldzset
        r swap.evict coldzset2
        wait_key_cold r coldzset
        wait_key_cold r coldzset2
        assert_equal [r bzmpop 1 2 coldzset coldzset2 MAX COUNT 1] {coldzset {{m4 40}}}
        r swap.evict coldzset
        r swap.evict coldzset2
        wait_key_cold r coldzset
        wait_key_cold r coldzset2
        assert_equal [r zrange coldzset 0 -1 WITHSCORES] {m3 30}
        assert_equal [r zmpop 2 nonexist_zset coldzset2 MIN COUNT 1] {coldzset2 {{a1 100}}}
        r swap.evict coldzset2
        wait_key_cold r coldzset2
        assert_equal [r bzmpop 1 2 nonexist_zset coldzset2 MAX COUNT 1] {coldzset2 {{a3 300}}}
        r swap.evict coldzset2
        wait_key_cold r coldzset2
        assert_equal [r zrange coldzset2 0 -1 WITHSCORES] {a2 200}

        # Direct subkey deletions (HDEL/ZREM/SREM) on cold keys without prior SWAP_IN survive reload
        r hmset colddel_h f1 v1 f2 v2 f3 v3
        r zadd colddel_z 10 m1 20 m2 30 m3
        r sadd colddel_s s1 s2 s3
        r swap.evict colddel_h colddel_z colddel_s
        wait_key_cold r colddel_h
        wait_key_cold r colddel_z
        wait_key_cold r colddel_s
        assert_equal [r hdel colddel_h f1 f2] 2
        assert_equal [r zrem colddel_z m1 m3] 2
        assert_equal [r srem colddel_s s2 s3] 2
        r debug reload
        assert_equal [r hgetall colddel_h] {f3 v3}
        assert_equal [r zrange colddel_z 0 -1 WITHSCORES] {m2 20}
        assert_equal [r smembers colddel_s] {s1}
        r del colddel_h colddel_z colddel_s

        # Corrupt RDB_TYPE_HASH_2 with duplicate fields is rejected by hashLoadHT2
        r del hfe coldlist coldzset coldzset2
        r hmset duphfe dupf1 val1 dupf2 val2
        r hpexpire duphfe 600000 FIELDS 1 dupf1
        r swap.evict duphfe
        wait_key_warm r duphfe
        r debug set-skip-checksum-validation 1
        r save
        set dump_path [file join [lindex [r config get dir] 1] [lindex [r config get dbfilename] 1]]
        set fp [open $dump_path rb]
        set rdb_bytes [read $fp]
        close $fp
        set corrupted_bytes [string map {dupf2 dupf1} $rdb_bytes]
        set fp [open $dump_path wb]
        puts -nonewline $fp $corrupted_bytes
        close $fp
        catch {r debug reload nosave} reload_err
        assert_match "*Error trying to load the RDB dump*" $reload_err
        r debug set-skip-checksum-validation 0
        r del duphfe

        # Pure volatile RDB_TYPE_HASH_2 (all fields have TTLs -> nfeeds == 0, loaded as pure hot)
        r hmset purevol vf1 vv1 vf2 vv2
        r hpexpire purevol 600000 FIELDS 2 vf1 vf2
        r save
        r debug reload
        assert_equal [object_is_hot r purevol] 1
        assert_equal [rio_get_meta r purevol] {}
        assert_equal [r hmget purevol vf1 vf2] {vv1 vv2}
        assert {[lindex [r httl purevol FIELDS 2 vf1 vf2] 0] > 0}
        r del purevol

        # All-expired RDB_TYPE_HASH_2 (all fields expired on RDB load -> skipped with RDB_LOAD_ERR_ALL_ITEMS_EXPIRED)
        r hmset expvol ef1 ev1 ef2 ev2
        r hpexpire expvol 800 FIELDS 2 ef1 ef2
        assert_equal [r exists expvol] 1
        r save
        after 1000
        r debug reload nosave
        assert_equal [r exists expvol] 0

        # RDB_TYPE_HASH_2 with expired key-level TTL must not leak cold DATA_CF/META_CF or cold_keys on RDB load
        r hmset expkey_warm cf1 cv1 vf1 vv1
        r hpexpire expkey_warm 600000 FIELDS 1 vf1
        r swap.evict expkey_warm
        wait_key_warm r expkey_warm
        set expkey_warm_ver [object_meta_version r expkey_warm]
        r hmset expkey_all vf1 vv1 cf1 cv1 cf2 cv2
        r hpexpire expkey_all 600000 FIELDS 1 vf1
        r swap.evict expkey_all
        wait_key_warm r expkey_all
        set expkey_all_ver [object_meta_version r expkey_all]
        r hmset expkey_all2 cf1 cv1 vf1 vv1
        r hpexpire expkey_all2 600000 FIELDS 1 vf1
        r swap.evict expkey_all2
        wait_key_warm r expkey_all2
        set expkey_all2_ver [object_meta_version r expkey_all2]

        r pexpire expkey_warm 1000
        r hpexpire expkey_all 800 FIELDS 1 vf1
        r pexpire expkey_all 1000
        r hpexpire expkey_all2 800 FIELDS 1 vf1
        r pexpire expkey_all2 1000
        assert_equal [r exists expkey_warm] 1
        assert_equal [r exists expkey_all] 1
        assert_equal [r exists expkey_all2] 1
        r save
        after 1200
        r debug reload nosave
        assert_equal [r exists expkey_warm] 0
        assert_equal [rio_get_meta r expkey_warm] {}
        assert_equal [rio_get_data r expkey_warm $expkey_warm_ver cf1] {}
        assert_equal [r swap rio-scan data [r swap encode-meta-key expkey_warm]] {}
        assert_equal [r exists expkey_all] 0
        assert_equal [rio_get_meta r expkey_all] {}
        assert_equal [rio_get_data r expkey_all $expkey_all_ver cf1] {}
        assert_equal [r swap rio-scan data [r swap encode-meta-key expkey_all]] {}
        assert_equal [r exists expkey_all2] 0
        assert_equal [rio_get_meta r expkey_all2] {}
        assert_equal [rio_get_data r expkey_all2 $expkey_all2_ver cf1] {}
        assert_equal [r swap rio-scan data [r swap encode-meta-key expkey_all2]] {}

        # Large RDB_TYPE_HASH_2 (len > hash-max-listpack-entries) with mixed cold & volatile fields + dup hot field rejection
        set orig_max_lp [lindex [r config get hash-max-listpack-entries] 1]
        r config set hash-max-listpack-entries 2
        r hmset largehfe hotf1 hv1 hotf2 hv2 coldf1 cv1 coldf2 cv2
        r hpexpire largehfe 600000 FIELDS 2 hotf1 hotf2
        r swap.evict largehfe
        wait_key_warm r largehfe
        r save
        r debug reload
        assert_match {*hot_meta:*len=2*cold_meta:*len=2*} [r swap object largehfe]
        assert_equal [r hmget largehfe hotf1 hotf2 coldf1 coldf2] {hv1 hv2 cv1 cv2}

        r debug set-skip-checksum-validation 1
        r save
        set fp [open $dump_path rb]
        set rdb_bytes [read $fp]
        close $fp
        set corrupted_bytes [string map {hotf2 hotf1} $rdb_bytes]
        set fp [open $dump_path wb]
        puts -nonewline $fp $corrupted_bytes
        close $fp
        catch {r debug reload nosave} reload_err
        assert_match "*Error trying to load the RDB dump*" $reload_err
        r debug set-skip-checksum-validation 0
        r config set hash-max-listpack-entries $orig_max_lp

        r config set swap-debug-evict-keys -1
        r flushall
    }
}

start_server {overrides {save "" swap-debug-evict-keys -1} tags {"swap" "select"}} {
    start_server {} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]

        test {multi-db repl by rdb} {
            $master config set swap-debug-evict-keys 0

            for {set db 0} {$db < 5} {incr db} {
                $master select $db
                for {set count 0} {$count < 100} {incr count} {
                    $master set kv-$count db-$db 
                    if {$count < 50} {
                        $master swap.evict kv-$count
                    }
                }

                for {set count 0} {$count < 100} {incr count} {
                    $master hmset hash-$count f1 db-$db f2 db-$db f3 db-$db f4 db-$db f5 db-$db f6 db-$db f7 db-$db f8 db-$db

                    if {$count < 25} {
                        $master config set swap-evict-step-max-subkeys 1024
                        $master swap.evict hash-$count
                    }

                    if {$count < 50} {
                        $master config set swap-evict-step-max-subkeys 4
                        $master swap.evict hash-$count 
                    }
                }
            }

            after 1000

            $slave slaveof $master_host $master_port
            wait_for_sync $slave

            for {set db 0} {$db < 5} {incr db} {
                $slave select $db
                for {set count 0} {$count < 100} {incr count} {
                    assert_equal [$slave get kv-$count] db-$db
                }

                for {set count 0} {$count < 100} {incr count} {
                    assert_equal [$slave hmget hash-$count f1 f8] [list db-$db db-$db]
                }
            }
        }

        test {multi-db propagate expire} {
            $master select 1
            $master setex key 1 db1
            $master select 0
            $master setex key 1 db0

            wait_for_ofs_sync $master $slave

            $slave select 0
            assert_equal [$slave get key] db0
            $slave select 1
            assert_equal [$slave get key] db1

            after 1000

            $slave select 0
            assert_equal [$slave get key] {}
            $slave select 1
            assert_equal [$slave get key] {}
        }

        test {multi-db multi/exec} {
            $master multi
            $master select 0
            $master set key db0
            $master hmset myhash f1 v1 f2 v2 f3 v3
            $master select 1
            $master set key db1
            $master rpush mylist 1 2 3 4 5
            $master exec

            wait_for_ofs_sync $master $slave

            # wait_keyspace_cold $slave #TODO uncomment when list ready
            $slave select 0
            wait_key_cold $slave key
            wait_key_cold $slave myhash
            r select 1
            wait_key_cold r key

            $slave multi
            $slave select 1
            $slave lindex mylist 3
            $slave hmget myhash f1 f2
            $slave select 0
            $slave get key
            $slave hmget myhash f1 f2
            $slave select 1
            $slave get key
            assert_equal [$slave exec] {OK 4 {{} {}} OK db0 {v1 v2} OK db1}
        }

    }
}

start_server {overrides {gtid-enabled "yes"} tags {"swap" "select"}} {
    start_server {overrides {gtid-enabled "yes"}} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]

        test {multi-db repl by rdb} {
            for {set db 0} {$db < 5} {incr db} {
                $master select $db
                for {set count 0} {$count < 100} {incr count} {
                    $master set kv-$count db-$db 
                }

                for {set count 0} {$count < 100} {incr count} {
                    $master hmset hash-$count f1 db-$db f2 db-$db f3 db-$db f4 db-$db f5 db-$db f6 db-$db f7 db-$db f8 db-$db
                }
            }

            after 1000

            $slave slaveof $master_host $master_port
            wait_for_sync $slave

            for {set db 0} {$db < 5} {incr db} {
                $slave select $db
                for {set count 0} {$count < 100} {incr count} {
                    assert_equal [$slave get kv-$count] db-$db
                }

                for {set count 0} {$count < 100} {incr count} {
                    assert_equal [$slave hmget hash-$count f1 f8] [list db-$db db-$db]
                }
            }
        }

        test {multi-db multi/exec} {
            $master select 0
            # $master select 1 TODO uncomment 

            $master multi
            $master select 0
            $master set key db0
            $master hmset myhash f1 v1 f2 v2 f3 v3
            $master select 1
            $master set key db1
            $master rpush mylist 1 2 3 4 5
            $master exec

            wait_for_ofs_sync $master $slave

            wait_keyspace_cold $slave

            $slave multi
            $slave select 1
            $slave lindex mylist 3
            $slave hmget myhash f1 f2
            $slave select 0
            $slave get key
            $slave hmget myhash f1 f2
            $slave select 1
            $slave get key
            assert_equal [$slave exec] {OK 4 {{} {}} OK db0 {v1 v2} OK db1}
        }
    }
}

start_server {overrides {save "" swap-debug-evict-keys -1} tags {"swap" "select"}} {
    start_server {} {

        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]

        test {multi-db chaos load} {
            $slave slaveof $master_host $master_port
            wait_for_sync $slave

            set dbnum 3
            for {set db 0} {$db < $dbnum} {incr db} {
                set load_handle($db) [start_bg_complex_data $master_host $master_port $db 100000000]
            }

            after 10000

            for {set db 0} {$db < $dbnum} {incr db} {
                stop_bg_complex_data $load_handle($db)
            }

            wait_load_handlers_disconnected

            wait_for_ofs_sync $master $slave

            for {set db 0} {$db < $dbnum} {incr db} {
                $master select $db
                $slave select $db
                wait_for_condition 500 10 {
                    [$master dbsize] eq [$slave dbsize]
                } else {
                    fail "db$db dbsize not match"
                }
            }
        }
    }
}
