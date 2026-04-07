# Tests for commit: [fix] metascan is only allowed for current dbid
#
# Bug: metaScanEncodeRange used *end = NULL, so a scan on DB0 could walk
#      through other DBs' meta keys in RocksDB (meta keys are encoded with a
#      dbid prefix). This caused:
#        1. Extremely long scans that never finished "within" a DB.
#        2. Keys from DB1/DB2/... incorrectly appearing in DB0 scan results.
#
# Fix: Set an explicit upper-bound key (dbid+1) on the RocksDB iterator, and
#      validate dbid in decodeData to treat out-of-range keys as EOF.

start_server {overrides {save ""} tags {"swap" "metascan"}} {
    r config set swap-debug-evict-keys 0

    # --------------------------------------------------------------------------
    # Helper: collect all keys via SCAN in the currently selected DB.
    # --------------------------------------------------------------------------
    proc full_scan {client} {
        set keys {}
        set cursor 0
        while 1 {
            set res [$client scan $cursor]
            set cursor [lindex $res 0]
            lappend keys {*}[lindex $res 1]
            if {$cursor == 0} break
        }
        lsort $keys
    }

    test {metascan cross-db isolation: cold DB0 keys do NOT appear in DB1 scan} {
        # Write one cold key to DB0 and one to DB1.
        r select 0
        r set db0key db0val
        r swap.evict db0key
        wait_key_cold r db0key

        r select 1
        r set db1key db1val
        r swap.evict db1key
        wait_key_cold r db1key

        # Scan DB1: must only see db1key.
        set got [full_scan r]
        assert_equal $got {db1key}

        # Switch to DB0 and verify its scan is equally clean.
        r select 0
        set got [full_scan r]
        assert_equal $got {db0key}

        r select 0 ; r del db0key
        r select 1 ; r del db1key
        r select 0
    }

    test {metascan scan terminates (cursor returns 0) after DB boundary} {
        # Fill DB0 with several cold keys. DB1 has its own cold keys.
        # Before the fix the scan cursor could wander into DB1's RocksDB range
        # and never return cursor=0 for DB0.
        r select 0
        foreach k {a b c d e} { r set $k $k }
        r swap.evict a b c d e
        foreach k {a b c d e} { wait_key_cold r $k }

        r select 1
        foreach k {x y z} { r set $k $k }
        r swap.evict x y z
        foreach k {x y z} { wait_key_cold r $k }

        # Exhaust DB0 scan; cursor must eventually reach 0.
        r select 0
        set all_keys [full_scan r]
        assert_equal [lsort $all_keys] {a b c d e}

        # Exhaust DB1 scan similarly.
        r select 1
        set all_keys [full_scan r]
        assert_equal [lsort $all_keys] {x y z}

        r select 0 ; r del a b c d e
        r select 1 ; r del x y z
        r select 0
    }

    test {metascan RANDOMKEY cross-db isolation: DB0 randomkey never returns DB1 key} {
        # DB0 is empty; DB1 has a cold key.
        r select 0
        r flushdb

        r select 1
        r flushdb
        r set onlydb1 val
        r swap.evict onlydb1
        wait_key_cold r onlydb1

        # DB0 has no keys; RANDOMKEY must return nil (not "onlydb1" from DB1).
        r select 0
        assert_equal [r randomkey] {}

        r select 1 ; r del onlydb1
        r select 0
    }

    test {metascan RANDOMKEY picks from correct db when both DBs have cold keys} {
        r select 0
        r flushdb
        r set keyA valA
        r swap.evict keyA
        wait_key_cold r keyA

        r select 1
        r flushdb
        r set keyB valB
        r swap.evict keyB
        wait_key_cold r keyB

        # DB0 randomkey must be keyA, DB1 randomkey must be keyB.
        r select 0
        assert_equal [r randomkey] keyA
        r select 1
        assert_equal [r randomkey] keyB

        r select 0 ; r del keyA
        r select 1 ; r del keyB
        r select 0
    }

    test {metascan scan count matches dbsize across multiple DBs} {
        # Populate three DBs with cold keys and verify each scan returns exactly
        # the right number of keys (guards against cross-db count pollution).
        for {set db 0} {$db < 3} {incr db} {
            r select $db
            r flushdb
            for {set i 0} {$i < 10} {incr i} {
                r set "db${db}-key${i}" "val"
            }
            r swap.evict {*}[r keys *]
            wait_keyspace_cold r
        }

        for {set db 0} {$db < 3} {incr db} {
            r select $db
            set scanned [full_scan r]
            assert_equal [llength $scanned] 10 \
                "DB$db scan returned [llength $scanned] keys, expected 10"
            # Every key must belong to this db.
            foreach k $scanned {
                assert_match "db${db}-key*" $k
            }
        }

        for {set db 0} {$db < 3} {incr db} {
            r select $db ; r flushdb
        }
        r select 0
    }

    test {metascan scan with count=1 step-by-step stays within DB boundary} {
        # Use count=1 to force many RocksDB iterations; a buggy end-bound would
        # cause the scan to step into the next DB's range.
        r select 0
        r flushdb
        foreach k {p q r s} { r set $k $k }
        r swap.evict p q r s
        foreach k {p q r s} { wait_key_cold r $k }

        r select 1
        r flushdb
        foreach k {w x y z} { r set $k $k }
        r swap.evict w x y z
        foreach k {w x y z} { wait_key_cold r $k }

        # Drive DB0 scan with step count=1 until done.
        r select 0
        set cursor 0
        set found {}
        set iterations 0
        while 1 {
            set res [r scan $cursor count 1]
            set cursor [lindex $res 0]
            lappend found {*}[lindex $res 1]
            incr iterations
            if {$cursor == 0} break
            # Safety valve: should finish in well under 100 steps for 4 keys.
            if {$iterations > 100} {
                fail "DB0 scan did not terminate within 100 iterations"
            }
        }
        assert_equal [lsort $found] {p q r s}

        r select 0 ; r del p q r s
        r select 1 ; r del w x y z
        r select 0
    }

    test {metascan mixed hot and cold keys do not bleed across DBs} {
        r select 0
        r flushdb
        r set hot0 hotval      ;# stays hot
        r set cold0 coldval
        r swap.evict cold0
        wait_key_cold r cold0

        r select 1
        r flushdb
        r set hot1 hotval      ;# stays hot
        r set cold1 coldval
        r swap.evict cold1
        wait_key_cold r cold1

        r select 0
        set got [lsort [full_scan r]]
        assert_equal $got {cold0 hot0}

        r select 1
        set got [lsort [full_scan r]]
        assert_equal $got {cold1 hot1}

        r select 0 ; r flushdb
        r select 1 ; r flushdb
        r select 0
    }
}
