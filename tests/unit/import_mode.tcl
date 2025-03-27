start_server {tags {"import mode"} overrides {}}  {
    test "import start end" {

        r import start
        assert_equal [r import status] {1}
        assert_range [r import get ttl] 3595 3600

        assert_equal [r import end] {OK}
        assert_equal [r import status] {0}

        r import start 120
        assert_equal [r import status] {1}
        assert_range [r import get ttl] 115 120

        r import end
        assert_equal [r import status] {0}

    }

    test "import mode restart" {

        r import start
        assert_equal [r import status] {1}

        # not yet ending, restarting directing 
        r import start 120
        assert_range [r import get ttl] 115 120

    }

    test "import set get " {

        r import start
        assert_equal [r import set ttl 300] {OK}
        assert_range [r import get ttl] 295 300

        assert_equal [r import get expire] {0}

        assert_equal [r import set expire 1] {OK}
        assert_equal [r import get expire] {1}

        # not ending, restart, options will be inherited
        r import start
        assert_equal [r import get expire] {1}

        # after ending, then starting, options are default value 
        r import end

        r import start
        assert_equal [r import get expire] {0}

        r import end
    }

    test "import passive-expire " {
        r flushdb

        r setex key1 1 k
        r setex key2 1 k
        r setex key3 1 k

        r import start


        after 1500

        assert_equal [r get key1] {}

        assert_equal [r dbsize] {3}

        assert_equal [r import set expire 1] {OK}

        assert_equal [r get key1] {}
        assert_equal [r get key2] {}
        assert_equal [r get key3] {}

        assert_equal [r dbsize] {0}

        assert_equal [r import set expire 0] {OK}

        r setex key1 1 k

        after 1500
        assert_equal [r get key1] {}

        assert_equal [r dbsize] {1}

        r import end
    }

    test "import active-expire " {
        r flushdb

        r import start

        assert_equal [r import set expire 1] {OK}
        assert_equal [r import get expire] {1}

        r setex key1 1 k
        r setex key2 1 k
        r setex key3 1 k

        after 1500

        assert_equal [r dbsize] {0}

        assert_equal [r get key1] {}

        assert_equal [r import set expire 0] {OK}

        r setex key1 1 k
        r setex key2 1 k
        r setex key3 1 k

        after 1500

        assert_equal [r get key1] {}

        assert_equal [r dbsize] {3}

        r import end
    }

    test "import end works " {
    
        r flushdb

        r setex key1 1 k
        r setex key2 1 k
        r setex key3 1 k

        r import start

        after 1500
        assert_equal [r dbsize] {3}

        r import end

        after 1500
        assert_equal [r dbsize] {0}
    }

    test "illegal operation " {

        r import start

        assert_equal [r import get expire] {0}

        assert_equal [r import set expire 1] {OK}

        assert_equal [r import set expire no] {OK}

        assert_equal [r import set evict normal] {OK}

        assert_error "*Invalid option*"  {r import get auto-compaction}

        assert_error "*not an integer*"  {r import set ttl no}

        assert_error "*Invalid option*"   {r import set auto-compaction ok}

        assert_error "*Invalid evicting policy*"   {r import set evict ok}

        r import end

        assert_error "*already ended*"   {r import end}

        assert_error "*IMPORT GET must be*"   {r import get expire}

        assert_error "*IMPORT SET must be*"   {r import set expire 1}

        assert_error "*not an integer*"   {r import start six}

        assert_error "*Invalid subcommand*"   {r import set auto-compaction ok no 1}
    }

    test "importing evict simply" {

        r flushdb
        r config set swap-debug-evict-keys 0
        r config set maxmemory-policy allkeys-lru

        set used [s used_memory]

        # set a key of 1000 bytes
        set buf [string repeat "abcde" 200]
        set limit [expr {$used + 1000 * 100}]

        # set the low limit to make sure importing evict happen
        r config set maxmemory $limit

        set numkeys 0
        while 1 {
            r set "key:$numkeys" $buf
            if {[s used_memory]+1000 > $limit} {
                break
            }
            incr numkeys
        }

        r import start

        for {set i 0} {$i < 200} {incr i} {
            r set "import_key:$i" $buf
        }

        after 100

        r import end

        set cold_cursor 0
        set hot_cursor 0

        for {set i 0} {$i < 200} {incr i} {
            if {[object_is_hot r "import_key:$i"]} {
                set cold_cursor [expr {$i - 1}]
                break
            }
        }

        assert_morethan_equal $cold_cursor 0

        for {set i 199} {$i > 0} {incr i -1} {
            if {[object_is_cold r "import_key:$i"]} {
                set hot_cursor [expr {$i + 1}]
                break
            }
        }

        assert_lessthan_equal $cold_cursor $hot_cursor
    }

    test "importing evict, open and close serveral times" {

        r flushdb
        r config set swap-debug-evict-keys 0
        r config set maxmemory-policy allkeys-lru

        set used [s used_memory]

        # set a key of 1000 bytes
        set buf [string repeat "abcde" 200]

        # set the low limit to make sure importing evict happen
        set limit [expr {$used + 1000 * 100}]
        r config set maxmemory $limit

        for {set i 0} {$i < 100} {incr i} {
            r set "key:$i" $buf
        }

        r import start

        for {set i 200} {$i < 300} {incr i} {
            r set "key:$i" $buf
        }

        r import end

        for {set i 100} {$i < 200} {incr i} {
            r set "key:$i" $buf
        }

        r import start

        for {set i 300} {$i < 400} {incr i} {
            r set "key:$i" $buf
        }
        
        after 500

        r import end

        set cold_cursor 0
        set hot_cursor 0

        for {set i 200} {$i < 400} {incr i} {
            if {[object_is_hot r "key:$i"]} {
                set cold_cursor [expr {$i - 1}]
                break
            }
        }

        assert_morethan_equal $cold_cursor 0

        for {set i 399} {$i > 200} {incr i -1} {
            if {[object_is_cold r "key:$i"]} {
                set hot_cursor [expr {$i + 1}]
                break
            }
        }

        assert_lessthan_equal $cold_cursor $hot_cursor

        assert_equal [r dbsize] 400
    }

    test "not reaching maxmemory during importing evict" {
 
        r flushdb
        r config set swap-debug-evict-keys 0
        r config set maxmemory-policy allkeys-lru

        set used [s used_memory]
        set limit [expr {$used+1000 * 10000000}]

        # set a key of 1000 bytes
        set buf [string repeat "abcde" 200]

        r config set maxmemory $limit
        
        r import start

        for {set i 0} {$i < 400} {incr i} {
            r set "import_key:$i" $buf
        }

        r import end

        if {[s used_memory]+1000 < $limit} {

            for {set i 0} {$i < 400} {incr i} {
                assert [object_is_hot r "import_key:$i"]
            }
        }
    }

    test "set evict normal" {

        r flushdb
        r config set swap-debug-evict-keys 0
        r config set maxmemory-policy allkeys-lru

        set used [s used_memory]

        # set a key of 1000 bytes
        set buf [string repeat "abcde" 200]
        set limit [expr {$used + 1000 * 100}]

        # set the low limit to make sure importing evict happen
        r config set maxmemory $limit

        set numkeys 0
        while 1 {
            r set "key:$numkeys" $buf
            if {[s used_memory]+1000 > $limit} {
                break
            }
            incr numkeys
        }

        r import start

        for {set i 0} {$i < 200} {incr i} {
            r set "import_key:$i" $buf
        }

        after 100

        r import set evict normal

        set cold_cursor 0
        set hot_cursor 0

        for {set i 0} {$i < 200} {incr i} {
            if {[object_is_hot r "import_key:$i"]} {
                set cold_cursor [expr {$i - 1}]
                break
            }
        }

        assert_morethan_equal $cold_cursor 0

        for {set i 199} {$i > 0} {incr i -1} {
            if {[object_is_cold r "import_key:$i"]} {
                set hot_cursor [expr {$i + 1}]
                break
            }
        }

        assert_lessthan_equal $cold_cursor $hot_cursor
    }
}