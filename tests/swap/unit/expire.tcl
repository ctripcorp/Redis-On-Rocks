proc scan_all_keys {r} {
    set keys {}
    set cursor 0
    while {1} {
        set res [$r scan $cursor]
        set cursor [lindex $res 0]
        lappend keys {*}[lindex $res 1]
        if {$cursor == 0} {
            break
        }
    }
    set _ $keys
}
# start_server {tags {"scan all keys"}} {
    # for {set i 0} {$i < 30} {incr i} {
        # r set $i $i
    # }
    # wait_keyspace_cold r
    # puts "keys-1: [scan_all_keys r]"
    # puts "keys-2: [scan_all_keys r]"
    # press_enter_to_continue
# }

start_server {tags {"swap string"}} {
    r config set swap-debug-evict-keys 0
    test {swap out string} {
        r set k v
        r pexpire k 200
        assert_match "*keys=1,*" [r info keyspace]
        r swap.evict k
        wait_key_cold r k
        wait_for_condition 100 100 {
            [r get k] eq ""
        } else {
            fail "cold key with pexpire did not expire"
        }
    }

    test {scan trigger cold key expire} {
        set wait_cold_time 200
        r psetex foo $wait_cold_time bar
        r swap.evict foo
        wait_key_cold r foo
        assert_equal [scan_all_keys r] {foo}
        after [expr {$wait_cold_time*2}]
        assert_equal [scan_all_keys r] {}
        #puts [r config get port]
        #press_enter_to_continue
        catch {r swap object foo} err
        assert_match {*ERR no such key*} $err
    }
}

start_server {tags "expire"} {
    # control evict manually
    r config set swap-debug-evict-keys 0

    # TODO enable when active expire ready
    # test {cold key active expire} {
        # r psetex foo 100 bar
        # r swap.evict foo
        # after 400
        # assert_equal [r dbsize] 0
        # assert {[rio_get_meta r foo] == ""}
    # }

    test {cold key passive expire} {
        r debug set-active-expire 0
        r psetex foo 200 bar
        r swap.evict foo
        wait_for_condition 100 100 {
            [r ttl foo] == -2
        } else {
            fail "cold key did not expire passively"
        }
        assert {[rio_get_meta r foo] == ""}
        r debug set-active-expire 1
    }

    test {cold key expire scaned} {
        r debug set-active-expire 0
        r psetex foo 200 bar
        r swap.evict foo
        after 500
        set res [r scan 0]
        assert_equal [lindex $res 0] 1
        set res [r scan 1]
        assert_equal [llength [lindex $res 1]] 0
        r debug set-active-expire 1
    }

    test {hot key active expire} {
        r psetex foo 100 bar
        wait_for_condition 100 100 {
            [r dbsize] == 0
        } else {
            fail "hot key not expired by active expire"
        }
    }

    test {hot key(non-dirty) active expire} {
        r psetex foo 500 bar
        r swap.evict foo
        wait_key_cold r foo
        assert {[rio_get_meta r foo] != ""}
        assert_equal [r get foo] bar
        # wait for active expire cycle to process the now-hot key
        wait_for_condition 100 100 {
            [r dbsize] == 0
        } else {
            fail "hot key (non-dirty) not expired by active expire"
        }
        assert {[rio_get_meta r foo] == ""}
    }

    test {hot key passive expire} {
        r debug set-active-expire 0
        r psetex foo 200 bar
        wait_for_condition 100 100 {
            [r ttl foo] == -2
        } else {
            fail "hot key did not expire passively"
        }
        r debug set-active-expire 1
    }

    test {hot key(non-dirty) passive expire} {
        r debug set-active-expire 0

        r psetex foo 500 bar
        r swap.evict foo
        wait_key_cold r foo
        assert {[rio_get_meta r foo] != ""}
        assert_equal [r get foo] bar
        # trigger passive expire by polling ttl until key is gone
        wait_for_condition 100 100 {
            [r ttl foo] == -2
        } else {
            fail "hot key (non-dirty) did not expire passively"
        }
        assert_equal [r dbsize] 0
        assert {[rio_get_meta r foo] == ""}
        r debug set-active-expire 1
    }

    test {hot key expire scaned} {
        r debug set-active-expire 0
        r psetex foo 200 bar
        r swap.evict foo
        after 500
        set res [r scan 0]
        set next_cursor [lindex $res 0]
        assert_equal [llength [lindex $res 1]] 0
        set res [r scan $next_cursor]
        assert_equal [llength [lindex $res 1]] 0
        r debug set-active-expire 1
    }
}

# Guard tests for scanMetaExpireIfNeeded ordering regression (fixed in dfa99d68a).
# Bug: scanMetaExpireIfNeeded was placed AFTER stringmatchlen / type filters,
# so expired cold keys that didn't match MATCH or TYPE were never deleted.
# These tests must FAIL on pre-fix code and PASS on post-fix code.
start_server {tags "scanMetaExpireIfNeeded"} {
    r config set swap-debug-evict-keys 0

    test {scanMetaExpireIfNeeded: expired cold keys deleted even when MATCH does not match} {
        # Pre-fix: stringmatchlen ran first; non-matching keys skipped before
        # scanMetaExpireIfNeeded, so they were never submitted for deletion.
        r debug set-active-expire 0
        r flushall

        foreach key {expire:1 expire:2 expire:3} {
            r psetex $key 500 bar
            r swap.evict $key
            wait_key_cold r $key
        }
        after 600

        # Full scan with a pattern that matches NONE of the expired keys.
        set cursor 0
        while 1 {
            set res [r scan $cursor match other:*]
            set cursor [lindex $res 0]
            if {$cursor == 0} break
        }

        # scanMetaExpireIfNeeded must fire regardless of MATCH.
        wait_for_condition 100 100 {
            [r dbsize] eq 0
        } else {
            fail "Expired cold keys not deleted when MATCH pattern did not match"
        }
        foreach key {expire:1 expire:2 expire:3} {
            assert_equal [rio_get_meta r $key] {}
        }

        r debug set-active-expire 1
    }

    test {scanMetaExpireIfNeeded: expired cold keys deleted even when TYPE filter does not match} {
        # Pre-fix: type filter ran first; non-matching keys skipped before
        # scanMetaExpireIfNeeded, so they were never submitted for deletion.
        r debug set-active-expire 0
        r flushall

        # String keys will not match TYPE hash.
        foreach key {strexp:1 strexp:2 strexp:3} {
            r psetex $key 500 bar
            r swap.evict $key
            wait_key_cold r $key
        }
        after 600

        set cursor 0
        while 1 {
            set res [r scan $cursor type hash]
            set cursor [lindex $res 0]
            if {$cursor == 0} break
        }

        # scanMetaExpireIfNeeded must fire regardless of TYPE filter.
        wait_for_condition 100 100 {
            [r dbsize] eq 0
        } else {
            fail "Expired cold keys not deleted when TYPE filter did not match"
        }
        foreach key {strexp:1 strexp:2 strexp:3} {
            assert_equal [rio_get_meta r $key] {}
        }

        r debug set-active-expire 1
    }

    test {scanMetaExpireIfNeeded: expired cold keys not matching MATCH are also deleted} {
        # Same regression with a mixed key set: some keys match the pattern,
        # some do not.  Pre-fix: non-matching key (other:99) is never deleted.
        r debug set-active-expire 0
        r flushall

        foreach key {match:1 match:2 other:99} {
            r psetex $key 500 bar
            r swap.evict $key
            wait_key_cold r $key
        }
        after 600

        set cursor 0
        while 1 {
            set res [r scan $cursor match match:*]
            set cursor [lindex $res 0]
            assert_equal [llength [lindex $res 1]] 0
            if {$cursor == 0} break
        }

        # All three keys must be deleted, including other:99 which didn't match.
        wait_for_condition 100 100 {
            [r dbsize] eq 0
        } else {
            fail "Expired cold key (other:99) not deleted despite MATCH mismatch"
        }
        foreach key {match:1 match:2 other:99} {
            assert_equal [rio_get_meta r $key] {}
        }

        r debug set-active-expire 1
    }
}

start_server {tags "unlink cold string"} {
    test {swap out and unlink cold string} {
        r set k v
        after 999
        r swap.evict k
        after 999
        r unlink k
        after 10
        set swap_rio_GET [getInfoProperty [r info swap] swap_rio_GET]
        # puts $swap_rio_GET
        assert_equal [string match {*batch=0,count=0*} $swap_rio_GET] 1
        set swap_rio_DEL [getInfoProperty [r info swap] swap_rio_DEL]
        # puts $swap_rio_DEL
        assert_equal [string match {*batch=1,count=1*} $swap_rio_DEL] 1
    }
}