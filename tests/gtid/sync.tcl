# [functional testing]
# master slave sync
start_server {tags {"gtid replication"} overrides {gtid-enabled yes}} {
start_server {overrides {gtid-enabled yes}} {
    # Config
    set debug_msg 0                 ; # Enable additional debug messages

    set no_exit 0                   ; # Do not exit at end of the test

    set duration 20                 ; # Total test seconds
    for {set j 0} {$j < 2} {incr j} {
        set R($j) [srv [expr 0-$j] client]
        set R_host($j) [srv [expr 0-$j] host]
        set R_port($j) [srv [expr 0-$j] port]
        set R_unixsocket($j) [srv [expr 0-$j] unixsocket]
        if {$debug_msg} {puts "Log file: [srv [expr 0-$j] stdout]"}
    }
    test "REPLICATION" {
        test {GTID SET} {
            $R(0) set k v
            $R(1) slaveof $R_host(0) $R_port(0)
            wait_for_condition 50 1000 {
                [status $R(1) master_link_status] == "up" &&
                [dbsize_loadsafe $R(1) replica_dbsize] &&
                $replica_dbsize == 1
            } else {
                fail "Replicas not replicating from master"
            }
            # exclude the select command
            $R(0) set k v1
            set maxtries 3
            set result 0
            set backlog_size 58
            while {[incr maxtries -1] >= 0} {
                set before [status $R(0) master_repl_offset]
                $R(0) gtid A:1 $::target_db set x foobar
                set after [status $R(0) master_repl_offset]
                if {[expr $after-$before] == $backlog_size} {
                    set result 1
                    break
                }
            }
            assert_equal $result 1
        }
        test "SYNC GTID COMMAND" {
            set repl [attach_to_replication_stream]
            $R(0) gtid A:2 $::target_db set k v
            assert_replication_stream $repl {
                {select *}
                {gtid A:2 * set k v}
            }
        }
        test "SYNC SET=>GTID SET COMMAND " {
            set repl [attach_to_replication_stream]
            $R(0) set k v1
            assert_replication_stream $repl {
                {select *}
                {gtid * * set k v1}
            }
            assert_equal [$R(0) get k] v1
        }
        test "GTID MULTI " {
            wait_for_gtid_sync $R(0) $R(1)
            set repl [attach_to_replication_stream]
            $R(0) multi
            $R(0) set k v2
            $R(0) exec
            $R(0) set k v3
            assert_replication_stream $repl {
                {select *}
                {gtid * * set k v2}
                {gtid * * set k v3}
            }
            wait_for_gtid_sync $R(0) $R(1)
            $R(1) get k
        } {v3}
        test "GTID MULTI ERROR" {
            wait_for_gtid_sync $R(0) $R(1)
            set repl [attach_to_replication_stream]
            $R(0) multi
            $R(0) set k v4 k
            $R(0) set k v5
            catch {$R(0) exec } error
            $R(0) set k v6
            assert_replication_stream $repl {
                {select *}
                {gtid * * set k v5}
                {gtid * * set k v6}
            }
            wait_for_gtid_sync $R(0) $R(1)
            $R(1) get k
        } {v6}

        test "EXPIRE" {
            wait_for_gtid_sync $R(0) $R(1)
            set repl [attach_to_replication_stream]
            $R(0) setex k 1 v7
            after 1000
            assert_replication_stream $repl {
                {select *}
                {gtid * * SET k v7 PXAT *}
                {gtid * * DEL k}
            }
            wait_for_gtid_sync $R(0) $R(1)
            $R(1) get k
        } {}

        test "GTID with list arg rewrite" {
            wait_for_gtid_sync $R(0) $R(1)
            $R(0) MSET key1 val1 key2 val2
            $R(0) HMSET myhash f1 v1 f2 v2
            $R(0) RPUSH mylist a b c 1 2 3

            if {$::swap} {
                # list disabled untill 1.0.1
                catch { wait_keyspace_cold $R(0) }
            }

            set repl [attach_to_replication_stream]
            $R(0) multi
            $R(0) mget key1 key2
            $R(0) ltrim mylist 1 -2
            $R(0) hdel myhash f1 f2 f3
            $R(0) exec

            wait_for_ofs_sync $R(0) $R(1)

            assert_replication_stream $repl {
                {multi}
                {select *}
                {ltrim mylist 1 -2}
                {hdel myhash f1 f2 f3}
                {gtid * * EXEC}
            }

            assert_equal [$R(1) mget key1 key2] {val1 val2}
            assert_equal [$R(1) lrange mylist 0 -1] {b c 1 2}
            assert_equal [$R(1) hmget myhash f1 f2 f3] {{} {} {}}
        }
    }
}
}

# full sync test set
start_server {tags {"gtid replication"} overrides {gtid-enabled yes}} {
start_server {overrides {gtid-enabled yes}} {
     # Config
    set debug_msg 0                 ; # Enable additional debug messages

    set no_exit 0                   ; # Do not exit at end of the test

    set duration 20                 ; # Total test seconds
    for {set j 0} {$j < 2} {incr j} {
        set R($j) [srv [expr 0-$j] client]
        set R_host($j) [srv [expr 0-$j] host]
        set R_port($j) [srv [expr 0-$j] port]
        set R_unixsocket($j) [srv [expr 0-$j] unixsocket]
        if {$debug_msg} {puts "Log file: [srv [expr 0-$j] stdout]"}
    }
    test "REPLICATION" {
        test {FULL SYNC SET} {
            $R(0) set k v
            $R(1) slaveof $R_host(0) $R_port(0)
            wait_for_condition 50 1000 {
                [status $R(1) master_link_status] == "up" &&
                [dbsize_loadsafe $R(1) replica_dbsize] &&
                $replica_dbsize == 1
            } else {
                fail "Replicas not replicating from master"
            }
            wait_for_gtid_sync $R(0) $R(1)
            assert_equal [gtid_cmp [get_gtid $R(1)] [get_gtid $R(0)]] 0
            $R(0) set k1 v1
            wait_for_gtid_sync $R(0) $R(1)
            assert_equal [$R(1) get k1] v1
            assert_equal [gtid_cmp [get_gtid $R(1)] [get_gtid $R(0)]] 0
        }
    }
}
}


# partial sync test set
start_server {tags {"gtid replication"} overrides {gtid-enabled yes}} {
start_server {overrides {gtid-enabled yes}} {
     # Config
    set debug_msg 0                 ; # Enable additional debug messages

    set no_exit 0                   ; # Do not exit at end of the test

    set duration 20                 ; # Total test seconds
    for {set j 0} {$j < 2} {incr j} {
        set R($j) [srv [expr 0-$j] client]
        set R_host($j) [srv [expr 0-$j] host]
        set R_port($j) [srv [expr 0-$j] port]
        set R_unixsocket($j) [srv [expr 0-$j] unixsocket]
        if {$debug_msg} {puts "Log file: [srv [expr 0-$j] stdout]"}
    }
    test "REPLICATION" {
        test {PARTIAL SYNC SET} {
            $R(1) slaveof $R_host(0) $R_port(0)
            wait_for_condition 50 1000 {
                [status $R(1) master_link_status] == "up"
            } else {
                fail "Replicas not replicating from master"
            }
            $R(0) set k v
            wait_for_gtid_sync $R(0) $R(1)
            assert_equal [$R(1) get k] v
            assert_equal [gtid_cmp [get_gtid $R(1)] [get_gtid $R(0)]] 0
        }
    }
}
}
