proc restart_server_gtided {level wait_ready rotate_logs gtid_enabled {reconnect 1} {shutdown sigterm}} {
    set srv [lindex $::servers end+$level]
    if {$shutdown ne {sigterm}} {
        catch {[dict get $srv "client"] shutdown $shutdown}
    }
    # Kill server doesn't mind if the server is already dead
    kill_server $srv
    # Remove the default client from the server
    dict unset srv "client"

    set pid [dict get $srv "pid"]
    set stdout [dict get $srv "stdout"]
    set stderr [dict get $srv "stderr"]
    if {$rotate_logs} {
        set ts [clock format [clock seconds] -format %y%m%d%H%M%S]
        file rename $stdout $stdout.$ts.$pid
        file rename $stderr $stderr.$ts.$pid
    }
    set prev_ready_count [count_message_lines $stdout "Ready to accept"]

    # if we're inside a test, write the test name to the server log file
    if {[info exists ::cur_test]} {
        set fd [open $stdout "a+"]
        puts $fd "### Restarting server for test $::cur_test"
        close $fd
    }

    set config_file [dict get $srv "config_file"]
    set fileId [open $config_file "a"]
    puts $fileId [format "\ngtid-enabled %s"  $gtid_enabled]
    flush $fileId
    close $fileId

    set pid [spawn_server $config_file $stdout $stderr {}]
    
    # check that the server actually started
    wait_server_started $config_file $stdout $pid

    # update the pid in the servers list
    dict set srv "pid" $pid
    # re-set $srv in the servers list
    lset ::servers end+$level $srv

    if {$wait_ready} {
        while 1 {
            # check that the server actually started and is ready for connections
            if {[count_message_lines $stdout "Ready to accept"] > $prev_ready_count} {
                break
            }
            after 10
        }
    }
    if {$reconnect} {
        reconnect $level
        # In swap mode, restarting loads a heavier RDB (includes RocksDB
        # snapshot data), so wait until loading completes before returning.
        if {$::swap} {
            wait_done_loading [srv $level client]
        }
    }
}


# master psync, slave psync 
proc restart_test {master_gtid_enabled slave_gtid_enabled restat_master_gtid_enabled is_full_sync} {
    test [format "restart master (%s => %s) slave (%s)" $master_gtid_enabled $restat_master_gtid_enabled $slave_gtid_enabled ]] {
        start_server [list overrides [list "gtid-enabled" $slave_gtid_enabled]] {
            start_server [list overrides  [list "gtid-enabled" $master_gtid_enabled]] {
                set master_id 0 
                set master [srv 0 client]
                set master_host [srv 0 host]
                set master_port [srv 0 port]
            

                #@step1 master sync data to slave
                set slave [srv -1 client]
                set slave_host [srv -1 host]
                set slave_port [srv -1 port]
                $slave slaveof $master_host $master_port 
            
                wait_for_condition 50 100 {
                    [status $slave master_link_status] eq {up}
                } else {
                    fail "Replication not started."
                }

                # @step2 send command + bgsave
                $master debug set-active-expire 0
                for {set j 0} {$j < 16} {incr j} {
                    # $master select [expr $j%16]
                    $master select 0
                    $master set $j somevalue px 20
                }


                after 20
                # Wait until master has received ACK from replica. If the master thinks
                # that any replica is lagging when it shuts down, master would send
                # GETACK to the replicas, affecting the replication offset.
                set offset [status $master master_repl_offset]
                wait_for_condition 500 100 {
                    [string match "*slave0:*,offset=$offset,*" [$master info replication]] &&
                    $offset == [status $slave master_repl_offset] 
                } else {
                    fail "Replicas and master offsets were unable to match *exactly*."
                }

                set offset [status $master master_repl_offset]
                #@step3 save rdb 
                $master bgsave
                wait_for_condition 1000 10 {
                    [s rdb_bgsave_in_progress] eq 0
                } else {
                    fail "bgsave did not stop in time"
                }
                
                
                # @step4 master restart load rdb
                catch {
                    # Unlike the test above, here we use SIGTERM, which behaves
                    # differently compared to SHUTDOWN NOW if there are lagging
                    # replicas. This is just to increase coverage and let each test use
                    # a different shutdown approach. In this case there are no lagging
                    # replicas though.
                    restart_server_gtided 0 true false $restat_master_gtid_enabled
                    set master [srv 0 client]
                }  

                # In swap mode, restart loads a heavier RDB (RocksDB state).
                # wait_done_loading inside restart_server_gtided has only a 5s
                # timeout and is wrapped in catch, so add an explicit longer wait
                # here to guarantee the master is fully loaded before proceeding.
                if {$::swap} {
                    set master [srv 0 client]
                    wait_done_loading $master
                }

                #@step4 check slave sync is continue
                if $is_full_sync {
                    wait_for_condition 1000 10 {
                        [status $master sync_full] eq 1
                    } else {
                        puts [status $master sync_full]
                        fail "full sync fail"
                    }
                } else {
                    # assert {[status $master sync_partial_ok] eq 0}
                    wait_for_condition 1000 10 {
                        [status $master sync_partial_ok] eq 1
                    } else {
                        puts [status $master sync_partial_ok]
                        fail "partial sync fail"
                    }
                }

                # In swap mode with full sync, the slave loads a new RDB
                # (including RocksDB state) which can take longer than in mem
                # mode. Wait until the slave exits LOADING before checking dbsize,
                # because wait_for_condition propagates LOADING errors immediately
                # rather than retrying.
                if {$::swap && $is_full_sync} {
                    wait_done_loading $slave
                }

                wait_for_condition 1000 30 {
                    [$master dbsize] eq [$slave dbsize]
                    && [$slave dbsize] eq 0
                } else {
                    puts [$master dbsize]
                    puts [$slave dbsize]
                    fail "slave dbszie != 0"
                }
            
            }
        }
    }
    
}


# unchange gtid mode
test "unchange gtid mode" {
    restart_test "no" "no" "no" 0 
    restart_test "yes" "no" "yes" 0 
    restart_test "yes" "yes" "yes" 0 
    restart_test "no" "yes" "no" 0 
}


test "change gtid mode" {
    restart_test "no" "no" "yes" 0 
    restart_test "yes" "yes" "no" 1
    restart_test "yes" "no" "no" 1
    restart_test "no" "yes" "yes" 0
}
