proc log_file_matches {log pattern} {
    set fp [open $log r]
    set content [read $fp]
    close $fp
    string match $pattern $content
}

start_server {tags {"repl network"}} {
    set slave [srv 0 client]
    set slave_host [srv 0 host]
    set slave_port [srv 0 port]
    set slave_log [srv 0 stdout]
    start_server {} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        # Configure the master in order to hang waiting for the BGSAVE
        # operation, so that the slave remains in the handshake state.
        $master config set repl-diskless-sync yes
        $master config set repl-diskless-sync-delay 1000

        # Use a short replication timeout on the slave, so that if there
        # are no bugs the timeout is triggered in a reasonable amount
        # of time.
        $slave config set repl-timeout 5

        # Start the replication process...
        $slave slaveof $master_host $master_port

        test {Slave enters handshake} {
            wait_for_condition 50 1000 {
                [string match *handshake* [$slave role]]
            } else {
                fail "Replica does not enter handshake state"
            }
        }

        # But make the master unable to send
        # the periodic newlines to refresh the connection. The slave
        # should detect the timeout.
        $master debug sleep 10

        test {Slave is able to detect timeout during handshake} {
            wait_for_condition 50 1000 {
                [log_file_matches $slave_log "*Timeout connecting to the MASTER*"]
            } else {
                fail "Replica is not able to detect timeout"
            }
        }
    }
}

start_server {tags {"repl"}} {
    set A [srv 0 client]
    set A_host [srv 0 host]
    set A_port [srv 0 port]
    start_server {} {
        set B [srv 0 client]
        set B_host [srv 0 host]
        set B_port [srv 0 port]

        test {Set instance A as slave of B} {
            $A slaveof $B_host $B_port
            wait_for_condition 50 100 {
                [lindex [$A role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$A info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }
        }

        tags {memonly} {
        test {INCRBYFLOAT replication, should not remove expire} {
            r set test 1 EX 100
            r incrbyfloat test 0.1
            after 1000
            assert_equal [$A debug digest] [$B debug digest]
        }
        }

        test {GETSET replication} {
            $A config resetstat
            $A config set loglevel debug
            $B config set loglevel debug
            r set test foo
            assert_equal [r getset test bar] foo
            wait_for_condition 500 10 {
                [$A get test] eq "bar"
            } else {
                fail "getset wasn't propagated"
            }
            assert_equal [r set test vaz get] bar
            wait_for_condition 500 10 {
                [$A get test] eq "vaz"
            } else {
                fail "set get wasn't propagated"
            }
            assert_match {*calls=3,*} [cmdrstat set $A]
            assert_match {} [cmdrstat getset $A]
        }

        test {BRPOPLPUSH replication, when blocking against empty list} {
            $A config resetstat
            set rd [redis_deferring_client]
            $rd brpoplpush a b 5
            r lpush a foo
            wait_for_condition 50 100 {
                [$A debug digest] eq [$B debug digest]
            } else {
                fail "Master and replica have different digest: [$A debug digest] VS [$B debug digest]"
            }
            assert_match {*calls=1,*} [cmdrstat rpoplpush $A]
            assert_match {} [cmdrstat lmove $A]
        }

        tags {memonly} {
        test {BRPOPLPUSH replication, list exists} {
            $A config resetstat
            set rd [redis_deferring_client]
            r lpush c 1
            r lpush c 2
            r lpush c 3
            $rd brpoplpush c d 5
            after 1000
            assert_equal [$A debug digest] [$B debug digest]
            assert_match {*calls=1,*} [cmdrstat rpoplpush $A]
            assert_match {} [cmdrstat lmove $A]
        }

        foreach wherefrom {left right} {
            foreach whereto {left right} {
                test "BLMOVE ($wherefrom, $whereto) replication, when blocking against empty list" {
                    $A config resetstat
                    set rd [redis_deferring_client]
                    $rd blmove a b $wherefrom $whereto 5
                    r lpush a foo
                    wait_for_condition 50 100 {
                        [$A debug digest] eq [$B debug digest]
                    } else {
                        fail "Master and replica have different digest: [$A debug digest] VS [$B debug digest]"
                    }
                    assert_match {*calls=1,*} [cmdrstat lmove $A]
                    assert_match {} [cmdrstat rpoplpush $A]
                }

                test "BLMOVE ($wherefrom, $whereto) replication, list exists" {
                    $A config resetstat
                    set rd [redis_deferring_client]
                    r lpush c 1
                    r lpush c 2
                    r lpush c 3
                    $rd blmove c d $wherefrom $whereto 5
                    after 1000
                    assert_equal [$A debug digest] [$B debug digest]
                    assert_match {*calls=1,*} [cmdrstat lmove $A]
                    assert_match {} [cmdrstat rpoplpush $A]
                }
            }
        }
        }

        test {BLPOP followed by role change, issue #2473} {
            set rd [redis_deferring_client]
            $rd blpop foo 0 ; # Block while B is a master

            # Turn B into master of A
            $A slaveof no one
            $B slaveof $A_host $A_port
            wait_for_condition 50 100 {
                [lindex [$B role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$B info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }

            # Push elements into the "foo" list of the new replica.
            # If the client is still attached to the instance, we'll get
            # a desync between the two instances.
            $A rpush foo a b c
            after 100

            wait_for_condition 50 100 {
                [$A debug digest] eq [$B debug digest] &&
                [$A lrange foo 0 -1] eq {a b c} &&
                [$B lrange foo 0 -1] eq {a b c}
            } else {
                fail "Master and replica have different digest: [$A debug digest] VS [$B debug digest]"
            }
        }
    }
}

start_server {tags {"repl"}} {
    r set mykey foo

    start_server {} {
        test {Second server should have role master at first} {
            s role
        } {master}

        test {SLAVEOF should start with link status "down"} {
            r multi
            r slaveof [srv -1 host] [srv -1 port]
            r info replication
            r exec
        } {*master_link_status:down*}

        test {The role should immediately be changed to "replica"} {
            s role
        } {slave}

        wait_for_sync r
        test {Sync should have transferred keys from master} {
            r get mykey
        } {foo}

        test {The link status should be up} {
            s master_link_status
        } {up}

        test {SET on the master should immediately propagate} {
            r -1 set mykey bar

            wait_for_condition 500 100 {
                [r  0 get mykey] eq {bar}
            } else {
                fail "SET on master did not propagated on replica"
            }
        }

        test {FLUSHALL should replicate} {
            r -1 flushall
            if {$::valgrind} {after 2000}
            list [r -1 dbsize] [r 0 dbsize]
        } {0 0}

        test {ROLE in master reports master with a slave} {
            set res [r -1 role]
            lassign $res role offset slaves
            assert {$role eq {master}}
            assert {$offset > 0}
            assert {[llength $slaves] == 1}
            lassign [lindex $slaves 0] master_host master_port slave_offset
            assert {$slave_offset <= $offset}
        }

        test {ROLE in slave reports slave in connected state} {
            set res [r role]
            lassign $res role master_host master_port slave_state slave_offset
            assert {$role eq {slave}}
            assert {$slave_state eq {connected}}
        }
    }
}

tags {memonly} {
foreach mdl {no yes} {
    foreach sdl {disabled swapdb} {
        start_server {tags {"repl"}} {
            set master [srv 0 client]
            $master config set repl-diskless-sync $mdl
            $master config set repl-diskless-sync-delay 1
            set master_host [srv 0 host]
            set master_port [srv 0 port]
            set slaves {}
            start_server {} {
                lappend slaves [srv 0 client]
                start_server {} {
                    lappend slaves [srv 0 client]
                    start_server {} {
                        lappend slaves [srv 0 client]
                        test "Connect multiple replicas at the same time (issue #141), master diskless=$mdl, replica diskless=$sdl" {
                            # start load handles only inside the test, so that the test can be skipped
                            set load_handle0 [start_bg_complex_data $master_host $master_port 9 100000000]
                            set load_handle1 [start_bg_complex_data $master_host $master_port 10 100000000]
                            set load_handle2 [start_bg_complex_data $master_host $master_port 11 100000000]
                            set load_handle3 [start_write_load $master_host $master_port 8]
                            set load_handle4 [start_write_load $master_host $master_port 4]
                            after 5000 ;# wait for some data to accumulate so that we have RDB part for the fork

                            # Send SLAVEOF commands to slaves
                            [lindex $slaves 0] config set repl-diskless-load $sdl
                            [lindex $slaves 1] config set repl-diskless-load $sdl
                            [lindex $slaves 2] config set repl-diskless-load $sdl
                            [lindex $slaves 0] slaveof $master_host $master_port
                            [lindex $slaves 1] slaveof $master_host $master_port
                            [lindex $slaves 2] slaveof $master_host $master_port

                            # Wait for all the three slaves to reach the "online"
                            # state from the POV of the master.
                            set retry 500
                            while {$retry} {
                                set info [r -3 info]
                                if {[string match {*slave0:*state=online*slave1:*state=online*slave2:*state=online*} $info]} {
                                    break
                                } else {
                                    incr retry -1
                                    after 100
                                }
                            }
                            if {$retry == 0} {
                                error "assertion:Slaves not correctly synchronized"
                            }

                            # Wait that slaves acknowledge they are online so
                            # we are sure that DBSIZE and DEBUG DIGEST will not
                            # fail because of timing issues.
                            wait_for_condition 500 100 {
                                [lindex [[lindex $slaves 0] role] 3] eq {connected} &&
                                [lindex [[lindex $slaves 1] role] 3] eq {connected} &&
                                [lindex [[lindex $slaves 2] role] 3] eq {connected}
                            } else {
                                fail "Slaves still not connected after some time"
                            }

                            # Stop the write load
                            stop_bg_complex_data $load_handle0
                            stop_bg_complex_data $load_handle1
                            stop_bg_complex_data $load_handle2
                            stop_write_load $load_handle3
                            stop_write_load $load_handle4

                            # Make sure no more commands processed
                            wait_load_handlers_disconnected

                            wait_for_ofs_sync $master [lindex $slaves 0]
                            wait_for_ofs_sync $master [lindex $slaves 1]
                            wait_for_ofs_sync $master [lindex $slaves 2]

                            # Check digests
                            set digest [$master debug digest]
                            set digest0 [[lindex $slaves 0] debug digest]
                            set digest1 [[lindex $slaves 1] debug digest]
                            set digest2 [[lindex $slaves 2] debug digest]

                            assert {$digest ne 0000000000000000000000000000000000000000}
                            assert {$digest eq $digest0}
                            assert {$digest eq $digest1}
                            assert {$digest eq $digest2}
                        }
                   }
                }
            }
        }
    }
}
}

start_server {tags {"repl" "memonly"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    start_server {} {
        test "Master stream is correctly processed while the replica has a script in -BUSY state" {
            set load_handle0 [start_write_load $master_host $master_port 3]
            set slave [srv 0 client]
            $slave config set lua-time-limit 500
            $slave slaveof $master_host $master_port

            # Wait for the slave to be online
            wait_for_condition 500 100 {
                [lindex [$slave role] 3] eq {connected}
            } else {
                fail "Replica still not connected after some time"
            }

            # Wait some time to make sure the master is sending data
            # to the slave.
            after 5000

            # Stop the ability of the slave to process data by sendig
            # a script that will put it in BUSY state.
            $slave eval {for i=1,3000000000 do end} 0

            # Wait some time again so that more master stream will
            # be processed.
            after 2000

            # Stop the write load
            stop_write_load $load_handle0

            # number of keys
            wait_for_condition 50 100 {
                [$master debug digest] eq [$slave debug digest]
            } else {
                fail "Different datasets between replica and master"
            }
        }
    }
}

test {slave fails full sync and diskless load swapdb recovers it} {
    start_server {tags {"repl" "memonly"}} {
        set slave [srv 0 client]
        set slave_host [srv 0 host]
        set slave_port [srv 0 port]
        set slave_log [srv 0 stdout]
        start_server {} {
            set master [srv 0 client]
            set master_host [srv 0 host]
            set master_port [srv 0 port]

            # Put different data sets on the master and slave
            # we need to put large keys on the master since the slave replies to info only once in 2mb
            $slave debug populate 2000 slave 10
            $master debug populate 200 master 100000
            $master config set rdbcompression no

            # Set master and slave to use diskless replication
            $master config set repl-diskless-sync yes
            $master config set repl-diskless-sync-delay 0
            $slave config set repl-diskless-load swapdb

            # Set master with a slow rdb generation, so that we can easily disconnect it mid sync
            # 10ms per key, with 200 keys is 2 seconds
            $master config set rdb-key-save-delay 10000

            # Start the replication process...
            $slave slaveof $master_host $master_port

            # wait for the slave to start reading the rdb
            wait_for_condition 50 100 {
                [s -1 loading] eq 1
            } else {
                fail "Replica didn't get into loading mode"
            }

            # make sure that next sync will not start immediately so that we can catch the slave in betweeen syncs
            $master config set repl-diskless-sync-delay 5
            # for faster server shutdown, make rdb saving fast again (the fork is already uses the slow one)
            $master config set rdb-key-save-delay 0

            # waiting slave to do flushdb (key count drop)
            wait_for_condition 50 100 {
                2000 != [scan [regexp -inline {keys\=([\d]*)} [$slave info keyspace]] keys=%d]
            } else {
                fail "Replica didn't flush"
            }

            # make sure we're still loading
            assert_equal [s -1 loading] 1

            # kill the slave connection on the master
            set killed [$master client kill type slave]

            # wait for loading to stop (fail)
            wait_for_condition 50 100 {
                [s -1 loading] eq 0
            } else {
                fail "Replica didn't disconnect"
            }

            # make sure the original keys were restored
            assert_equal [$slave dbsize] 2000
        }
    }
}

test {diskless loading short read} {
    start_server {tags {"repl"}} {
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]
        start_server {} {
            set master [srv 0 client]
            set master_host [srv 0 host]
            set master_port [srv 0 port]

            # Set master and replica to use diskless replication
            $master config set repl-diskless-sync yes
            $master config set rdbcompression no
            $replica config set repl-diskless-load swapdb
            $master config set hz 500
            $replica config set hz 500
            $master config set dynamic-hz no
            $replica config set dynamic-hz no
            # Try to fill the master with all types of data types / encodings
            set start [clock clicks -milliseconds]
            for {set k 0} {$k < 3} {incr k} {
                for {set i 0} {$i < 10} {incr i} {
                    r set "$k int_$i" [expr {int(rand()*10000)}]
                    r expire "$k int_$i" [expr {int(rand()*10000)}]
                    r set "$k string_$i" [string repeat A [expr {int(rand()*1000000)}]]
                    r hset "$k hash_small" [string repeat A [expr {int(rand()*10)}]]  0[string repeat A [expr {int(rand()*10)}]]
                    r hset "$k hash_large" [string repeat A [expr {int(rand()*10000)}]] [string repeat A [expr {int(rand()*1000000)}]]
                    r sadd "$k set_small" [string repeat A [expr {int(rand()*10)}]]
                    r sadd "$k set_large" [string repeat A [expr {int(rand()*1000000)}]]
                    r zadd "$k zset_small" [expr {rand()}] [string repeat A [expr {int(rand()*10)}]]
                    r zadd "$k zset_large" [expr {rand()}] [string repeat A [expr {int(rand()*1000000)}]]
                    r lpush "$k list_small" [string repeat A [expr {int(rand()*10)}]]
                    r lpush "$k list_large" [string repeat A [expr {int(rand()*1000000)}]]
                    for {set j 0} {$j < 10} {incr j} {
                        r xadd "$k stream" * foo "asdf" bar "1234"
                    }
                    r xgroup create "$k stream" "mygroup_$i" 0
                    r xreadgroup GROUP "mygroup_$i" Alice COUNT 1 STREAMS "$k stream" >
                }
            }

            if {$::verbose} {
                set end [clock clicks -milliseconds]
                set duration [expr $end - $start]
                puts "filling took $duration ms (TODO: use pipeline)"
                set start [clock clicks -milliseconds]
            }

            # Start the replication process...
            set loglines [count_log_lines -1]
            $master config set repl-diskless-sync-delay 0
            $replica replicaof $master_host $master_port

            # kill the replication at various points
            set attempts 100
            if {$::accurate} { set attempts 500 }
            for {set i 0} {$i < $attempts} {incr i} {
                # wait for the replica to start reading the rdb
                # using the log file since the replica only responds to INFO once in 2mb
                set res [wait_for_log_messages -1 {"*Loading DB in memory*"} $loglines 2000 1]
                set loglines [lindex $res 1]

                # add some additional random sleep so that we kill the master on a different place each time
                after [expr {int(rand()*50)}]

                # kill the replica connection on the master
                set killed [$master client kill type replica]

                set res [wait_for_log_messages -1 {"*Internal error in RDB*" "*Finished with success*" "*Successful partial resynchronization*"} $loglines 1000 1]
                if {$::verbose} { puts $res }
                set log_text [lindex $res 0]
                set loglines [lindex $res 1]
                if {![string match "*Internal error in RDB*" $log_text]} {
                    # force the replica to try another full sync
                    $master multi
                    $master client kill type replica
                    $master set asdf asdf
                    # the side effect of resizing the backlog is that it is flushed (16k is the min size)
                    $master config set repl-backlog-size [expr {16384 + $i}]
                    $master exec
                }
                # wait for loading to stop (fail)
                wait_for_condition 1000 1 {
                    [s -1 loading] eq 0
                } else {
                    fail "Replica didn't disconnect"
                }
            }
            if {$::verbose} {
                set end [clock clicks -milliseconds]
                set duration [expr $end - $start]
                puts "test took $duration ms"
            }
            # enable fast shutdown
            $master config set rdb-key-save-delay 0
        }
    }
}

# get current stime and utime metrics for a thread (since it's creation)
proc get_cpu_metrics { statfile } {
    if { [ catch {
        set fid   [ open $statfile r ]
        set data  [ read $fid 1024 ]
        ::close $fid
        set data  [ split $data ]

        ;## number of jiffies it has been scheduled...
        set utime [ lindex $data 13 ]
        set stime [ lindex $data 14 ]
    } err ] } {
        error "assertion:can't parse /proc: $err"
    }
    set mstime [clock milliseconds]
    return [ list $mstime $utime $stime ]
}

# compute %utime and %stime of a thread between two measurements
proc compute_cpu_usage {start end} {
    set clock_ticks [exec getconf CLK_TCK]
    # convert ms time to jiffies and calc delta
    set dtime [ expr { ([lindex $end 0] - [lindex $start 0]) * double($clock_ticks) / 1000 } ]
    set utime [ expr { [lindex $end 1] - [lindex $start 1] } ]
    set stime [ expr { [lindex $end 2] - [lindex $start 2] } ]
    set pucpu  [ expr { ($utime / $dtime) * 100 } ]
    set pscpu  [ expr { ($stime / $dtime) * 100 } ]
    return [ list $pucpu $pscpu ]
}


# test diskless rdb pipe with multiple replicas, which may drop half way
start_server {tags {"repl" "memonly"}} {
    set master [srv 0 client]
    $master config set repl-diskless-sync yes
    $master config set repl-diskless-sync-delay 1
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set master_pid [srv 0 pid]
    # put enough data in the db that the rdb file will be bigger than the socket buffers
    # and since we'll have key-load-delay of 100, 20000 keys will take at least 2 seconds
    # we also need the replica to process requests during transfer (which it does only once in 2mb)
    $master debug populate 20000 test 10000
    $master config set rdbcompression no
    # If running on Linux, we also measure utime/stime to detect possible I/O handling issues
    set os [catch {exec uname}]
    set measure_time [expr {$os == "Linux"} ? 1 : 0]
    foreach all_drop {no slow fast all timeout} {
        test "diskless $all_drop replicas drop during rdb pipe" {
            set replicas {}
            set replicas_alive {}
            # start one replica that will read the rdb fast, and one that will be slow
            start_server {} {
                lappend replicas [srv 0 client]
                lappend replicas_alive [srv 0 client]
                start_server {} {
                    lappend replicas [srv 0 client]
                    lappend replicas_alive [srv 0 client]

                    # start replication
                    # it's enough for just one replica to be slow, and have it's write handler enabled
                    # so that the whole rdb generation process is bound to that
                    set loglines [count_log_lines -1]
                    [lindex $replicas 0] config set repl-diskless-load swapdb
                    [lindex $replicas 0] config set key-load-delay 100 ;# 20k keys and 100 microseconds sleep means at least 2 seconds
                    [lindex $replicas 0] replicaof $master_host $master_port
                    [lindex $replicas 1] replicaof $master_host $master_port

                    # wait for the replicas to start reading the rdb
                    # using the log file since the replica only responds to INFO once in 2mb
                    wait_for_log_messages -1 {"*Loading DB in memory*"} $loglines 800 10

                    if {$measure_time} {
                        set master_statfile "/proc/$master_pid/stat"
                        set master_start_metrics [get_cpu_metrics $master_statfile]
                        set start_time [clock seconds]
                    }

                    # wait a while so that the pipe socket writer will be
                    # blocked on write (since replica 0 is slow to read from the socket)
                    after 500

                    # add some command to be present in the command stream after the rdb.
                    $master incr $all_drop

                    # disconnect replicas depending on the current test
                    set loglines [count_log_lines -2]
                    if {$all_drop == "all" || $all_drop == "fast"} {
                        exec kill [srv 0 pid]
                        set replicas_alive [lreplace $replicas_alive 1 1]
                    }
                    if {$all_drop == "all" || $all_drop == "slow"} {
                        exec kill [srv -1 pid]
                        set replicas_alive [lreplace $replicas_alive 0 0]
                    }
                    if {$all_drop == "timeout"} {
                        $master config set repl-timeout 2
                        # we want the slow replica to hang on a key for very long so it'll reach repl-timeout
                        exec kill -SIGSTOP [srv -1 pid]
                        after 2000
                    }

                    # wait for rdb child to exit
                    wait_for_condition 500 100 {
                        [s -2 rdb_bgsave_in_progress] == 0
                    } else {
                        fail "rdb child didn't terminate"
                    }

                    # make sure we got what we were aiming for, by looking for the message in the log file
                    if {$all_drop == "all"} {
                        wait_for_log_messages -2 {"*Diskless rdb transfer, last replica dropped, killing fork child*"} $loglines 1 1
                    }
                    if {$all_drop == "no"} {
                        wait_for_log_messages -2 {"*Diskless rdb transfer, done reading from pipe, 2 replicas still up*"} $loglines 1 1
                    }
                    if {$all_drop == "slow" || $all_drop == "fast"} {
                        wait_for_log_messages -2 {"*Diskless rdb transfer, done reading from pipe, 1 replicas still up*"} $loglines 1 1
                    }
                    if {$all_drop == "timeout"} {
                        wait_for_log_messages -2 {"*Disconnecting timedout replica (full sync)*"} $loglines 1 1
                        wait_for_log_messages -2 {"*Diskless rdb transfer, done reading from pipe, 1 replicas still up*"} $loglines 1 1
                        # master disconnected the slow replica, remove from array
                        set replicas_alive [lreplace $replicas_alive 0 0]
                        # release it
                        exec kill -SIGCONT [srv -1 pid]
                    }

                    # make sure we don't have a busy loop going thought epoll_wait
                    if {$measure_time} {
                        set master_end_metrics [get_cpu_metrics $master_statfile]
                        set time_elapsed [expr {[clock seconds]-$start_time}]
                        set master_cpu [compute_cpu_usage $master_start_metrics $master_end_metrics]
                        set master_utime [lindex $master_cpu 0]
                        set master_stime [lindex $master_cpu 1]
                        if {$::verbose} {
                            puts "elapsed: $time_elapsed"
                            puts "master utime: $master_utime"
                            puts "master stime: $master_stime"
                        }
                        if {!$::no_latency && ($all_drop == "all" || $all_drop == "slow" || $all_drop == "timeout")} {
                            assert {$master_utime < 70}
                            assert {$master_stime < 70}
                        }
                        if {!$::no_latency && ($all_drop == "none" || $all_drop == "fast")} {
                            assert {$master_utime < 15}
                            assert {$master_stime < 15}
                        }
                    }

                    # verify the data integrity
                    foreach replica $replicas_alive {
                        # Wait that replicas acknowledge they are online so
                        # we are sure that DBSIZE and DEBUG DIGEST will not
                        # fail because of timing issues.
                        wait_for_condition 150 100 {
                            [lindex [$replica role] 3] eq {connected}
                        } else {
                            fail "replicas still not connected after some time"
                        }

                        # Make sure that replicas and master have same
                        # number of keys
                        wait_for_condition 50 100 {
                            [$master dbsize] == [$replica dbsize]
                        } else {
                            fail "Different number of keys between master and replicas after too long time."
                        }

                        # Check digests
                        set digest [$master debug digest]
                        set digest0 [$replica debug digest]
                        assert {$digest ne 0000000000000000000000000000000000000000}
                        assert {$digest eq $digest0}
                    }
                }
            }
        }
    }
}

test "diskless replication child being killed is collected" {
    # when diskless master is waiting for the replica to become writable
    # it removes the read event from the rdb pipe so if the child gets killed
    # the replica will hung. and the master may not collect the pid with waitpid
    start_server {tags {"repl" "memonly"}} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set master_pid [srv 0 pid]
        $master config set repl-diskless-sync yes
        $master config set repl-diskless-sync-delay 0
        # put enough data in the db that the rdb file will be bigger than the socket buffers
        $master debug populate 20000 test 10000
        $master config set rdbcompression no
        start_server {} {
            set replica [srv 0 client]
            set loglines [count_log_lines 0]
            $replica config set repl-diskless-load swapdb
            $replica config set key-load-delay 1000000
            $replica replicaof $master_host $master_port

            # wait for the replicas to start reading the rdb
            wait_for_log_messages 0 {"*Loading DB in memory*"} $loglines 800 10

            # wait to be sure the eplica is hung and the master is blocked on write
            after 500

            # simulate the OOM killer or anyone else kills the child
            set fork_child_pid [get_child_pid -1]
            exec kill -9 $fork_child_pid

            # wait for the parent to notice the child have exited
            wait_for_condition 50 100 {
                [s -1 rdb_bgsave_in_progress] == 0
            } else {
                fail "rdb child didn't terminate"
            }
        }
    }
}

test "diskless replication read pipe cleanup" {
    # In diskless replication, we create a read pipe for the RDB, between the child and the parent.
    # When we close this pipe (fd), the read handler also needs to be removed from the event loop (if it still registered).
    # Otherwise, next time we will use the same fd, the registration will be fail (panic), because
    # we will use EPOLL_CTL_MOD (the fd still register in the event loop), on fd that already removed from epoll_ctl
    start_server {tags {"repl" "memonly"}} {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set master_pid [srv 0 pid]
        $master config set repl-diskless-sync yes
        $master config set repl-diskless-sync-delay 0

        # put enough data in the db, and slowdown the save, to keep the parent busy at the read process
        $master config set rdb-key-save-delay 100000
        $master debug populate 20000 test 10000
        $master config set rdbcompression no
        start_server {} {
            set replica [srv 0 client]
            set loglines [count_log_lines 0]
            $replica config set repl-diskless-load swapdb
            $replica replicaof $master_host $master_port

            # wait for the replicas to start reading the rdb
            wait_for_log_messages 0 {"*Loading DB in memory*"} $loglines 800 10

            set loglines [count_log_lines 0]
            # send FLUSHALL so the RDB child will be killed
            $master flushall

            # wait for another RDB child process to be started
            wait_for_log_messages -1 {"*Background RDB transfer started by pid*"} $loglines 800 10

            # make sure master is alive
            $master ping
        }
    }
}

test {replicaof right after disconnection} {
    # this is a rare race condition that was reproduced sporadically by the psync2 unit.
    # see details in #7205
    start_server {tags {"repl"}} {
        set replica1 [srv 0 client]
        set replica1_host [srv 0 host]
        set replica1_port [srv 0 port]
        set replica1_log [srv 0 stdout]
        start_server {} {
            set replica2 [srv 0 client]
            set replica2_host [srv 0 host]
            set replica2_port [srv 0 port]
            set replica2_log [srv 0 stdout]
            start_server {} {
                set master [srv 0 client]
                set master_host [srv 0 host]
                set master_port [srv 0 port]
                $replica1 replicaof $master_host $master_port
                $replica2 replicaof $master_host $master_port

                wait_for_condition 50 100 {
                    [string match {*master_link_status:up*} [$replica1 info replication]] &&
                    [string match {*master_link_status:up*} [$replica2 info replication]]
                } else {
                    fail "Can't turn the instance into a replica"
                }

                set rd [redis_deferring_client -1]
                $rd debug sleep 1
                after 100

                # when replica2 will wake up from the sleep it will find both disconnection
                # from it's master and also a replicaof command at the same event loop
                $master client kill type replica
                $replica2 replicaof $replica1_host $replica1_port
                $rd read

                wait_for_condition 50 100 {
                    [string match {*master_link_status:up*} [$replica2 info replication]]
                } else {
                    fail "role change failed."
                }

                # make sure psync succeeded, and there were no unexpected full syncs.
                assert_equal [status $master sync_full] 2
                assert_equal [status $replica1 sync_full] 0
                assert_equal [status $replica2 sync_full] 0
            }
        }
    }
}

test {Kill rdb child process if its dumping RDB is not useful} {
    start_server {tags {"repl" "memonly"}} {
        set slave1 [srv 0 client]
        start_server {} {
            set slave2 [srv 0 client]
            start_server {} {
                set master [srv 0 client]
                set master_host [srv 0 host]
                set master_port [srv 0 port]
                for {set i 0} {$i < 10} {incr i} {
                    $master set $i $i
                }
                # Generating RDB will cost 10s(10 * 1s)
                $master config set rdb-key-save-delay 1000000
                $master config set repl-diskless-sync no
                $master config set save ""

                $slave1 slaveof $master_host $master_port
                $slave2 slaveof $master_host $master_port

                # Wait for starting child
                wait_for_condition 50 100 {
                    ([s 0 rdb_bgsave_in_progress] == 1) &&
                    ([string match "*wait_bgsave*" [s 0 slave0]]) &&
                    ([string match "*wait_bgsave*" [s 0 slave1]])
                } else {
                    fail "rdb child didn't start"
                }

                # Slave1 disconnect with master
                $slave1 slaveof no one
                # Shouldn't kill child since another slave wait for rdb
                after 100
                assert {[s 0 rdb_bgsave_in_progress] == 1}

                # Slave2 disconnect with master
                $slave2 slaveof no one
                # Should kill child
                wait_for_condition 100 10 {
                    [s 0 rdb_bgsave_in_progress] eq 0
                } else {
                    fail "can't kill rdb child"
                }

                # If have save parameters, won't kill child
                $master config set save "900 1"
                $slave1 slaveof $master_host $master_port
                $slave2 slaveof $master_host $master_port
                wait_for_condition 50 100 {
                    ([s 0 rdb_bgsave_in_progress] == 1) &&
                    ([string match "*wait_bgsave*" [s 0 slave0]]) &&
                    ([string match "*wait_bgsave*" [s 0 slave1]])
                } else {
                    fail "rdb child didn't start"
                }
                $slave1 slaveof no one
                $slave2 slaveof no one
                after 200
                assert {[s 0 rdb_bgsave_in_progress] == 1}
                catch {$master shutdown nosave}
            }
        }
    }
}
