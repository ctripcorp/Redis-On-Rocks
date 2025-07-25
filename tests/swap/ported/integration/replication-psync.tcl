# Creates a master-slave pair and breaks the link continuously to force
# partial resyncs attempts, all this while flooding the master with
# write queries.
#
# You can specify backlog size, ttl, delay before reconnection, test duration
# in seconds, and an additional condition to verify at the end.
#
# If reconnect is > 0, the test actually try to break the connection and
# reconnect with the master, otherwise just the initial synchronization is
# checked for consistency.
proc test_psync {descr duration backlog_size backlog_ttl delay cond mdl sdl bgsave reconnect} {
    start_server {tags {"repl"}} {
        start_server {} {

            set master [srv -1 client]
            set master_host [srv -1 host]
            set master_port [srv -1 port]
            set slave [srv 0 client]

            $master config set save $bgsave
            $master config set repl-backlog-size $backlog_size
            $master config set repl-backlog-ttl $backlog_ttl
            $master config set repl-diskless-sync $mdl
            $master config set repl-diskless-sync-delay 1
            $master config set swap-debug-swapout-notify-delay-micro 10000
            $slave config set repl-diskless-load $sdl

            set load_handle0 [start_bg_complex_data $master_host $master_port 0 100000]
            set load_handle1 [start_bg_complex_data $master_host $master_port 0 100000]
            set load_handle2 [start_bg_complex_data $master_host $master_port 0 100000]
            test {Slave should be able to synchronize with the master} {
                $slave slaveof $master_host $master_port
                wait_for_condition 50 1000 {
                    [lindex [r role] 0] eq {slave} &&
                    [lindex [r role] 3] eq {connected}
                } else {
                    fail "Replication not started."
                }
            }

            # Check that the background clients are actually writing.
            test {Detect write load to master} {
                wait_for_condition 50 1000 {
                    [$master dbsize] > 100
                } else {
                    fail "Can't detect write load from background clients."
                }
            }

            test "Test replication partial resync: $descr (diskless: $mdl, $sdl, save: \{$bgsave\}, reconnect: $reconnect)" {
                # Now while the clients are writing data, break the maste-slave
                # link multiple times.
                if ($reconnect) {
                    for {set j 0} {$j < $duration*10} {incr j} {
                        after 100
                        # catch {puts "MASTER [$master dbsize] keys, REPLICA [$slave dbsize] keys"}

                        if {($j % 20) == 0} {
                            catch {
                                if {$delay} {
                                    $slave multi
                                    $slave client kill $master_host:$master_port
                                    $slave debug sleep $delay
                                    $slave exec
                                } else {
                                    $slave client kill $master_host:$master_port
                                }
                            }
                        }
                    }
                }
                stop_bg_complex_data $load_handle0
                stop_bg_complex_data $load_handle1
                stop_bg_complex_data $load_handle2

                # Wait for the slave to reach the "online"
                # state from the POV of the master.
                wait_slave_online $master 5000 100 {
                    error "assertion:Slave not correctly synchronized"
                }

                # Wait that slave acknowledge it is online so
                # we are sure that DBSIZE and DEBUG DIGEST will not
                # fail because of timing issues. (-LOADING error)
                wait_for_condition 5000 100 {
                    [lindex [$slave role] 3] eq {connected}
                } else {
                    fail "Slave still not connected after some time"
                }

                wait_for_condition 100 100 {
                    [$master dbsize] == [$slave dbsize]
                } else {
                    set csv1 [csvdump r]
                    set csv2 [csvdump {r -1}]
                    set fd [open /tmp/repldump1.txt w]
                    puts -nonewline $fd $csv1
                    close $fd
                    set fd [open /tmp/repldump2.txt w]
                    puts -nonewline $fd $csv2
                    close $fd
                    fail "Master - Replica inconsistency, Run diff -u against /tmp/repldump*.txt for more info"
                }
                if {[catch {swap_data_comp $master $slave } e]} {
                    puts $e
                    puts "master info replication: [$master info replication]"
                    puts "slave info replication: [$slave info replication]"
                    puts "try later in 5 seconds"
                    puts "master info replication: [$master info replication]"
                    puts "slave info replication: [$slave info replication]"
                    swap_data_comp $master $slave
                }

                assert {[$master dbsize] > 0}
                eval $cond
                after 1000; # wait a bit to aovid the listMetaCreate leak false alarm.
            }
        }
    }
}


foreach mdl {no yes} {
    foreach sdl {disabled swapdb} {
        foreach bgsave {"1 1" ""} {
            test_psync {no reconnection, just sync} 6 1000000 3600 0 {
            } $mdl $sdl $bgsave 0

            test_psync {ok psync} 6 100000000 3600 0 {
            assert {[s -1 sync_partial_ok] > 0}
            } $mdl $sdl $bgsave 1

            test_psync {no backlog} 6 100 3600 0.5 {
            assert {[s -1 sync_partial_err] > 0}
            } $mdl $sdl $bgsave 1

            test_psync {ok after delay} 3 100000000 3600 3 {
            assert {[s -1 sync_partial_ok] > 0}
            } $mdl $sdl $bgsave 1

            test_psync {backlog expired} 3 100000000 1 3 {
            assert {[s -1 sync_partial_err] > 0}
            } $mdl $sdl $bgsave 1
        }
    }
}
