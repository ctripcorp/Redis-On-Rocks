# Minimal repro for "Master - Replica inconsistency" seen in replication-psync.tcl
# when replica uses repl-diskless-load=swapdb.
#
# This test aims to:
# - keep the original test intent (background writes + replication)
# - minimize matrix and runtime
# - reliably generate /tmp/repldump1.txt and /tmp/repldump2.txt on mismatch

proc scan_keys_all {r} {
    set keys {}
    set cursor 0
    while {1} {
        set res [{*}$r scan $cursor]
        set cursor [lindex $res 0]
        foreach k [lindex $res 1] {
            lappend keys $k
        }
        if {$cursor == 0} break
    }
    return [lsort $keys]
}

proc dump_keylist {r path} {
    set fd [open $path w]
    foreach k [scan_keys_all $r] {
        set t [{*}$r type $k]
        puts $fd "$k\t$t"
    }
    close $fd
}

start_server {tags {"repl"}} {
    start_server {} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]

        # Match the failing axis reported in replication-psync.tcl failures:
        # master diskless=no, replica diskless load=swapdb, save={1 1}
        $master config set save "1 1"
        $master config set repl-backlog-size 100000000
        $master config set repl-backlog-ttl 1
        $master config set repl-diskless-sync no
        $master config set repl-diskless-sync-delay 1
        $master config set swap-debug-swapout-notify-delay-micro 10000
        $slave config set repl-diskless-load swapdb

        # Background writers to master (increase pressure to reproduce).
        set load_handle0 [start_bg_complex_data $master_host $master_port 0 200000]
        set load_handle1 [start_bg_complex_data $master_host $master_port 0 200000]
        set load_handle2 [start_bg_complex_data $master_host $master_port 0 200000]

        test {repro swapdb: slave sync start} {
            $slave slaveof $master_host $master_port
            wait_for_condition 50 1000 {
                [lindex [$slave role] 0] eq {slave} &&
                [lindex [$slave role] 3] eq {connected}
            } else {
                fail "Replication not started."
            }
        }

        test {repro swapdb: detect load} {
            wait_for_condition 50 1000 {
                [$master dbsize] > 100
            } else {
                fail "Can't detect write load from background client."
            }
        }

        test {repro swapdb: stop load and compare datasets} {
            # Break link a few times to exercise cached_master/swapdb load paths.
            for {set j 0} {$j < 30} {incr j} {
                after 100
                if {($j % 10) == 0} {
                    catch {
                        $slave multi
                        $slave client kill $master_host:$master_port
                        $slave debug sleep 3
                        $slave exec
                    }
                }
            }

            stop_bg_complex_data $load_handle0
            stop_bg_complex_data $load_handle1
            stop_bg_complex_data $load_handle2

            wait_load_handlers_disconnected

            # Wait for master to see slave online.
            wait_slave_online $master 5000 100 {
                error "assertion:Slave not correctly synchronized"
            }

            # Wait slave connected and DB sizes converge.
            wait_for_condition 5000 100 {
                [lindex [$slave role] 3] eq {connected}
            } else {
                fail "Slave still not connected after some time"
            }

            # dbsize equality can be transient while replicated commands are
            # still in flight. Wait for repl offsets to converge first.
            wait_for_ofs_sync $master $slave

            wait_for_condition 200 100 {
                [dbsize_loadsafe $master master_dbsize] &&
                [dbsize_loadsafe $slave slave_dbsize] &&
                $master_dbsize == $slave_dbsize
            } else {
                # dump key lists (scan-based, works for cold keys too)
                dump_keylist $master /tmp/replkeys_master.txt
                dump_keylist $slave /tmp/replkeys_slave.txt
                fail "Master - Replica dbsize mismatch, keylists written to /tmp/replkeys_*.txt"
            }

            if {[catch {swap_data_comp $master $slave} e]} {
                puts $e
                puts "master info replication: [$master info replication]"
                puts "slave info replication: [$slave info replication]"
                puts "try later in 5 seconds"
                after 5000
                puts "master info replication: [$master info replication]"
                puts "slave info replication: [$slave info replication]"
                if {[catch {swap_data_comp $master $slave} retry_e]} {
                    puts $retry_e
                    dump_keylist $master /tmp/replkeys_master.txt
                    dump_keylist $slave /tmp/replkeys_slave.txt
                    fail "Master - Replica inconsistency, keylists written to /tmp/replkeys_*.txt"
                }
            }
        }
    }
}
