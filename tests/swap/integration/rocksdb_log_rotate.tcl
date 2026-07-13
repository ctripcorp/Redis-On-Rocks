
proc do_fullsync {slave master_host master_port} {
    $slave slaveof no one
    wait_for_condition [repl_wait_maxtries 20] [repl_wait_delay] {
        [lindex [$slave role] 0] eq {master}
    } else {
        fail "replica didn't promote in time"
    }
    $slave set testing.fullsync 1
    $slave slaveof $master_host $master_port
    wait_for_sync $slave
}


proc count_old_logs {logs_dir} {
    return [llength [glob -nocomplain "$logs_dir/*_LOG.old.*"]]
}


proc get_log_file {logs_dir} {
    set list [glob -nocomplain "$logs_dir/*_LOG"]
    if {[llength $list] != 1} {
        return ""
    }
    return [lindex $list 0]
}


start_server {tags {"swap rocksdb-log-rotate"} overrides {
    swap-repl-rordb-sync yes
}} {
    start_server {overrides {
        swap-repl-rordb-sync yes
    }} {
        set master      [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set master_dir  [lindex [$master config get dir] 1]
        set slave       [srv 0 client]
        set slave_dir  [lindex [$slave config get dir] 1]


        set master_logs_dir  "$master_dir/logs.rocks"
        set slave_logs_dir  "$slave_dir/logs.rocks"

        $master mset k1 v1 k2 v2 k3 v3

        test {rocksdb log rotate: step1 ror fullsync logs.rocks/LOG exist} {
            $master set k1 v1
            $master set k2 v2

            $slave slaveof $master_host $master_port
            wait_for_sync $slave

            wait_for_condition 50 100 {
                [get_log_file $master_logs_dir] != ""
            } else {
                fail "full sync logs.rocks/LOG exist fail"
            }

            assert {[file size [get_log_file $master_logs_dir]] > 1}
            assert {[file size [get_log_file $slave_logs_dir]] > 1}
        }

        test {rocksdb log rotate:step2 ror fullsync slave LOG.old.*  count<=12} {
            

            for {set i 0} {$i < 13} {incr i} {
                do_fullsync $slave $master_host $master_port
                wait_for_condition 50 200 {
                    [status $master sync_full] == [expr {$i + 2}]
                } else {
                    fail "sync_full not updated"
                }
            }

            after 500

            assert {[get_log_file $master_logs_dir] != ""}

            assert {[file size [get_log_file $master_logs_dir]] > 1}
            assert {[count_old_logs $master_logs_dir] == 0}
            assert {[file size [get_log_file $slave_logs_dir]] > 1}
            assert {[count_old_logs $slave_logs_dir] == 11}
        }

        
    }
}


start_server {tags {"swap rocksdb-log-rotate rdb fullsync"} overrides {
    swap-repl-rordb-sync no
}} {
    start_server {overrides {
        swap-repl-rordb-sync no
    }} {
        set master      [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set master_dir  [lindex [$master config get dir] 1]
        set slave       [srv 0 client]
        set slave_dir  [lindex [$slave config get dir] 1]
        


        set master_logs_dir  "$master_dir/logs.rocks"
        set slave_logs_dir  "$slave_dir/logs.rocks"

        $master mset k1 v1 k2 v2 k3 v3

        test {rocksdb log rotate: step1 rdb fullsync logs.rocks/LOG exist} {
            $master set k1 v1
            $master set k2 v2

            $slave slaveof $master_host $master_port
            wait_for_sync $slave

            wait_for_condition 50 100 {
                [get_log_file $master_logs_dir] != ""
            } else {
                fail "full sync logs.rocks/LOG exist fail"
            }

            assert {[file size [get_log_file $master_logs_dir]] > 1}
            assert {[file size [get_log_file $slave_logs_dir]] > 1}
        }

        test {rocksdb log rotate:step2 rdb fullsync slave LOG.old.*  count == 0} {

            for {set i 0} {$i < 13} {incr i} {
                do_fullsync $slave $master_host $master_port
                wait_for_condition 50 200 {
                    [status $master sync_full] == [expr {$i + 2}]
                } else {
                    fail "sync_full not updated"
                }
            }

            assert {[get_log_file $master_logs_dir] != ""}

            assert {[file size [get_log_file $master_logs_dir]] > 1}
            assert {[count_old_logs $master_logs_dir] == 0}
            assert {[file size [get_log_file $slave_logs_dir]] > 1}
            assert {[count_old_logs $slave_logs_dir] == 0}
        }
    }
}
