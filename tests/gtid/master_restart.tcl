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
    }
}

# # master psync ， slave psync => continue
start_server {tags {"psync2 external:skip"} overrides {gtid-enabled no}} {
    start_server {tags {"psync2 external:skip"} overrides {gtid-enabled no}} {
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
        for {set j 0} {$j < 1024} {incr j} {
            $master select [expr $j%16]
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


        
        $master bgsave

        wait_for_condition 1000 10 {
            [s rdb_bgsave_in_progress] eq 0
        } else {
            fail "bgsave did not stop in time"
        }

        $master config resetstat
        # @step3 master restart
        catch {
            # Unlike the test above, here we use SIGTERM, which behaves
            # differently compared to SHUTDOWN NOW if there are lagging
            # replicas. This is just to increase coverage and let each test use
            # a different shutdown approach. In this case there are no lagging
            # replicas though.
            restart_server 0 true false
            set master [srv 0 client]
        } err 

        
        set offset [status $master master_repl_offset]
        wait_for_condition 500 100 {
            [string match "*slave0:*,offset=$offset,*" [$master info replication]] &&
            $offset == [status $slave master_repl_offset] 
        } else {
            fail "Replicas and master offsets were unable to match *exactly*."
        }

        #@step4 check slave sync is continue
        assert {[status $master sync_partial_ok] == 1}

        wait_for_condition 1000 10 {
            [$master dbsize] eq [$slave dbsize]
        } else {
            fail "sync fail"
        }

    }
}


# #master psync ， slave xsync => continue
start_server {tags {"psync2 external:skip"} overrides {gtid-enabled yes}} {
    start_server {tags {"psync2 external:skip"} overrides {gtid-enabled no}} {
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
        for {set j 0} {$j < 1024} {incr j} {
            $master select [expr $j%16]
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


        
        $master bgsave

        wait_for_condition 1000 10 {
            [s rdb_bgsave_in_progress] eq 0
        } else {
            fail "bgsave did not stop in time"
        }

        $master config resetstat
        # @step3 master restart
        catch {
            # Unlike the test above, here we use SIGTERM, which behaves
            # differently compared to SHUTDOWN NOW if there are lagging
            # replicas. This is just to increase coverage and let each test use
            # a different shutdown approach. In this case there are no lagging
            # replicas though.
            restart_server 0 true false
            set master [srv 0 client]
        } err 
        
        set offset [status $master master_repl_offset]
        wait_for_condition 500 100 {
            [string match "*slave0:*,offset=$offset,*" [$master info replication]] &&
            $offset == [status $slave master_repl_offset] 
        } else {
            fail "Replicas and master offsets were unable to match *exactly*."
        }

        #@step4 check slave sync is continue
        assert {[status $master sync_partial_ok] == 1}
        wait_for_condition 1000 10 {
            [$master dbsize] eq [$slave dbsize]
        } else {
            fail "sync fail"
        }

    }
}

# #master xsync ， slave xsync => continue
start_server {tags {"psync2 external:skip"} overrides {gtid-enabled yes}} {
    start_server {tags {"psync2 external:skip"} overrides {gtid-enabled yes}} {
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
        for {set j 0} {$j < 1024} {incr j} {
            $master select [expr $j%16]
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


        set repl [attach_to_replication_stream_on_connection -1]
        $master bgsave

        wait_for_condition 1000 10 {
            [s rdb_bgsave_in_progress] eq 0
        } else {
            fail "bgsave did not stop in time"
        }

        $master config resetstat
        # @step3 master restart
        catch {
            # Unlike the test above, here we use SIGTERM, which behaves
            # differently compared to SHUTDOWN NOW if there are lagging
            # replicas. This is just to increase coverage and let each test use
            # a different shutdown approach. In this case there are no lagging
            # replicas though.
            restart_server 0 true false
            set master [srv 0 client]
        } err 
        
        


        
        #@step4 check slave sync is continue
        assert {[status $master sync_partial_ok] eq 0}
        wait_for_condition 1000 10 {
            [status $master sync_partial_ok] eq 1
        } else {
            fail "sync fail"
        }

        


        wait_for_condition 1000 10 {
            [$master dbsize] eq [$slave dbsize]
        } else {
            fail "sync fail"
        }

    }
}


# master xsync (restart => xsync ） ， slave xsync => continue
start_server {tags {"psync2 external:skip"} overrides {gtid-enabled yes}} {
    start_server {tags {"psync2 external:skip"} overrides {gtid-enabled yes}} {
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
        for {set j 0} {$j < 1024} {incr j} {
            $master select [expr $j%16]
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


        set repl [attach_to_replication_stream_on_connection -1]
        $master bgsave

        wait_for_condition 1000 10 {
            [s rdb_bgsave_in_progress] eq 0
        } else {
            fail "bgsave did not stop in time"
        }

        # @step3 master restart
        catch {
            # Unlike the test above, here we use SIGTERM, which behaves
            # differently compared to SHUTDOWN NOW if there are lagging
            # replicas. This is just to increase coverage and let each test use
            # a different shutdown approach. In this case there are no lagging
            # replicas though.
            restart_server_gtided 0 true false "no"
            set master [srv 0 client]
        } err 
        
        


        
        

        #@step4 check slave sync is continue
        assert {[status $master sync_partial_ok] eq 0}
        wait_for_condition 1000 10 {
            [status $master sync_partial_ok] eq 1
        } else {
            fail "sync fail"
        }





        wait_for_condition 1000 10 {
            [$master dbsize] eq [$slave dbsize]
        } else {
            fail "sync fail"
        }

    }
}





# # master psync (restart => xsync ） ， slave psync => fullsync
start_server {tags {"psync2 external:skip"} overrides {gtid-enabled no}} {
    start_server {tags {"psync2 external:skip"} overrides {gtid-enabled no}} {
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
        for {set j 0} {$j < 1024} {incr j} {
            $master select [expr $j%16]
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


        set repl [attach_to_replication_stream_on_connection -1]
        $master bgsave

        wait_for_condition 1000 10 {
            [s rdb_bgsave_in_progress] eq 0
        } else {
            fail "bgsave did not stop in time"
        }
        # $master config set gtid-enabled yes 
        # $master config rewrite
        # @step3 master restart
        catch {
            # Unlike the test above, here we use SIGTERM, which behaves
            # differently compared to SHUTDOWN NOW if there are lagging
            # replicas. This is just to increase coverage and let each test use
            # a different shutdown approach. In this case there are no lagging
            # replicas though.
            restart_server_gtided 0 true false "yes"
            set master [srv 0 client]
        } err 
        
        


        set offset [status $master master_repl_offset]
        wait_for_condition 500 100 {
            [string match "*slave0:*,offset=$offset,*" [$master info replication]] &&
            $offset == [status $slave master_repl_offset] 
        } else {
            fail "Replicas and master offsets were unable to match *exactly*."
        }

        #@step4 check slave sync is continue
        assert {[status $master sync_partial_ok] == 1}
        
        #@step4 check slave sync is continue


        wait_for_condition 1000 10 {
            [$master dbsize] eq [$slave dbsize] &&
            [$slave dbsize] eq 0 
        } else {
            fail "sync fail"
        }

    }
}