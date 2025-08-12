proc get_info_field {info field} {
    set fl [string length $field]
    append field :
    foreach line [split $info "\n"] {
        set line [string trim $line "\r\n "]
        if {[string range $line 0 $fl] eq $field} {
            return [string range $line [expr {$fl+1}] end]
        }
    }
    return {}
}

proc get_kv_value {input key} {
    foreach pair [split $input ","] {
        if {[regexp {^\s*([^=]+)\s*=\s*(.+?)\s*$} $pair -> k v]} {
            if {$k eq $key} {
                return $v
            }
        }
    }
    return ""
}

start_server {overrides {}} {
    r set k v 

    
    test "threads 1 => n and n => 1" {
        for {set thread_size 2} {$thread_size < 5} {incr thread_size} {
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "none"
            assert {[get_info_field [r info threads] io_thread_1 ] eq ""}
            # set io-threads n
            # when client size < thread size , thread scale up task finish
            if {!$::external} {
                set lines [count_log_lines 0]
            }
            assert_equal [get_kv_value [get_info_field [r info threads] io_thread_0 ] clients] 1
            r config set io-threads $thread_size 
            if {!$::external} {
                verify_log_message 0 "*IO threads scale-up end*" $lines
            }
            assert_equal [r get k] v 


            # reset io-threads 1
            if {!$::external} {
                set lines [count_log_lines 0]
            }
            r config set io-threads 1
            after 100
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "down"
            wait_for_condition 100 50 {
                [get_info_field [r info threads] io_thread_1 ] eq ""
            } else {
                fail "thread down n => 1 fail"
            }
            if {!$::external} {
                verify_log_message 0 "*IO threads scale-down end*" $lines
            }

            # add clients
            set clients []
            for {set j 0} {$j < 100} {incr j} {
                set cli [redis [srv 0 "host"] [srv 0 "port"] 0 $::tls]
                if {!$::singledb} {
                    $cli select 9
                }
                lappend clients $cli
            }
            assert_equal [get_kv_value [get_info_field [r info threads] io_thread_0 ] clients] 101
            # set io-threads n
            if {!$::external} {
                set lines [count_log_lines 0]
            }
            r config set io-threads $thread_size 

            if {!$::external} {
                verify_log_message 0 "*IO threads scale-up end*" $lines
            }
            assert_equal [r get k] v
            for {set j 0} {$j < 100} {incr j} {
                set cli [lindex $clients $j]
                assert_equal [$cli get k] v
            }


            # reset io-threads 1
            if {!$::external} {
                set lines [count_log_lines 0]
            }
            r config set io-threads 1
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "down"
            wait_for_condition 100 50 {
                [get_info_field [r info threads] io_thread_1 ] eq ""
            } else {
                fail "thread down n => 1 fail"
            }
            if {!$::external} {
                verify_log_message 0 "*IO threads scale-down end*" $lines
            }

            # close all clients
            for {set j 0} {$j < 100} {incr j} {
                set cli [lindex $clients $j]
                $cli close
            }
        }


    }
    
    # test before set io-threads 2
    r config set io-threads 2
    
    test "threads 2 => n and n => 2" {
        for {set thread_size 3} {$thread_size < 5} {incr thread_size} {
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "none"
            assert {[get_info_field [r info threads] io_thread_1 ] ne ""}
            assert {[get_info_field [r info threads] io_thread_2 ] eq ""}

            # set io-threads n
            # when client size < thread size , thread scale up task finish
            if {!$::external} {
                set lines [count_log_lines 0]
            }
            assert_equal [get_kv_value [get_info_field [r info threads] io_thread_1 ] clients] 1
            r config set io-threads $thread_size 
            if {!$::external} {
                verify_log_message 0 "*IO threads scale-up end*" $lines
            }


            # reset io-threads 2
            if {!$::external} {
                set lines [count_log_lines 0]
            }
            r config set io-threads 2
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "down"
            wait_for_condition 100 50 {
                [get_info_field [r info threads] io_thread_2 ] eq ""
            } else {
                fail "thread down n => 2 fail"
            }
            if {!$::external} {
                verify_log_message 0 "*IO threads scale-down end*" $lines
            }

            # add clients
            set clients []
            for {set j 0} {$j < 100} {incr j} {
                set cli [redis [srv 0 "host"] [srv 0 "port"] 0 $::tls]
                if {!$::singledb} {
                    $cli select 9
                }
                lappend clients $cli
            }
            assert_equal [get_kv_value [get_info_field [r info threads] io_thread_1 ] clients] 101

            # set io-threads n
            # wait CLIENT_IO_PENDING_CRON ,load balancing
            if {!$::external} {
                set lines [count_log_lines 0]
            }
            r config set io-threads $thread_size 
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "up"
            wait_for_condition 100 50 {
                [get_info_field [r info threads] io_thread_scale_status] eq  "none"
            } else {
                fail "thread up 2=>n fail"
            }

            if {!$::external} {
                assert {[catch {verify_log_message 0 "*IO threads scale-up client num(1)< thread num*" $lines} errorMsg]}
                assert {$errorMsg ne ""}
                verify_log_message 0 "*IO threads scale-up end*" $lines
            }

            # reset io-threads 2
            # wait CLIENT_IO_PENDING_CRON
            if {!$::external} {
                set lines [count_log_lines 0]
            }
            r config set io-threads 2
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "down"
            wait_for_condition 100 50 {
                [get_info_field [r info threads] io_thread_2 ] eq ""
            } else {
                fail "thread down n => 2 fail"
            }
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "none"
            if {!$::external} {
                verify_log_message 0 "*IO threads scale-down end*" $lines
            }

            # set io-threads n
            # client write, load balancing
            if {!$::external} {
                set lines [count_log_lines 0]
            }
            r config set io-threads $thread_size 
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "up"
            for {set j 0} {$j < 100} {incr j} {
                set cli [lindex $clients $j]
                assert_equal [$cli get k] v
            }
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "none"
            if {!$::external} {
                assert {[catch {verify_log_message 0 "*IO threads scale-up client num(1)< thread num*" $lines} errorMsg]}
                assert {$errorMsg ne ""}
                verify_log_message 0 "*IO threads scale-up end*" $lines
            }


            # reset io-threads 2
            # client write 
            if {!$::external} {
                set lines [count_log_lines 0]
            }
            r config set io-threads 2
            assert_equal [get_info_field [r info threads] io_thread_scale_status] "down"
            assert_equal [r get k] v 
            for {set j 0} {$j < 100} {incr j} {
                set cli [lindex $clients $j]
                assert_equal [$cli get k] v
            }

            set info [r info threads]
            if {[get_info_field $info io_thread_2] ne ""} {
                assert_equal [get_kv_value [get_info_field [r info threads] io_thread_2 ] clients] 0
                # need wait thread_join
                wait_for_condition 100 50 {
                    [get_info_field [r info threads] io_thread_scale_status] eq "none"
                } else {
                    fail "thread down n => 2 fail"
                }
            } else {
                assert_equal [get_info_field [r info threads] io_thread_scale_status] "none"
            }

            
            if {!$::external} {
                verify_log_message 0 "*IO threads scale-down end*" $lines
            }



            # close all clients
            for {set j 0} {$j < 100} {incr j} {
                set cli [lindex $clients $j]
                $cli close
            }

        }

    }
    

}