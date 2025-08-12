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
    set r1 [redis [srv 0 "host"] [srv 0 "port"] 0 $::tls]
    if {!$::singledb} {
        $r1 select 9
    }
    test "thread up" {
        if {!$::external} {
            set lines [count_log_lines 0]
        }
        assert_equal [get_kv_value [get_info_field [r info threads] io_thread_0 ] clients] 2
        # threads 1 => 2 
        r config set io-threads 2
        for {set j 0} {$j < 2} {incr j} {
            assert_equal [r get k] v
            assert_equal [$r1 get k] v
        }
        
        wait_for_condition 100 50 {
            [get_kv_value [get_info_field [r info threads] io_thread_1 ] clients] eq 2
        } else {
            fail "thread up 1=>2 fail"
        }
        assert {[get_kv_value [get_info_field [r info threads] io_thread_1 ] writes] > 0}
        if {!$::external} {
            verify_log_message 0 "*IO threads scale-up end*" $lines
        }
        if {!$::external} {
            set lines [count_log_lines 0]
        }

        # threads 2 => 3
        r config set io-threads 3
        # client num < thread num
        wait_for_condition 10 50 {
            [get_info_field [r info threads] io_thread_2 ] ne ""
        } else {
            fail "thread down 2=>3 fail"
        }
        if {!$::external} {
            verify_log_message 0 "*IO threads scale-up end*" $lines
        }
    }

    test "thread down" {
        if {!$::external} {
            set lines [count_log_lines 0]
        }
        # threads 3 => 2
        r config set io-threads 2
        wait_for_condition 100 50 {
            [get_info_field [r info threads] io_thread_2 ] eq ""
        } else {
            fail "thread down 3 =>2 fail"
        }
        assert {[get_info_field [r info threads] io_thread_1 ] ne ""}
        if {!$::external} {
            verify_log_message 0 "*IO threads scale-down end*" $lines
        }

        # threads 2 => 1
        if {!$::external} {
            set lines [count_log_lines 0]
        }
        r config set io-threads 1
        wait_for_condition 100 50 {
            [get_info_field [r info threads] io_thread_1 ] eq ""
        } else {
            fail "thread down 2=>1 fail"
        }
        if {!$::external} {
            verify_log_message 0 "*IO threads scale-down end*" $lines
        }
    }
    set r2 [redis [srv 0 "host"] [srv 0 "port"] 0 $::tls]
    if {!$::singledb} {
        $r2 select 9
    }
    set r3 [redis [srv 0 "host"] [srv 0 "port"] 0 $::tls]
    if {!$::singledb} {
        $r3 select 9
    }
    test "again thread up and down" {
    
        assert_equal [get_kv_value [get_info_field [r info threads] io_thread_0 ] clients] 4
        # threads 1 => 3
        if {!$::external} {
            set lines [count_log_lines 0]
        }
        r config set io-threads 3
        after 1000
        wait_for_condition 100 50 {
            [get_info_field [r info threads] io_thread_2 ] ne ""
        } else {
            puts [r info threads]
            fail "thread up 1=>3 fail"
        }
        if {!$::external} {
            verify_log_message 0 "*IO threads scale-up end*" $lines
        }

        # threads 3 => 1
        if {!$::external} {
            set lines [count_log_lines 0]
        }
        r config set io-threads 1

        wait_for_condition 100 50 {
            [get_info_field [r info threads] io_thread_1 ] eq ""
        } else {
            fail "again thread 3=>1 down fail"
        }
        assert {[get_kv_value [get_info_field [r info threads] io_thread_0 ] clients] eq 4}
        if {!$::external} {
            verify_log_message 0 "*IO threads scale-down end*" $lines
        }
        assert {[get_kv_value [get_info_field [r info threads] io_thread_0 ] writes] > 0}

    }
}

