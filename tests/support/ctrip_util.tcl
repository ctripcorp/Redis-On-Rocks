proc get_gtid { r } {
    set result [dict create]
    if {[regexp "\r\nall:(.*?)\r\n" [{*}$r info gtid] _ value]} {
        set uuid_sets [split $value ","]
        foreach uuid_set $uuid_sets {
            set uuid_set [split $uuid_set ":"]
            set uuid [lindex $uuid_set 0]
            set value [lreplace $uuid_set 0 0]
            dict set result $uuid $value 
        }
    }
    return $result
}

# example  
#   a { A:1 }       b {A:1}        return 0
#   a { A:1,B:1}    b {A:1}        return 1
#   a { A:1}        b {A:1,B:1}    return 2
#   a { A:1 }       b {B:1}        return -1
proc gtid_cmp {a b} {
    set a_size [dict size $a]
    set b_size [dict size $b]
    set min $a
    set max $b
    if {$a_size == $b_size} {
        set result 0
    } elseif {$a_size > $b_size} {
        set result 1
        set min $b
        set max $a 
    } else {
        set result 2
    }
    dict for {key value} $min {
        if {[dict get $a $key] != $value} {
            set result -1
            return -1
        }
    }
    return $result
}

proc gtid_set_is_equal {repr1  repr2} {
    if {[join [lsort [split $repr1 ","]] ","] eq [join [lsort [split $repr2 ","]] ","]} {
        set _ 1
    } else {
        set _ 0
    }
}

proc wait_for_gtid_sync {r1 r2} {
    wait_for_condition 500 100 {
        [gtid_set_is_equal [status $r1 gtid_set] [status $r2 gtid_set] ]
    } else {
        puts "[$r1 config get port]"
        puts [$r1 info gtid]

        puts "[$r2 config get port]"
        puts [$r2 info gtid]

        press_enter_to_continue

        fail "gtid didn't sync in time"
    }
}
