proc press_enter_to_continue {{message "Hit Enter to continue ==> "}} {
    puts -nonewline $message
    flush stdout
    gets stdin
}

proc start_run_load {host port seconds counter code} {
    set tclsh [info nameofexecutable]
    exec $tclsh tests/swap/helpers/gen_run_load.tcl $host $port $seconds $counter $::tls $::target_db $code &
}

proc get_info_property {r section line property} {
    set str [$r info $section]
    if {[regexp ".*${line}:\[^\r\n\]*${property}=(\[^,\r\n\]*).*" $str match submatch]} {
        set _ $submatch
    }
}

proc swap_object_property {str section property} {
    if {[regexp ".*${section}:\[^\r\n]*${property}=(\[^,\r\n\]*).*" $str match submatch]} {
        set _ $submatch
    } else {
        set _ ""
    }
}

proc keyspace_is_empty {r} {
    if {[regexp ".*db0.*" [$r info keyspace] match]} {
        set _ 0
    } else {
        set _ 1
    }
}

proc object_is_dirty {r key} {
    set str [$r swap object $key]
    if {[swap_object_property $str value dirty] == 1} {
        set _ 1
    } else {
        set _ 0
    }
}

proc object_is_cold {r key} {
    set str [$r swap object $key]
    if { [swap_object_property $str value at] == "" && [swap_object_property $str cold_meta object_type] != "" } {
        set _ 1
    } else {
        set _ 0
    }
}

proc object_is_warm {r key} {
    set str [$r swap object $key]
    if { [swap_object_property $str value at] != "" && [swap_object_property $str hot_meta object_type] != ""} {
        set _ 1
    } else {
        set _ 0
    }
}

proc wait_key_cold {r key} {
    wait_for_condition 50 40 {
        [object_is_cold $r $key]
    } else {
        fail "wait $key cold failed."
    }
}

proc keyspace_is_cold {r} {
    if {[get_info_property r keyspace db0 keys] == "0"} {
        set _ 1
    } else {
        set _ 0
    }
}

proc wait_keyspace_cold {r} {
    wait_for_condition 50 40 {
        [keyspace_is_cold $r]
    } else {
        fail "wait keyspace cold failed."
    }
}

proc wait_key_warm {r key} {
    wait_for_condition 50 40 {
        [object_is_warm $r $key]
    } else {
        fail "wait $key warm failed."
    }
}

proc object_meta_len {r key} {
    set str [$r swap object $key]
    set meta_len [swap_object_property $str hot_meta len]
    if {$meta_len == ""} {
        set meta_len [swap_object_property $str cold_meta len]
    }
    if {$meta_len != ""} {
        set _ $meta_len
    } else {
        set _ 0
    }
}

proc object_meta_version {r key} {
    if { [catch {$r swap object $key} e] } {
        set _ 0
    } else {
        set str [$r swap object $key]
        set meta_version [swap_object_property $str hot_meta version]
        if {$meta_version == ""} {
            set meta_version [swap_object_property $str cold_meta version]
        }
        if {$meta_version != ""} {
            set _ $meta_version
        } else {
            set _ 0
        }
    }
}

proc rio_get_meta {r key} {
    lindex [$r swap rio-get meta [$r swap encode-meta-key $key ]] 0
}

proc rio_get_data {r key version subkey} {
    lindex [$r swap rio-get data [$r swap encode-data-key $key $version $subkey]] 0
}

proc get_info {r section line} {
    set str [$r info $section]
    if {[regexp ".*${line}:(\[^\r\n\]*)\r\n" $str match submatch]} {
        set _ $submatch
    }
}
