if {!$::swap} {
source tests/support/aofmanifest.tcl
set defaults {appendonly {yes} appendfilename {appendonly.aof} appenddirname {appendonlydir} auto-aof-rewrite-percentage {0}}
set server_path [tmpdir server.multi.aof]
set aof_dirname "appendonlydir"
set aof_basename "appendonly.aof"
set aof_dirpath "$server_path/$aof_dirname"
set aof_base1_file "$server_path/$aof_dirname/${aof_basename}.1$::base_aof_sufix$::aof_format_suffix"
set aof_base2_file "$server_path/$aof_dirname/${aof_basename}.2$::base_aof_sufix$::aof_format_suffix"
set aof_incr1_file "$server_path/$aof_dirname/${aof_basename}.1$::incr_aof_sufix$::aof_format_suffix"
set aof_incr2_file "$server_path/$aof_dirname/${aof_basename}.2$::incr_aof_sufix$::aof_format_suffix"
set aof_incr3_file "$server_path/$aof_dirname/${aof_basename}.3$::incr_aof_sufix$::aof_format_suffix"
set aof_manifest_file "$server_path/$aof_dirname/${aof_basename}$::manifest_suffix"
set aof_old_name_old_path "$server_path/$aof_basename"
set aof_old_name_new_path "$aof_dirpath/$aof_basename"
set aof_old_name_old_path2 "$server_path/${aof_basename}2"
set aof_manifest_file2 "$server_path/$aof_dirname/${aof_basename}2$::manifest_suffix"


test "aof" {
    proc read_command_arg {fd} {
        set len [::redis::redis_read_line $fd]
        set len [string range $len 1 end]
        set buf [gets $fd]
        while {[string length $buf] != $len} {
            set buf1 [gets $fd]
            set buf "$buf$buf1"
        }
        return $buf
    }
    proc read_command {fd} {
        set count [::redis::redis_read_line $fd]
        if {$count == 0} {
            return {}
        }
        set count [string range $count 1 end]
        set res {}
        for {set j 0} {$j < $count} {incr j} {
            set arg [read_command_arg $fd]
            if {$j == 0} {set arg [string tolower $arg]}
            lappend res $arg
        }
        return $res
    }
    proc try_read_aof {file line_count} {
        set try_num 3
        set res {}
        while {$try_num > 0} {
            set fd [open $file]
            puts $file
            set res {}
            set command [read_command $fd]
            while {$command != {}} {
                lappend res $command
                set command [read_command $fd]
            }
            close $fd
            if {[llength $res] == $line_count} {
                break
            }
            set try_num [expr {$try_num - 1}]
            after 1000
        }
        assert_equal [llength $res]  $line_count
        return $res
    }
    proc assert_aof {s patterns} {
        assert_equal [llength $s] [llength $patterns]
        for {set j 0} {$j < [llength $patterns]} {incr j} {
            assert_match [lindex $patterns $j] [lindex $s $j]
        }
    }
    test "save aof and reload aof" {
        
        start_server_aof [list dir $server_path aof-load-truncated yes gtid-enabled yes] {
            test "write expire command save to aof" {
                set client [redis [srv host] [srv port] 0 $::tls]
                $client set k1 v ex 1000
                $client set k2 v px 2000
                $client set k3 v
                $client expire k3 1000
                $client set k4 v
                $client pexpire k4 2000
                $client set k v
                # $client bgrewriteaof
                # waitForBgrewriteaof $client
                set config [srv config]
                set dir [dict get $config dir]
                set res [try_read_aof $dir/appendonlydir/appendonly.aof.1.incr.aof 8]
                assert_aof $res {
                    {select *}
                    {gtid * 0 SET k1 v PXAT *}
                    {gtid * 0 SET k2 v PXAT *}
                    {gtid * 0 set k3 v}
                    {gtid * 0 PEXPIREAT k3 *}
                    {gtid * 0 set k4 v}
                    {gtid * 0 PEXPIREAT k4 *}
                    {gtid * 0 set k v}
                }
            }
        }

        start_server_aof [list dir $server_path aof-load-truncated yes] {
            test "restart redis load aof" {
                set config [srv config]
                set dir [dict get $config dir]
                set res [try_read_aof  $dir/appendonlydir/appendonly.aof.1.incr.aof  8]
                assert_aof $res {
                    {select *}
                    {gtid * 0 SET k1 v PXAT *}
                    {gtid * 0 SET k2 v PXAT *}
                    {gtid * 0 set k3 v}
                    {gtid * 0 PEXPIREAT k3 *}
                    {gtid * 0 set k4 v}
                    {gtid * 0 PEXPIREAT k4 *}
                    {gtid * 0 set k v}
                }
                after 500
                set client [redis [ srv host] [srv port] 0 $::tls]
                assert_equal [$client get k] v
            }
        }
        create_aof $aof_dirpath $aof_incr1_file {
            append_to_aof [formatCommand gtid A:1 0 set k1 y]
            append_to_aof [formatCommand set k2 y]
            append_to_aof [formatCommand gtid A:2 0 PEXPIREAT k2 100000]
        }


        start_server_aof [list dir $server_path aof-load-truncated yes gtid-enabled yes] {
            test "Unfinished MULTI: Server should start if load-truncated is yes" {
                assert_equal 1 [is_alive [srv pid]]
                set client [redis [srv host] [srv port] 0 $::tls]
                assert_equal [$client get k1] y
                assert_equal [$client get k2] {}
            }
        }

        start_server [list overrides [list dir $server_path appendonly yes appendfilename appendonly.aof2 gtid-enabled yes]] {
            test {Redis should not try to convert DEL into EXPIREAT for EXPIRE -1} {
                r setex k1 10 y
                r set k2 y
                r expire k2 1000
                r set k3 y ex 1000
                r set k4 y
                r gtid A:1 9 expire k4 200000000000
                r set k5 y
                set config [srv 0 config]
                set dir [dict get $config dir]
                set res [try_read_aof $dir/appendonlydir/appendonly.aof2.1.incr.aof  8]
                assert_aof $res {
                    {select *}
                    {gtid * 9 SET k1 y PXAT *}
                    {gtid * 9 set k2 y}
                    {gtid * 9 PEXPIREAT k2 *}
                    {gtid * 9 SET k3 y PXAT *}
                    {gtid * 9 set k4 y}
                    {gtid * 9 expire k4 *}
                    {gtid * 9 set k5 y}
                }
            }
        }
    }
    
}
}