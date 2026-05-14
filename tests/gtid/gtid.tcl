start_server {tags {"gtid"} overrides {gtid-enabled yes}} {
    set master_repl [attach_to_replication_stream]
    set orig_db 0

    start_server {overrides {gtid-enabled yes}} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        $slave config set repl-rdb-channel no
        # Init replication link and and repl stream
        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        set slave_repl [attach_to_replication_stream]

        $master SET key val0
        wait_for_gtid_sync $master $slave
        assert_equal [$master GET key] val0
        assert_equal [$slave GET key] val0

        set myuuid [status $master gtid_uuid]
        set mygno  [status $master gtid_executed_gno_count]

        assert_replication_stream $master_repl [list {select *} "gtid $myuuid:$mygno * SET key val0"]
        assert_replication_stream $slave_repl  [list {select *} "gtid $myuuid:$mygno * SET key val0"]

        test "propagte repl: GTID-ENABLED(yes) TX(yes) CMD(write,may-replicate)" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master MULTI
            $master GET key
            $master SET key val1
            $master PUBLISH hello world
            $master SET key val2
            $master EXEC

            wait_for_gtid_sync $master $slave

            incr mygno
            set mygtidset "$myuuid:1-$mygno"

            assert_equal [$master GET key] val2
            assert_equal [$slave GET key] val2

            assert_replication_stream $master_repl [list multi {set key val1} {publish hello world} {set key val2} "gtid $myuuid:$mygno * EXEC"]
            assert_replication_stream $slave_repl  [list multi {set key val1} {publish hello world} {set key val2} "gtid $myuuid:$mygno * EXEC"]

            assert_match  "*$mygtidset*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "$myuuid:$mygno"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "$myuuid:$mygno"
        }

        test "propagte repl: GTID-ENABLED(yes) TX(yes) CMD(gtid) => not allowed" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            set orig_val [$master GET key]

            $master MULTI
            $master PUBLISH hello world
            catch {$master GTID A:1 1 set key valx} err
            assert_match "*gtidCommand not allowed in multi*" $err
            catch {$master EXEC} err
            assert_match "*EXECABORT Transaction discarded because of previous errors*" $err

            assert_replication_stream $master_repl {{}}
            assert_replication_stream $slave_repl  {{}}

            assert_equal [$master GET key] $orig_val
            assert_equal [$slave GET key] $orig_val
        }

        test "propagte repl: GTID-ENABLED(yes) TX(no) CMD(write)" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master SET key val3

            wait_for_gtid_sync $master $slave

            incr mygno
            set mygtidset "$myuuid:1-$mygno"

            assert_equal [$master GET key] val3
            assert_equal [$slave GET key] val3

            assert_replication_stream $master_repl [list "gtid $myuuid:$mygno * SET key val3"]
            assert_replication_stream $slave_repl  [list "gtid $myuuid:$mygno * SET key val3"]

            assert_match  "*$mygtidset*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "$myuuid:$mygno"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "$myuuid:$mygno"
        }

        test "propagte repl: GTID-ENABLED(yes) TX(no) CMD(may-replicate)" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master PUBLISH hello world

            wait_for_gtid_sync $master $slave

            # mygno not incrmented
            set mygtidset "$myuuid:1-$mygno"

            assert_replication_stream $master_repl {{publish hello world}}
            assert_replication_stream $slave_repl  {{publish hello world}}

            assert_match  "*$mygtidset*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] -1
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] -1

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] {}
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] {}
        }

        test "propagte repl: GTID-ENABLED(yes) TX(no) CMD(gtid)" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master GTID A:1 1 SET key val4; # master changed to db-1

            wait_for_gtid_sync $master $slave

            $master select 1
            $slave select 1
            assert_equal [$master GET key] val4
            assert_equal [$slave GET key] val4

            assert_replication_stream $master_repl [list {select 1} "gtid A:1 * SET key val4"]
            assert_replication_stream $slave_repl  [list {select 1} "gtid A:1 * SET key val4"]

            assert_match  "*A:1*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            # length of *2\r\n$6\r\nselect\r\n$1\r\n0\r\n is 23
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1+23]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "A:1"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "A:1"

            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master select $orig_db
            $slave select $orig_db

            $master GTID A:2 0 SET key val5

            wait_for_gtid_sync $master $slave

            assert_equal [$master GET key] val5
            assert_equal [$slave GET key] val5

            assert_replication_stream $master_repl [list "select $orig_db" "gtid A:2 * SET key val5"]
            assert_replication_stream $slave_repl  [list "select $orig_db" "gtid A:2 * SET key val5"]

            assert_match  "*A:1-2*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1+23]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "A:2"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "A:2"

        }

        test "propagte repl: expire" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master SET key val6 PX 100
            # Wait for active expire to fire and write DEL to the replication
            # stream. Using wait_for_condition instead of a hard after-delay
            # avoids flakiness in SWAP mode where cold-key expiry may not
            # complete within a fixed 200 ms window.
            wait_for_condition 100 100 {
                [$master EXISTS key] == 0
            } else {
                fail "key val6 did not expire on master"
            }

            wait_for_gtid_sync $master $slave

            assert_equal [$master EXISTS key] 0
            assert_equal [$slave EXISTS key] 0

            assert_replication_stream $master_repl [list "gtid $myuuid:[expr $mygno+1] * SET key val6 PXAT *" "gtid $myuuid:[expr $mygno+2] * DEL key"]
            assert_replication_stream $slave_repl [list "gtid $myuuid:[expr $mygno+1] * SET key val6 PXAT *" "gtid $myuuid:[expr $mygno+2] * DEL key"]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "$myuuid:[expr $mygno+1]-[expr $mygno+2]"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "$myuuid:[expr $mygno+1]-[expr $mygno+2]"

            incr mygno 2
            set mygtidset "$myuuid:1-$mygno"
            assert_match  "*$mygtidset*" [status $master gtid_executed]
        }
    }
}

start_server {tags {"gtid"} overrides {gtid-enabled no}} {
    set master_repl [attach_to_replication_stream]
    if {$::swap} {
        set orig_db 0
    } else {
        set orig_db 9
    }

    start_server {overrides {gtid-enabled no}} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]

        # Init replication link and and repl stream
        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        set slave_repl [attach_to_replication_stream]

        $master SET key val0
        wait_for_ofs_sync $master $slave
        assert_equal [$master GET key] val0
        assert_equal [$slave GET key] val0

        set myuuid [status $master gtid_uuid]
        set mygno  [status $master gtid_executed_gno_count]

        assert_replication_stream $master_repl [list {select *} "set key val0"]
        assert_replication_stream $slave_repl  [list {select *} "set key val0"]

        test "propagte repl: GTID-ENABLED(no) TX(no) CMD(gtid)" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master GTID A:1 $orig_db SET key val1

            wait_for_ofs_sync $master $slave

            assert_equal [$master GET key] val1
            assert_equal [$slave GET key] val1

            assert_replication_stream $master_repl [list "gtid A:1 * SET key val1"]
            assert_replication_stream $slave_repl  [list "gtid A:1 * SET key val1"]

            assert_match  "*A:1*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "A:1"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "A:1"
        }

        test "propagte repl: GTID-ENABLED(no) TX(yes) CMD(gtid) => not allowed" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            set orig_val [$master GET key]

            $master MULTI
            $master PUBLISH hello world
            catch { $master GTID A:1 1 set key valx } err
            assert_match "*gtidCommand not allowed in multi*" $err
            catch { $master EXEC} err
            assert_match "*EXECABORT Transaction discarded because of previous errors*" $err

            assert_replication_stream $master_repl {{}}
            assert_replication_stream $slave_repl  {{}}

            assert_equal [$master GET key] $orig_val
            assert_equal [$slave GET key] $orig_val
        }

    }
}

start_server {tags {"gtid"} overrides {gtid-enabled yes}} {

    test "gtid command with gtid.set-lost ignored" {
        r GTIDX ADD LOST A 1 10
        catch {r GTID A:5 0 set hello world} reply
        assert_match "*gtid command already executed*" $reply
    }

}

start_server {tags {"gtid"} overrides} {
    test "gtid uses jemalloc allocator" {
        assert_equal [s gtid_allocator] [s mem_allocator]
    }
}

start_server {tags {"master"} overrides} {
    test "change gtid-enabled efficient" {
        set repl [attach_to_replication_stream]
        r set k v1
        assert_replication_stream $repl {
            {select *}
            {set k v1}
        }
        assert_equal [r get k] v1

        r config set gtid-enabled yes

        set repl [attach_to_replication_stream]
        r set k v2

        assert_replication_stream $repl {
            {select *}
            {gtid * * set k v2}
        }
        assert_equal [r get k] v2

        r config set gtid-enabled no
        set repl [attach_to_replication_stream]
        r set k v3
        assert_replication_stream $repl {
            {select *}
            {set k v3}
        }
        assert_equal [r get k] v3
    }
}

#closed gtid-enabled, can exec gtid command
start_server {tags {"gtid"} overrides} {
    test "exec gtid command" {
        r gtid A:1 $::target_db set k v
        assert_equal [r get k] v
        assert_equal [dict get [get_gtid r] "A"] "1"
    }
}

# stand-alone redis exec gtid related commands
start_server {tags {"gtid"} overrides {gtid-enabled yes}} {
    test {COMMANDS} {
        test {GTID SET} {
            r gtid A:1 $::target_db set x foobar
            r get x
        } {foobar}

        test {GTID REPATE SET} {
            catch {r gtid A:1 $::target_db set x foobar} error
            assert_match $error "gtid command already executed, `A:1`, `$::target_db`, `set`,"
        }
        test {SET} {
            r set y foobar
            r get y
        } {foobar}

        test {MULTI} {
            r multi
            r set z foobar
            r gtid A:3 $::target_db exec
            r set z f
            r get z
        } {f}
        test {MULTI} {
            set z_value [r get z]
            r del x
            assert_equal [r get x] {}
            r multi
            r set z foobar1
            catch {r gtid A:3 $::target_db exec} error
            assert_equal $error "gtid command already executed, `A:3`, `$::target_db`, `exec`,"
            assert_equal [r get z] $z_value
            r set x f1
            r get x
        } {f1}
        test "ERR WRONG NUMBER" {
            catch {r gtid A } error
            assert_match "ERR wrong number of arguments for 'gtid' command" $error
        }

    }

    test {INFO GTID} {
        set dicts [dict get [get_gtid r] [status r run_id]]
        set value [lindex $dicts 0]
        assert_equal [string match {1-*} $value] 1
    }
}

# verify gtid command db
start_server {tags {"gtid"} overrides {gtid-enabled yes}} {
    test "multi-exec select db" {
        set repl [attach_to_replication_stream]
        r set k v
        r select 0
        r set k v

        if {$::swap} {
            assert_replication_stream $repl {
                {select *}
                {gtid * * set k v}
                {gtid * 0 set k v}
            }
        } else {
            assert_replication_stream $repl {
                {select *}
                {gtid * * set k v}
                {select *}
                {gtid * 0 set k v}
            }
        }
        r select $::target_db
    }

    test "multi-exec select db" {
        set repl [attach_to_replication_stream]
        r multi
        r set k v
        r select 0
        r set k v
        r exec
        r set k v1

        if {$::swap} {
            assert_replication_stream $repl {
                {multi}
                {select *}
                {set k v}
                {set k v}
                {gtid * * EXEC}
                {gtid * 0 set k v1}
            }
        } else {
            assert_replication_stream $repl {
                {multi}
                {select *}
                {set k v}
                {select 0}
                {set k v}
                {gtid * * EXEC}
                {gtid * 0 set k v1}
            }
        }
    }
}

start_server {tags {"repl"} overrides} {
    set master [srv 0 client]
    $master config set repl-diskless-sync-delay 1
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set gtid-enabled yes
    set repl [attach_to_replication_stream]
    start_server {tags {"slave"}} {
        set slave [srv 0 client]
        $slave slaveof $master_host $master_port
        wait_for_sync $slave
        $master multi
        $master select 1
        $master select 2
        $master set k v
        $master select 3
        $master set k v1
        $master exec

        if {$::swap} {
            assert_replication_stream $repl {
                {multi}
                {select 2}
                {set k v}
                {select 3}
                {set k v1}
                {gtid * 0 EXEC}
            }
        } else {
            assert_replication_stream $repl {
                {multi}
                {select 2}
                {set k v}
                {select 3}
                {set k v1}
                {gtid * 9 EXEC}
            }
        }
        
        after 1000

        assert_equal [$slave get k] {}
        $slave select 2
        assert_equal [$slave get k] v
        $slave select 3
        assert_equal [$slave get k] v1

    }
}
