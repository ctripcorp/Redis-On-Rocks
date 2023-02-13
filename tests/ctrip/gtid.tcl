# [functional testing]
# open or close gtid-enabled efficient
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

        test {GTID AND COMMENT SET} {
            r gtid A:2 $::target_db {/*comment*/} set x1 foobar 
            r get x1 
        } {foobar}

        test {GTID REPATE SET} {
            catch {r gtid A:1 $::target_db set x foobar} error
            assert_match $error "ERR gtid command is executed, `A:1`, `$::target_db`, `set`,"
        } 
        test {SET} {
            r set y foobar
            r get y 
        } {foobar}

        test {GTID.AUTO} {
            r gtid.auto /*comment*/ set y foobar1
            r get y 
        } {foobar1}

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
            assert_equal $error "ERR gtid command is executed, `A:3`, `$::target_db`, `exec`,"
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
        assert_equal [string match {*all:*A:1-*} [r info gtid]] 1
        set dicts [dict get [get_gtid r] [status r run_id]]
        set value [lindex $dicts 0]
        assert_equal [string match {1-*} $value] 1  
    } 

    test "GTID.LWM 0" {
        assert_equal [dict get [get_gtid r] "A"] "1-3"
        r gtid.lwm A 0
        assert_equal [dict get [get_gtid r] "A"] "1-3"
    }
    
    test "GTID.LWM" {
        assert_equal [string match {*all:*A:1-100*} [r info gtid]] 0
        r gtid.lwm A 100
        assert_equal [dict get [get_gtid r] "A"] "1-100"
    }
    
}

# verify gtid command db
start_server {tags {"gtid"} overrides {gtid-enabled yes}} {
    test "multi-exec select db" {
        set repl [attach_to_replication_stream]
        r set k v 
        r select 0
        r set k v 

        if {$::swap_mode == "disk"} {
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

        if {$::swap_mode == "disk"} {
            assert_replication_stream $repl {
                {select *}
                {multi}
                {set k v}
                {set k v}
                {gtid * * exec}
                {gtid * 0 set k v1}
            }
        } else {
            assert_replication_stream $repl {
                {select *}
                {multi}
                {set k v}
                {select 0}
                {set k v}
                {gtid * * exec}
                {gtid * 0 set k v1}
            }
        }
    }
}

start_server {tags {"gtid"} overrides {gtid-enabled yes}} {
    test "gtidx" {
        assert_equal [r gtidx remove A] 0
        catch {[r gtidx list A]} {e}
        assert_match {*uuid not found*} $e
        catch {[r gtidx stat A]} {e}
        assert_match {*uuid not found*} $e

        r gtid A:1 0 set k v
        assert_equal [r gtidx list A] {A:1}
        assert_match {A:uuid_count:1,used_memory:*,gap_count:1,gno_count:1} [r gtidx stat A]

        assert_equal [r gtidx remove A] 1
        catch {[r gtidx list A]} {e}
        assert_match {*uuid not found*} $e
        catch {[r gtidx stat A]} {e}
        assert_match {*uuid not found*} $e
    }
}

start_server {tags {"gtid"} overrides {gtid-enabled yes gtid-uuid-gap-max-memory 1024}} {
    test "gtid purge" {
        for {set i 1} {$i <= 64} {incr i 2} {
            r gtid A:$i 0 set k v
        }
        assert_equal [status r gtid_purged_gap_count] 0
        assert_equal [status r gtid_purged_gno_count] 0

        for {set i 65} {$i <= 96} {incr i 2} {
            r gtid A:$i 0 set k v
        }
        assert_equal [status r gtid_purged_gap_count] 16
        assert_equal [status r gtid_purged_gno_count] 16

        for {set i 97} {$i <= 128} {incr i 2} {
            r gtid A:$i 0 set k v
        }
        assert_equal [status r gtid_purged_gap_count] 32
        assert_equal [status r gtid_purged_gno_count] 32
    }
}

if {$::swap_mode != "disk" } {
    #swap disk can't select 1
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

            assert_replication_stream $repl {
                {select 2}
                {multi}
                {set k v}
                {select 3}
                {set k v1}
                {gtid * 2 exec}
            }
            after 1000

            assert_equal [$slave get k] {}
            $slave select 2
            assert_equal [$slave get k] v
            $slave select 3
            assert_equal [$slave get k] v1

        }
    }
}
