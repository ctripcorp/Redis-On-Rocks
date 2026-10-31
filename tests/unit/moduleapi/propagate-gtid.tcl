set testmodule [file normalize tests/modules/propagate.so]

if {[info exists ::swap] && $::swap} {
tags {"modules" "gtid"} {
    test {gtid wrapping module command with RedisModule_Replicate does not double-propagate} {
        # Guards gtidCommand() in ctrip_swap_repl.c: when a module command is
        # invoked via `GTID <uuid:gno> <dbid> <module-cmd>`, the module's
        # RedisModule_Replicate calls are auto-wrapped in a single MULTI/EXEC
        # transaction on the target DB, and preventCommandPropagation(c) must
        # suppress outer GTID command propagation so the replica applies the
        # module writes exactly once.
        start_server [list overrides [list gtid-enabled yes loadmodule "$testmodule"]] {
            set replica [srv 0 client]
            set replica_host [srv 0 host]
            set replica_port [srv 0 port]

            start_server [list overrides [list gtid-enabled yes loadmodule "$testmodule"]] {
                set master [srv 0 client]
                set master_host [srv 0 host]
                set master_port [srv 0 port]

                $replica replicaof $master_host $master_port
                wait_for_sync $replica

                set master_repl [attach_to_replication_stream]
                set replica_repl [attach_to_replication_stream]

                $master select 0
                $master gtid A:1 1 propagate-test.simple
                wait_for_ofs_sync $master $replica

                $replica select 1
                assert_equal 1 [$replica get counter-1]
                assert_equal 1 [$replica get counter-2]
                $replica select 0
                assert_equal {} [$replica get counter-1]

                assert_replication_stream $master_repl [list {multi} {select 1} {incr counter-1} {incr counter-2} {exec}]
                assert_replication_stream $replica_repl [list {multi} {select 1} {incr counter-1} {incr counter-2} {exec}]

                close_replication_stream $master_repl
                close_replication_stream $replica_repl
            }
        }
    }
}
}

