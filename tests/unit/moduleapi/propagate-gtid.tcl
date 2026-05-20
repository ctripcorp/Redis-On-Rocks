set testmodule [file normalize tests/modules/propagate.so]

tags {"modules" "gtid"} {
    test {module auto-wrapped RedisModule_Replicate maps to one GTID} {
        # This guards the cc178563b0281843 fix: module commands enqueue
        # internal writes plus EXEC, so GTID must still treat the whole
        # auto-wrapped MULTI/EXEC block as exactly one GTID.
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

                set uuid [status $master gtid_uuid]
                set before_gno [status $master gtid_executed_gno_count]
                set before_master_gtidset [status $master gtid_set]
                set before_replica_gtidset [status $replica gtid_set]
                set before_master_reploff [status $master master_repl_offset]
                set before_replica_reploff [status $replica master_repl_offset]

                $master propagate-test.simple
                wait_for_gtid_sync $master $replica

                set expected_gno [expr {$before_gno + 1}]
                set expected_gtid "$uuid:$expected_gno"

                assert_equal $expected_gno [status $master gtid_executed_gno_count]
                assert_equal 1 [$replica get counter-1]
                assert_equal 1 [$replica get counter-2]
                assert {[gtid_set_is_equal [status $master gtid_set] [status $replica gtid_set]]}

                assert_replication_stream $master_repl [list {select *} {multi} {incr counter-1} {incr counter-2} "gtid $expected_gtid * EXEC"]
                assert_replication_stream $replica_repl [list {select *} {multi} {incr counter-1} {incr counter-2} "gtid $expected_gtid * EXEC"]

                assert_equal [expr {$before_master_reploff + 1}] [lindex [$master GTIDX SEQ LOCATE $before_master_gtidset] 0]
                assert_equal $expected_gtid [lindex [$master GTIDX SEQ LOCATE $before_master_gtidset] 1]
                assert_equal $expected_gtid [lindex [$replica GTIDX SEQ LOCATE $before_replica_gtidset] 1]

                close_replication_stream $master_repl
                close_replication_stream $replica_repl
            }
        }
    }
}
