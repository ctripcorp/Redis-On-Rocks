start_server {tags {"gtid-seq"} overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    set master [srv -1 client]
    set master_host [srv -1 host]
    set master_port [srv -1 port]
    set slave [srv 0 client]

    $slave replicaof $master_host $master_port
    wait_for_sync $slave

    test "build gtid seq index" {
        # simple write command
        $master SET key val0
        wait_for_ofs_sync $master $slave
        assert_match {*:1} [$master GTIDX seq gtid.set]
        assert_match {*:1} [$slave GTIDX seq gtid.set]

        # simple read command
        wait_for_ofs_sync $master $slave
        assert_equal [$master GET key] "val0"
        assert_match {*:1} [$master GTIDX seq gtid.set]
        assert_match {*:1} [$slave GTIDX seq gtid.set]

        # publish
        $master PUBLISH foo bar
        wait_for_ofs_sync $master $slave
        assert_match {*:1} [$master GTIDX seq gtid.set]
        assert_match {*:1} [$slave GTIDX seq gtid.set]

        # multi
        $master MULTI
        $master SET key val1
        $master SET key val2
        $master EXEC
        wait_for_ofs_sync $master $slave
        assert_match {*:1-2} [$master GTIDX seq gtid.set]
        assert_match {*:1-2} [$slave GTIDX seq gtid.set]

        # lua
        $master EVAL "redis.call('SET','key','val3'); redis.call('SET','key','val4')" 0
        wait_for_ofs_sync $master $slave
        assert_match {*:1-3} [$master GTIDX seq gtid.set]
        assert_match {*:1-3} [$slave GTIDX seq gtid.set]

        # multi + lua
        $master MULTI
        $master SET key val5
        $master EVAL "redis.call('SET','key','val6'); redis.call('SET','key','val7')" 0
        $master SET key val8
        $master EXEC
        wait_for_ofs_sync $master $slave
        assert_match {*:1-4} [$master GTIDX seq gtid.set]
        assert_match {*:1-4} [$slave GTIDX seq gtid.set]


        # expire
        $master MULTI
        $master SET tmpkey val1
        $master pexpire tmpkey 100
        $master EXEC
        after 200
        assert_equal [$master GET tmpkey] {}
        assert_equal [$slave GET tmpkey] {}
        wait_for_ofs_sync $master $slave
        assert_match {*:1-6} [$master GTIDX seq gtid.set]
        assert_match {*:1-6} [$slave GTIDX seq gtid.set]

        # gtid
        $master GTID A:1 0 SET key val9
        wait_for_ofs_sync $master $slave
        assert_match {*:1-6,A:1} [$master GTIDX seq gtid.set]
        assert_match {*:1-6,A:1} [$slave GTIDX seq gtid.set]

        # multi + gtid: rejected
        $master MULTI
        catch {$master GTID A:2 0 SET key val10} e
        catch {$master GTID A:3 0 SET key val11} e
        catch {$master EXEC} e
        wait_for_ofs_sync $master $slave
        assert_match {*:1-6,A:1} [$master GTIDX seq gtid.set]
        assert_match {*:1-6,A:1} [$slave GTIDX seq gtid.set]

        # lua + gtid: rejected
        catch {$master EVAL "redis.call('GTID','A:4','SET','key','val12'); redis.call('GTID','A:5','SET','key','val13')" 0} e
        wait_for_ofs_sync $master $slave
        assert_match {*:1-6,A:1} [$master GTIDX seq gtid.set]
        assert_match {*:1-6,A:1} [$slave GTIDX seq gtid.set]
    }
}
}
