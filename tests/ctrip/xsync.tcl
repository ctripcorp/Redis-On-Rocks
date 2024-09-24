start_server {tags {"xsync"} overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    set master [srv -1 client]
    set master_host [srv -1 host]
    set master_port [srv -1 port]
    set slave [srv 0 client]

    test "master(X) slave(X) LOCATE( ): gtid not related => xfullresync" {
        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $slave gtid_repl_mode] xsync

        $master set hello world
        $slave set foo bar

        set orig_sync_full [status $master sync_full]
        set orig_xsync_xfullresync [get_info_property $slave gtid gtid_sync_stat xsync_xfullresync]

        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xfullresync] 0

        assert {[status $master gtid_uuid] != [status $slave gtid_uuid]}
        assert {[status $master gtid_executed] != [status $slave gtid_executed]}

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert_equal [status $master gtid_executed] [status $slave gtid_executed]

        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xfullresync] [expr $orig_xsync_xfullresync+1]

        $slave replicaof no one
    }

    # master: | (X) set hello world | (P) set hello world_1 | (X) set hello world_2 |
    # slave:  | (X)
    test "master( ) slave(X) LOCATE(?): xfullresync" {
        assert_equal [status $master gtid_executed] [status $slave gtid_executed]
        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $slave gtid_repl_mode] xsync

        $master set hello world
        assert {[status $master gtid_executed] != [status $slave gtid_executed]}

        $master config set gtid-enabled no
        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $master gtid_prev_repl_mode] xsync
        $master set hello world_1

        $master config set gtid-enabled yes
        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $master gtid_prev_repl_mode] psync
        $master set hello world_2

        set orig_log_lines [count_log_lines -1]
        set orig_sync_full [status $master sync_full]
        set orig_xsync_xfullresync [get_info_property $slave gtid gtid_sync_stat xsync_xfullresync]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert_equal [status $master gtid_executed] [status $slave gtid_executed]

        verify_log_message -1 "*Partial sync request from*rejected*psync offset(*) < prev_repl_mode.from(*)*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xfullresync] [expr $orig_xsync_xfullresync+1]

        $slave replicaof no one
    }

    # master: | (P) set hello world_1 set hello world_2 | (X) set hello world_3 | (P) set hello world_4 |
    # slave:  | (P) set hello world_1 |
    test "master( ) slave(P) LOCATE(?): fullresync" {
        $master config set gtid-enabled no
        $slave config set gtid-enabled no

        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $slave gtid_repl_mode] psync

        $slave replicaof $master_host $master_port
        wait_for_ofs_sync $master $slave

        $master set hello world_1
        wait_for_ofs_sync $master $slave
        assert_equal [$slave get hello] world_1

        $slave replicaof 127.0.0.1 0

        $master set hello world_2

        $master config set gtid-enabled yes
        $master set hello world_3

        $master config set gtid-enabled no
        $master set hello world_4

        set orig_log_lines [count_log_lines -1]
        set orig_sync_full [status $master sync_full]
        set orig_psync_fullresync [get_info_property $slave gtid gtid_sync_stat psync_fullresync]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave
        wait_for_ofs_sync $master $slave

        assert_equal [$slave get hello] world_4

        verify_log_message -1 "*Partial sync request from*rejected*psync offset(*) < prev_repl_mode.from(*)*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_fullresync] [expr $orig_psync_fullresync+1]

        $slave replicaof no one
        $master config set gtid-enabled yes
        $slave config set gtid-enabled yes
    }

    # master: |(X)
    # slave : |(X) set foo bar; ...(9999)...; set foo bar |
    test "master(X) slave(X) LOCATE(X): gap > maxgap => xfullresync" {
        assert_equal [status $master gtid_executed] [status $slave gtid_executed]
        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $slave gtid_repl_mode] xsync

        for {set i 0} {$i < 10001} {incr i} {
            $slave set foo bar
        }

        set orig_log_lines [count_log_lines -1]
        set orig_sync_full [status $master sync_full]
        set orig_xsync_xfullresync [get_info_property $slave gtid gtid_sync_stat xsync_xfullresync]

        assert {[status $master gtid_executed] != [status $slave gtid_executed]}

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert_equal [status $master gtid_executed] [status $slave gtid_executed]

        verify_log_message -1 "*Partial sync request from * rejected*gap*>*maxgap*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xfullresync] [expr $orig_xsync_xfullresync+1]

        $slave replicaof no one
    }

    # master: |(X) set hello world |
    # slave:  |(X) set foo bar |
    test "master(X) slave(X) LOCATE(X): gap <= maxgap => xcontinue" {
        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $slave gtid_repl_mode] xsync
        assert_equal [status $master gtid_executed] [status $slave gtid_executed]

        $master set hello world
        $slave set foo bar

        assert {[status $master gtid_executed] != [status $slave gtid_executed]}

        set orig_log_lines [count_log_lines -1]
        set orig_sync_partial_ok [status $master sync_partial_ok]
        set orig_xsync_xcontinue [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue]
        set orig_master_lost [status $master gtid_lost]
        set orig_slave_lost [status $slave gtid_lost]

        set slave_replid [status $slave master_replid]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        # replid and offset untouched
        assert_equal [status $slave master_replid] $slave_replid

        assert {[status $master gtid_executed] != [status $slave gtid_executed]}
        verify_log_message -1 "*Partial sync request from*accepted*gap=1 <= maxgap=*" $orig_log_lines
        assert_equal [status $master sync_partial_ok] [expr $orig_sync_partial_ok+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue+1]
        assert {[status $master gtid_lost] != $orig_master_lost}
        assert_equal [status $slave gtid_lost] $orig_slave_lost

        $slave replicaof no one
    }

    # master: | (P) set hello world_1 set hello world_2 | (X) set hello world_3 |
    # slave:  | (P) set hello world_1 |
    test "master(X) slave(P) LOCATE(P): continue + xcontinue" {
        $master config set gtid-enabled no
        $slave config set gtid-enabled no

        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $slave gtid_repl_mode] psync

        $slave replicaof $master_host $master_port
        wait_for_ofs_sync $master $slave

        $master set hello world_1
        wait_for_ofs_sync $master $slave
        assert_equal [$slave get hello] world_1

        $slave replicaof 127.0.0.1 0

        $master set hello world_2

        assert_equal [$slave get hello] world_1

        $master config set gtid-enabled yes

        $master set hello world_3

        set orig_log_lines [count_log_lines -1]
        set orig_sync_full [status $master sync_full]
        set orig_sync_partial_ok [status $master sync_partial_ok]
        set orig_psync_continue [get_info_property $slave gtid gtid_sync_stat psync_continue]
        set orig_psync_xcontinue [get_info_property $slave gtid gtid_sync_stat psync_xcontinue]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert_equal [$slave get hello] world_3

        verify_log_message -1 "*Partial sync request from * accepted: before psync => xsync*" $orig_log_lines
        verify_log_message -1 "*Disconnect slave * to notify repl mode switched*" $orig_log_lines
        verify_log_message -1 "*Partial sync request from * accepted: psync => xsync*" $orig_log_lines

        assert_equal [status $master sync_full] [expr $orig_sync_full]
        assert_equal [status $master sync_partial_ok] [expr $orig_sync_partial_ok+2]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_continue] [expr $orig_psync_continue+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_xcontinue] [expr $orig_psync_xcontinue+1]

        $slave replicaof no one
        $slave config set gtid-enabled yes
    }

    # master: | (psync) set hello world_1 | (X) set hello world_2 set hello world_3 |
    # slave:  | (psync) set hello world_1  set hello world_2 |
    test "master(X) slave(P) LOCATE(X): xfullresync" {
        $master config set gtid-enabled no
        $slave config set gtid-enabled no

        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $slave gtid_repl_mode] psync

        $slave replicaof $master_host $master_port
        wait_for_ofs_sync $master $slave

        $master set hello world_1
        wait_for_ofs_sync $master $slave
        assert_equal [$slave get hello] world_1

        $slave replicaof no one
        $slave set hello world_2

        $master config set gtid-enabled yes
        $master set hello world_2
        $master set hello world_3

        set orig_log_lines [count_log_lines -1]
        set orig_sync_full [status $master sync_full]
        set orig_psync_xfullresync [get_info_property $slave gtid gtid_sync_stat psync_xfullresync]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave
        wait_for_gtid_sync $master $slave

        assert_equal [$slave get hello] world_3

        verify_log_message -1 "*Partial sync request from * rejected: request mode(psync) != located mode(xsync)*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_xfullresync] [expr $orig_psync_xfullresync+1]

        $slave replicaof no one
        $slave config set gtid-enabled yes
    }

    # master: | (X) set hello world_1 set hello world_2 | (P) set hello world_3 |
    # slave:  | (X) set hello world_1 |
    test "master(P) slave(X) locate(X): gap=0 => xcontinue + continue" {
        $master config set gtid-enabled yes
        $slave config set gtid-enabled yes

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        $master set hello world_1
        wait_for_gtid_sync $master $slave
        assert_equal [$slave get hello] world_1

        $slave replicaof 127.0.0.1 0

        $master set hello world_2

        $master config set gtid-enabled no
        $master set hello world_3

        set master_replid [status $master master_replid]
        assert {[status $slave master_replid] != $master_replid}

        set orig_log_lines [count_log_lines -1]
        set orig_sync_partial_ok [status $master sync_partial_ok]
        set orig_xsync_xcontinue [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue]
        set orig_xsync_continue [get_info_property $slave gtid gtid_sync_stat xsync_continue]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave
        wait_for_gtid_sync $master $slave

        #replid and offset aligned
        assert_equal [status $slave master_replid] $master_replid

        assert_equal [$slave get hello] world_3

        verify_log_message -1 "*Partial sync request from * accepted: gap=0 <= maxgap=*" $orig_log_lines
        verify_log_message -1 "*Disconnect slave * to notify repl mode switched*" $orig_log_lines
        verify_log_message -1 "*Partial sync request from * accepted: xsync => psync, offset=*, limit=0, *" $orig_log_lines

        assert_equal [status $master sync_partial_ok] [expr $orig_sync_partial_ok+2]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_continue] [expr $orig_xsync_continue+1]

        $slave replicaof no one
    }

    # master: | (X) set hello world_1 set hello world_3 | (P) set hello world_4 |
    # slave:  | (X) set hello world_1 set hello world_2 |
    test "master(P) slave(X) locate(X): gap <= maxgap => xcontinue + continue" {
        $master config set gtid-enabled yes
        $slave config set gtid-enabled yes

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        $master set hello world_1
        wait_for_gtid_sync $master $slave
        assert_equal [$slave get hello] world_1

        $slave replicaof no one
        $slave set hello world_2

        $master set hello world_3

        $master config set gtid-enabled no
        $master set hello world_4

        set master_replid [status $master master_replid]
        assert {[status $slave master_replid] != $master_replid}

        set orig_log_lines [count_log_lines -1]
        set orig_sync_partial_ok [status $master sync_partial_ok]
        set orig_xsync_xcontinue [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue]
        set orig_xsync_continue [get_info_property $slave gtid gtid_sync_stat xsync_continue]
        set orig_master_gtid_lost [status $master gtid_lost]
        set orig_slave_gtid_lost [status $slave gtid_lost]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave
        wait_for_gtid_sync $master $slave

        assert_equal [status $slave master_replid] $master_replid
        assert_equal [status $master master_replid] $master_replid

        assert_equal [$slave get hello] world_4

        verify_log_message -1 "*Partial sync request from * accepted: gap=1 <= maxgap=*" $orig_log_lines
        verify_log_message -1 "*Disconnect slave * to notify repl mode switched*" $orig_log_lines
        verify_log_message -1 "*Partial sync request from * accepted: xsync => psync, offset=*, limit=0, *" $orig_log_lines

        assert_equal [status $master sync_partial_ok] [expr $orig_sync_partial_ok+2]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_continue] [expr $orig_xsync_continue+1]
        assert {[status $master gtid_lost] != $orig_master_gtid_lost}
        assert_equal [status $slave gtid_lost] $orig_slave_gtid_lost

        $slave replicaof no one
        $master set gtid-enabled yes
    }

    # master: | (X) set hello world_0 set foo bar_0 | (P) set hello world_2a |
    # slave : | (X) set hello world_0 ...(10000)... set hello world_10001 set hello world_2b |
    test "master(P) slave(X) locate(X): gap > maxgap => fullresync" {
        $master config set gtid-enabled yes
        $slave config set gtid-enabled yes

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        $master set hello world_0
        wait_for_gtid_sync $master $slave
        assert_equal [$slave get hello] world_0

        $slave replicaof no one
        assert_equal [status $slave gtid_repl_mode] xsync

        $master set foo bar_0

        $master config set gtid-enabled no
        assert_equal [status $master gtid_repl_mode] psync

        $master set hello world_2a

        for {set i 1} {$i < 10002} {incr i} {
            $slave set hello world_$i
        }
        $slave set hello world_2b

        set orig_log_lines [count_log_lines -1]
        set orig_sync_full [status $master sync_full]
        set orig_xsync_fullresync [get_info_property $slave gtid gtid_sync_stat xsync_fullresync]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave
        wait_for_gtid_sync $master $slave

        assert_equal [$slave get foo] bar_0
        assert_equal [$slave get hello] world_2a

        verify_log_message -1 "*Partial sync request from * rejected: gap=* > maxgap=*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_fullresync] [expr $orig_xsync_fullresync+1]

        $master config set gtid-enabled yes
        $slave replicaof no one

        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $slave gtid_repl_mode] xsync
    }

    # master: | (X) set hello world_0 | (P) set hello world_2a |
    # slave : | (X) set hello world_0 ...(10000)... set hello world_10001 set hello world_2b |
    test "master(P) slave(X) locate(P): gap > maxgap => fullresync" {
        $master config set gtid-enabled yes
        $slave config set gtid-enabled yes

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        $master set hello world_0
        wait_for_gtid_sync $master $slave
        assert_equal [$slave get hello] world_0

        $slave replicaof no one
        assert_equal [status $slave gtid_repl_mode] xsync

        $master config set gtid-enabled no
        assert_equal [status $master gtid_repl_mode] psync

        $master set hello world_2a

        for {set i 1} {$i < 10002} {incr i} {
            $slave set hello world_$i
        }
        $slave set hello world_2b

        set orig_log_lines [count_log_lines -1]
        set orig_sync_full [status $master sync_full]
        set orig_xsync_fullresync [get_info_property $slave gtid gtid_sync_stat xsync_fullresync]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave
        wait_for_gtid_sync $master $slave

        assert_equal [$slave get foo] bar_0
        assert_equal [$slave get hello] world_2a

        verify_log_message -1 "*continue point adjust to psync from*" $orig_log_lines
        verify_log_message -1 "*Partial sync request from * rejected: gap=* > maxgap=*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_fullresync] [expr $orig_xsync_fullresync+1]

        $master config set gtid-enabled yes
        $slave replicaof no one

        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $slave gtid_repl_mode] xsync
    }

    # Psync are used to align replication offset
    # master: | (P) set hello world | (X) set hello world_0             set hello world_1a | (P) set hello world_2 |
    # slave : | (P) set hello world | (X) set hello world_0 | (P) set hello world_1b |
    test "master(P) slave(P) locate(X): gap > maxgap => fullresync" {
        $master config set gtid-enabled no

        $slave replicaof $master_host $master_port

        $master set hello world
        wait_for_ofs_sync $master $slave

        assert_equal [$slave get hello] world

        $master config set gtid-enabled yes
        wait_for_sync $slave

        assert_equal [status $slave gtid_repl_mode] xsync

        $master set hello world_0
        wait_for_gtid_sync $master $slave
        assert_equal [$slave get hello] world_0

        $slave replicaof no one
        $slave config set gtid-enabled no
        $slave set hello world_1b

        $master set hello world_1a
        $master config set gtid-enabled no
        $master set hello world_2

        set orig_log_lines [count_log_lines -1]
        set orig_sync_full [status $master sync_full]
        set orig_psync_fullresync [get_info_property $slave gtid gtid_sync_stat psync_fullresync]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave
        wait_for_gtid_sync $master $slave

        assert_equal [$slave get hello] world_2

        verify_log_message -1 "*Partial sync request from * rejected: request mode(psync) != located mode(xsync)*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_fullresync] [expr $orig_psync_fullresync+1]

        $master config set gtid-enabled yes
        $slave config set gtid-enabled yes
        wait_for_sync $slave
        $slave replicaof no one

        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $slave gtid_repl_mode] xsync
    }

    # master: | (P) set hello world set hello world_1 |
    # slave : | (P) set hello world |
    test "master(P) slave(P) locate(P): handled by redis, offset valid => psync" {
        $master config set gtid-enabled no

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $slave gtid_repl_mode] psync

        $master set hello world
        wait_for_ofs_sync $master $slave
        assert_equal [$slave get hello] world

        $slave replicaof 127.0.0.1 0

        $master set hello world_1
        assert_equal [$slave get hello] world

        set orig_log_lines [count_log_lines -1]
        set orig_sync_partial_ok [status $master sync_partial_ok]
        set orig_psync_continue [get_info_property $slave gtid gtid_sync_stat psync_continue]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave
        wait_for_ofs_sync $master $slave

        assert_equal [$slave get hello] world_1

        verify_log_message -1 "*Partial sync request from * handle by vanilla redis*" $orig_log_lines
        assert_equal [status $master sync_partial_ok] [expr $orig_sync_partial_ok+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_continue] [expr $orig_psync_continue+1]

        $master config set gtid-enabled yes
        $slave config set gtid-enabled yes
        wait_for_sync $slave
        $slave replicaof no one
    }

    # master: | (P) set hello world set hello world_1 |
    # slave : | (P) set hello world; |
    test "master(P) slave(P) locate(P): handled by redis, replid changed => fullresync" {
        $master config set gtid-enabled no
        $slave config set gtid-enabled no

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $slave gtid_repl_mode] psync

        $master set hello world
        wait_for_ofs_sync $master $slave
        assert_equal [$slave get hello] world

        $slave replicaof no one

        $master set hello world_1
        assert_equal [$slave get hello] world

        set orig_log_lines [count_log_lines -1]
        set orig_sync_full [status $master sync_full]
        set orig_psync_fullresync [get_info_property $slave gtid gtid_sync_stat psync_fullresync]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave
        wait_for_ofs_sync $master $slave

        assert_equal [$slave get hello] world_1

        verify_log_message -1 "*Partial sync request from * handle by vanilla redis*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_fullresync] [expr $orig_psync_fullresync+1]

        $master config set gtid-enabled yes
        $slave config set gtid-enabled yes
        wait_for_sync $slave
        $slave replicaof no one
    }
}
}

start_server {tags {"xsync"} overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    set master [srv -1 client]
    set master_host [srv -1 host]
    set master_port [srv -1 port]
    set slave [srv 0 client]

    test "master xsync => psync: replid and offset reset" {
        assert_equal [status $master gtid_repl_mode] xsync

        set origin_replid [status $master master_replid]

        $master config set gtid-enabled no
        assert_equal [status $master gtid_repl_mode] psync

        assert {[status $master master_replid] != $origin_replid}
        assert_equal [status $master master_replid2] "0000000000000000000000000000000000000000"
    }

    test "master psync => xsync: replid and offset untouched" {
        assert_equal [status $master gtid_repl_mode] psync

        set origin_replid  [status $master master_replid]
        set origin_replid2 [status $master master_replid2]

        $master config set gtid-enabled yes
        assert_equal [status $master gtid_repl_mode] xsync

        assert_equal [status $master master_replid] $origin_replid
        assert_equal [status $master master_replid2] $origin_replid2
    }

    test "master xsync => psync (defered untill slave promoted to master): replid and offset reset" {
        $master config set gtid-enabled no
        $slave config set gtid-enabled no

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        set orig_replid [status $master master_replid]

        # master would shift replid
        # slave would psync ok and upate replid
        $master replicaof 127.0.0.1 0
        after 100
        assert_equal [status $slave master_link_status] "down"

        $master replicaof no one
        wait_for_sync $slave

        set master_replid [status $master master_replid]
        assert {$master_replid != $orig_replid}

        assert_equal [status $master master_replid] [status $slave master_replid]
        assert_equal [status $master master_replid2] [status $slave master_replid2]
        assert {[status $slave master_replid2] != "0000000000000000000000000000000000000000"}

        $master config set gtid-enabled yes
        after 100
        wait_for_sync $slave

        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $slave gtid_repl_mode] xsync

        assert_equal [status $master master_replid] $master_replid

        # psync mode defered untill slave promoted to master
        $slave replicaof no one

        assert_equal [status $slave gtid_repl_mode] psync
        assert {$master_replid != [status $slave master_replid]}
        assert_equal [status $slave master_replid2] "0000000000000000000000000000000000000000"

        $slave config set gtid-enabled yes
    }

    test "master psync => xsync (defered untill slave promoted to master): replid and offset untouched" {
        $master config set gtid-enabled no

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $master gtid_repl_mode] psync

        set orig_replid [status $slave master_replid]
        set orig_replid2 [status $slave master_replid2]

        $slave replicaof no one
        after 100
        assert_equal [status $slave gtid_repl_mode] xsync

        assert_equal $orig_replid [status $slave master_replid]
        assert_equal $orig_replid2 [status $slave master_replid2]

        $master config set gtid-enabled yes
    }

    # following case already tested in previous cases:
    # "slave xsync: replid and offset untouched"
    # "slave xsync => psync: replid and offset aligned"
    # "slave psync => xsync: replid and offset untouched"
}
}

start_server {tags {"xsync"} overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    set master [srv -1 client]
    set master_host [srv -1 host]
    set master_port [srv -1 port]
    set slave [srv 0 client]

    # master: (P)... | (X) set hello world_1 | (P) set hello world_3 |
    # slave:  (P)... | (X) set hello world_1 set hello world_2 ... set hello world_2 |
    test "xcontinue + continue: align offset would invalidate replication backlog" {
        $master config set gtid-enabled no
        $slave config set gtid-enabled yes

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        $master set hello world_0
        wait_for_ofs_sync $master $slave
        assert_equal [$slave get hello] world_0


        $master config set gtid-enabled yes
        after 100
        wait_for_sync $slave
        assert_equal [status $slave gtid_repl_mode] xsync

        $master set hello world_1
        wait_for_gtid_sync $master $slave
        assert_equal [$slave get hello] world_1

        $slave replicaof no one
        assert_equal [status $slave gtid_repl_mode] xsync

        for {set i 0} {$i < 10} {incr i} {
            $slave set hello world_2
        }

        $master config set gtid-enabled no
        $master set hello world_3

        set master_replid [status $master master_replid]
        assert {[status $slave master_replid] != $master_replid}

        set orig_log_lines [count_log_lines -1]
        set orig_sync_partial_ok [status $master sync_partial_ok]
        set orig_xsync_xcontinue [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue]
        set orig_xsync_continue [get_info_property $slave gtid gtid_sync_stat xsync_continue]

        $slave replicaof $master_host $master_port

        # gtid.set, replid and offset aligned
        wait_for_sync $slave
        wait_for_gtid_sync $master $slave
        assert_equal [status $slave master_replid] $master_replid

        assert_equal [$slave get hello] world_3
        $slave replicaof no one

        # if backlog not invalidated, append to gtid_seq would assert fail
        $slave set hello crash

        $master set gtid-enabled yes
    }
}
}

proc wait_for_repl_mode_sync {r1 r2} {
    wait_for_condition 500 100 {
        [status $r1 gtid_repl_mode] eq [status $r2 gtid_repl_mode]
    } else {
        fail "repl mode didn't sync in time"
    }
}

start_server {tags {"xsync"} overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {

    set M [srv -3 client]
    set M_host [srv -3 host]
    set M_port [srv -3 port]
    set S [srv -2 client]
    set S_host [srv -2 host]
    set S_port [srv -2 port]
    set SS1 [srv -1 client]
    set SS1_host [srv -1 host]
    set SS1_port [srv -1 port]
    set SS2 [srv 0 client]
    set SS2_host [srv 0 host]
    set SS2_port [srv 0 port]

    test "gtid.set-lost updated: init" {
        # create some gtid gap
        $M set hello master
        $S set hello slave
        $SS1 set hello sub-slave-1
        $SS2 set hello sub-slave-2

        assert {[status $M gtid_set] != [status $S gtid_set]}
        assert {[status $M gtid_set] != [status $SS1 gtid_set]}
        assert {[status $M gtid_set] != [status $SS2 gtid_set]}

        $SS1 replicaof $S_host $S_port
        $SS2 replicaof $S_host $S_port
        $S replicaof $M_host $M_port

        wait_for_sync $S
        wait_for_sync $SS1
        wait_for_sync $SS2

        wait_for_gtid_sync $M $S
        wait_for_gtid_sync $M $SS1
        wait_for_gtid_sync $M $SS2

        assert_equal [$M get hello] master
        assert_equal [$S get hello] master
        assert_equal [$SS1 get hello] master
        assert_equal [$SS2 get hello] master

        $M set hello master-again

        wait_for_gtid_sync $M $S
        wait_for_gtid_sync $M $SS1
        wait_for_gtid_sync $M $SS2

        assert_equal [$M get hello] master-again
        assert_equal [$S get hello] master-again
        assert_equal [$SS1 get hello] master-again
        assert_equal [$SS2 get hello] master-again
    }

    test "gtid.set-lost updated by downstream: disconnect upstream and downstreams (except triggering one)" {
        $SS1 replicaof no one
        $SS1 set hello sub-slave-1-again

        assert {[status $M gtid_set] != [status $SS1 gtid_set]}

        set orig_sync_partial_ok_M [status $M sync_partial_ok]
        set orig_sync_partial_ok_S [status $S sync_partial_ok]
        set orig_xsync_xcontinue_S [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]
        set orig_xsync_xcontinue_SS1 [get_info_property $SS1 gtid gtid_sync_stat xsync_xcontinue]
        set orig_xsync_xcontinue_SS2 [get_info_property $SS2 gtid gtid_sync_stat xsync_xcontinue]

        $SS1 replicaof $S_host $S_port

        wait_for_gtid_sync $SS1 $M
        wait_for_gtid_sync $SS1 $S
        wait_for_gtid_sync $SS1 $SS2

        assert_equal [status $M sync_partial_ok] [expr $orig_sync_partial_ok_M+1]
        assert_equal [status $S sync_partial_ok] [expr $orig_sync_partial_ok_S+2]
        assert_equal [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue_S+1]
        assert_equal [get_info_property $SS1 gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue_SS1+1]
        assert_equal [get_info_property $SS2 gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue_SS2+1]
    }

    test "gtid.set-lost updated by upstream: disconnect upstream and downstreams(except triggering one)" {
        $S replicaof no one
        $S set hello sub-slave-again

        $M gtidx add lost A 1 10

        wait_for_gtid_sync $S $SS1
        wait_for_gtid_sync $S $SS2

        assert_equal [$SS1 get hello] sub-slave-again
        assert_equal [$SS2 get hello] sub-slave-again

        assert {[status $M gtid_set] != [status $S gtid_set]}

        set orig_sync_partial_ok_M [status $M sync_partial_ok]
        set orig_sync_partial_ok_S [status $S sync_partial_ok]
        set orig_xsync_xcontinue_S [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]
        set orig_xsync_xcontinue_SS1 [get_info_property $SS1 gtid gtid_sync_stat xsync_xcontinue]
        set orig_xsync_xcontinue_SS2 [get_info_property $SS2 gtid gtid_sync_stat xsync_xcontinue]

        $S replicaof $M_host $M_port

        wait_for_gtid_sync $SS1 $M
        wait_for_gtid_sync $SS1 $S
        wait_for_gtid_sync $SS1 $SS2

        assert_equal [status $M sync_partial_ok] [expr $orig_sync_partial_ok_M+1]
        assert_equal [status $S sync_partial_ok] [expr $orig_sync_partial_ok_S+2]
        assert_equal [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue_S+1]
        assert_equal [get_info_property $SS1 gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue_SS1+1]
        assert_equal [get_info_property $SS2 gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue_SS2+1]
    }

    test "repl mode change propagates to all replicas" {
        assert_equal [status $M gtid_repl_mode] xsync
        assert_equal [status $S gtid_repl_mode] xsync

        set orig_sync_full_M [status $M sync_full]
        set orig_sync_partial_ok_M [status $M sync_partial_ok]
        set orig_sync_full_S [status $S sync_full]
        set orig_sync_partial_ok_S [status $S sync_partial_ok]

        for {set i 0} {$i < 10} {incr i} {
            puts "chaos repl mode change: round - $i"
            set msg "+++++ round $i +++++"
            write_log_line -3 $msg
            write_log_line -2 $msg
            write_log_line -1 $msg
            write_log_line  0 $msg

            if {[expr {$i % 2}] == 0} {
                set gtid_enabled "yes"
                set gtid_repl_mode "xsync"
            } else {
                set gtid_enabled "no"
                set gtid_repl_mode "psync"
            }

            set load_hanler [start_write_load $M_host $M_port 5]
            after 1000
            $M config set gtid-enabled $gtid_enabled
            stop_write_load $load_hanler
            after 100

            wait_for_sync $S
            wait_for_sync $SS1
            wait_for_sync $SS2
            wait_for_gtid_sync $M $S
            wait_for_gtid_sync $M $SS1
            wait_for_gtid_sync $M $SS2
            wait_for_repl_mode_sync $M $S
            wait_for_repl_mode_sync $M $SS1
            wait_for_repl_mode_sync $M $SS2

            assert_equal [status $M sync_full] $orig_sync_full_M
            # assert_equal [status $M sync_partial_ok] [expr $orig_sync_partial_ok_M+$i]
            assert_equal [status $S sync_full] $orig_sync_full_S
            # assert_equal [status $S sync_partial_ok]  [expr $orig_sync_partial_ok_S+2*$i]

            # TODO 判断 数据一致
        }
    }
}
}
}
}

proc region_info_create {redis1 redis2 keeper} {
    set region_info [dict create]

    set redis1_host [lindex [$redis1 config get bind] 1]
    if {$redis1_host == ""} {
        set redis1_host "127.0.0.1"
    }
    dict set region_info "redis1" "client" $redis1
    dict set region_info "redis1" "host" $redis1_host
    dict set region_info "redis1" "port" [lindex [$redis1 config get port] 1]

    set redis2_host [lindex [$redis2 config get bind] 1]
    if {$redis2_host == ""} {
        set redis2_host "127.0.0.1"
    }
    dict set region_info "redis2" "client" $redis2
    dict set region_info "redis2" "host" $redis2_host
    dict set region_info "redis2" "port" [lindex [$redis2 config get port] 1]

    set keeper_host [lindex [$keeper config get bind] 1]
    if {$keeper_host == ""} {
        set keeper_host "127.0.0.1"
    }
    dict set region_info "keeper" "client" $keeper
    dict set region_info "keeper" "host" $keeper_host
    dict set region_info "keeper" "port" [lindex [$keeper config get port] 1]

    return $region_info
}

# setup master/slave/keeper replicaion link within region.
# role can be master/dr.
proc region_setup_topo {region_info_var_name role master_name} {
    upvar $region_info_var_name region_info

    if {$role == "master"} {
        if {$master_name == "redis1"} {
            set slave_redis "redis2"
        } elseif {$master_name == "redis2"} {
            set slave_redis "redis1"
        } else {
            error "unexpected master name"
        }

        set M [dict get $region_info $master_name client]
        set M_host [dict get $region_info $master_name host]
        set M_port [dict get $region_info $master_name port]
        set S [dict get $region_info $slave_redis client]
        set K [dict get $region_info keeper client]

        $M replicaof no one
        $S replicaof $M_host $M_port
        $K replicaof $M_host $M_port

        dict set region_info master_name $master_name
    } elseif {$role == "dr"} {
        set K [dict get $region_info keeper client]
        set K_host [dict get $region_info keeper host]
        set K_port [dict get $region_info keeper port]
        set S1 [dict get $region_info redis1 client]
        set S2 [dict get $region_info redis2 client]

        $K replicaof no one
        $S1 replicaof $K_host $K_port
        $S2 replicaof $K_host $K_port

        dict set region_info master_name $master_name
    } else {
        error "unexpcted region role"
    }
}

# setup replication link across region.
proc region_setup_dr {master_ri dr_ri} {
    set dr_keeper [dict get $dr_ri keeper client]
    set M_host [dict get $master_ri keeper host]
    set M_port [dict get $master_ri keeper port]
    $dr_keeper replicaof $M_host $M_port
}

proc region_start_write_load {region_info_var_name} {
    upvar $region_info_var_name region_info
    if {[dict exists $region_info master_name]} {
        if {[dict get $region_info master_name] != "unset"} {
            if {[dict exists $region_info loader]} {
                stop_write_load [dict get $region_info loader]
            }
            set mi_info [dict get $region_info [dict get $region_info master_name]]
            set loader [start_write_load [dict get $mi_info host] [dict get $mi_info port] 3600]
            dict set region_info loader $loader
        }
    }
}

proc region_stop_write_load {region_info} {
    if {[dict exists $region_info loader]} {
        stop_write_load [dict get $region_info loader]
    }
}

proc region_wait_for_gtid_sync {region_info} {
    wait_for_gtid_sync [dict get $region_info redis1 client] [dict get $region_info redis2 client]
    wait_for_gtid_sync [dict get $region_info redis1 client] [dict get $region_info keeper client]
}

proc region_wait_for_sync {region_info} {
    if {[dict get $region_info master_name] != "redis1"} {
        wait_for_sync [dict get $region_info redis1 client]
    }
    if {[dict get $region_info master_name] != "redis2"} {
        wait_for_sync [dict get $region_info redis2 client]
    }
    wait_for_sync [dict get $region_info keeper client]
}

start_server {tags {"xsync"} overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {

    set A_info [region_info_create [srv -5 client] [srv -4 client] [srv -3 client]]
    set B_info [region_info_create [srv -2 client] [srv -1 client] [srv  0 client]]

    test "xsync chaos: init" {
        region_setup_topo A_info master redis1
        region_setup_topo B_info dr "unset"
        region_setup_dr $A_info $B_info

        region_wait_for_sync $A_info
        region_wait_for_sync $B_info
        region_wait_for_gtid_sync $A_info
        region_wait_for_gtid_sync $B_info
        wait_for_gtid_sync [dict get $A_info keeper client] [dict get $B_info keeper client]
    }

    test "xsync chaos: failover" {
        set orig_sync_full_redis1 [status [dict get $A_info redis1 client] sync_full]
        set orig_sync_full_redis2 [status [dict get $A_info redis2 client] sync_full]

        for {set i 0} {$i < 10} {incr i} {
            puts "xsync chaos: failover - round $i"

            if {[expr {$i % 2}] == 0} {
                set A_master "redis1"
            } else {
                set A_master "redis2"
            }

            region_start_write_load A_info

            after 1000

            # mock active failover
            region_setup_topo A_info master $A_master
            after 100
            region_stop_write_load $A_info

            region_wait_for_gtid_sync $A_info
            region_wait_for_gtid_sync $B_info
            wait_for_gtid_sync [dict get $A_info keeper client] [dict get $B_info keeper client]

            assert_equal [status [dict get $A_info redis1 client] sync_full] $orig_sync_full_redis1
            assert_equal [status [dict get $A_info redis2 client] sync_full] $orig_sync_full_redis2
        }
    }

    test "xsync chaos: active dr" {
        for {set i 0} {$i < 10} {incr i} {
            puts "xsync chaos: active dr - round $i"

            if {[expr {$i % 4}] < 2} {
                set master_name "redis1"
            } else {
                set master_name "redis2"
            }

            if {[expr {$i % 2}] == 0} {
                set A_role "master"
                set A_master $master_name
                set B_role "dr"
                set B_master "unset"
                upvar 0 A_info master_ri
                upvar 0 B_info dr_ri
            } else {
                set A_role "dr"
                set A_master "unset"
                set B_role "master"
                set B_master $master_name
                upvar 0 B_info master_ri
                upvar 0 A_info dr_ri
            }

            set orig_sync_full_A_redis1 [status [dict get $A_info redis1 client] sync_full]
            set orig_sync_full_A_redis2 [status [dict get $A_info redis2 client] sync_full]
            set orig_sync_full_B_redis1 [status [dict get $B_info redis1 client] sync_full]
            set orig_sync_full_B_redis2 [status [dict get $B_info redis2 client] sync_full]

            region_stop_write_load $A_info
            region_stop_write_load $B_info

            region_setup_topo A_info $A_role $A_master
            region_setup_topo B_info $B_role $B_master
            region_setup_dr $master_ri $dr_ri

            region_wait_for_sync $A_info
            region_wait_for_sync $B_info

            region_start_write_load master_ri
            after 1000
            region_stop_write_load $master_ri

            region_wait_for_gtid_sync $A_info
            region_wait_for_gtid_sync $B_info
            wait_for_gtid_sync [dict get $A_info keeper client] [dict get $B_info keeper client]

            assert_equal [status [dict get $A_info redis1 client] sync_full] $orig_sync_full_A_redis1
            assert_equal [status [dict get $A_info redis2 client] sync_full] $orig_sync_full_A_redis2
            assert_equal [status [dict get $B_info redis1 client] sync_full] $orig_sync_full_B_redis1
            assert_equal [status [dict get $B_info redis2 client] sync_full] $orig_sync_full_B_redis2
        }
    }

    test "xsync chaos: passive dr" {
        for {set i 0} {$i < 10} {incr i} {
            puts "xsync chaos: passive dr - round $i"

            if {[expr {$i % 4}] < 2} {
                set master_name "redis1"
            } else {
                set master_name "redis2"
            }

            if {[expr {$i % 2}] == 0} {
                set A_role "master"
                set A_master $master_name
                set B_role "dr"
                set B_master "unset"
                upvar 0 A_info master_ri
                upvar 0 B_info dr_ri
            } else {
                set A_role "dr"
                set A_master "unset"
                set B_role "master"
                set B_master $master_name
                upvar 0 B_info master_ri
                upvar 0 A_info dr_ri
            }

            set orig_sync_full_A_redis1 [status [dict get $A_info redis1 client] sync_full]
            set orig_sync_full_A_redis2 [status [dict get $A_info redis2 client] sync_full]
            set orig_sync_full_B_redis1 [status [dict get $B_info redis1 client] sync_full]
            set orig_sync_full_B_redis2 [status [dict get $B_info redis2 client] sync_full]

            # before topo change
            region_start_write_load master_ri
            region_start_write_load dr_ri
            after 1000 ; # wait write loader ready

            # topo change
            region_setup_topo A_info $A_role $A_master
            after 100 ; # single client ops < 10000/0.1, parital sync accepted
            region_setup_topo B_info $B_role $B_master
            after 100 ; # single client ops < 10000/0.1, parital sync accepted
            region_setup_dr $master_ri $dr_ri

            region_wait_for_sync $A_info
            region_wait_for_sync $B_info

            # after topo change
            after 1000
            region_stop_write_load $A_info
            region_stop_write_load $B_info

            region_wait_for_gtid_sync $A_info
            region_wait_for_gtid_sync $B_info
            wait_for_gtid_sync [dict get $A_info keeper client] [dict get $B_info keeper client]

            assert_equal [status [dict get $A_info redis1 client] sync_full] $orig_sync_full_A_redis1
            assert_equal [status [dict get $A_info redis2 client] sync_full] $orig_sync_full_A_redis2
            assert_equal [status [dict get $B_info redis1 client] sync_full] $orig_sync_full_B_redis1
            assert_equal [status [dict get $B_info redis2 client] sync_full] $orig_sync_full_B_redis2
        }
    }

}
}
}
}
}
}

