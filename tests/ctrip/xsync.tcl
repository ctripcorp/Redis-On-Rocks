start_server {tags {"gtid-seq"} overrides {gtid-enabled yes}} {
    start_server {overrides {gtid-enabled yes}} {
    set master [srv -1 client]
    set master_host [srv -1 host]
    set master_port [srv -1 port]
    set slave [srv 0 client]

    test "master(X) slave(X): gtid not related => xfullresync" {
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

    test "master(X) slave(X): gap > maxgap => xfullresync" {
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

        verify_log_message -1 "*Partial xsync request from * rejected*gap*>*maxgap*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xfullresync] [expr $orig_xsync_xfullresync+1]

        $slave replicaof no one
    }

    test "master(X) slave(X): locate continue before prev repl mode => xfullresync" {
        assert_equal [status $master gtid_executed] [status $slave gtid_executed]

        assert_equal [status $master gtid_repl_mode] xsync
        $master set hello world
        assert {[status $master gtid_executed] != [status $slave gtid_executed]}

        $master config set gtid-enabled no
        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $master gtid_prev_repl_mode] xsync
        $master set hello world

        $master config set gtid-enabled yes
        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $master gtid_prev_repl_mode] psync
        $master set hello world

        set orig_log_lines [count_log_lines -1]
        set orig_sync_full [status $master sync_full]
        set orig_xsync_xfullresync [get_info_property $slave gtid gtid_sync_stat xsync_xfullresync]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert_equal [status $master gtid_executed] [status $slave gtid_executed]

        verify_log_message -1 "*Partial sync request from*rejected*locate repl mode failed*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xfullresync] [expr $orig_xsync_xfullresync+1]

        $slave replicaof no one
    }

    test "master(X) slave(X): gap <= maxgap => xcontinue" {
        assert_equal [status $master gtid_executed] [status $slave gtid_executed]

        $master set hello world
        $slave set foo bar

        assert {[status $master gtid_executed] != [status $slave gtid_executed]}

        set orig_log_lines [count_log_lines -1]
        set orig_sync_partial_ok [status $master sync_partial_ok]
        set orig_xsync_xcontinue [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue]
        set orig_master_lost [status $master gtid_lost]
        set orig_slave_lost [status $slave gtid_lost]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert {[status $master gtid_executed] != [status $slave gtid_executed]}
        verify_log_message -1 "*Partial xsync request from*accepted*gap=1*" $orig_log_lines
        assert_equal [status $master sync_partial_ok] [expr $orig_sync_partial_ok+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue+1]
        assert {[status $master gtid_lost] != $orig_master_lost}
        assert_equal [status $slave gtid_lost] $orig_slave_lost

        $slave replicaof no one
    }

    # TODO 需要优化为continue
    # master: | (P) set hello world_1; set hello world_2 | (X) set hello world_3 |
    # slave:  | (P) set hello world_1 |
    test "master(X) slave(P): offset < repl_mode.from => xfullresync" {
        $master config set gtid-enabled no
        $slave config set gtid-enabled no

        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $slave gtid_repl_mode] psync

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

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
        set orig_psync_xfullresync [get_info_property $slave gtid gtid_sync_stat psync_xfullresync]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert_equal [$slave get hello] world_3

        verify_log_message -1 "*Partial sync request from * rejected: slave psync but my repl mode is xsync*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_xfullresync] [expr $orig_psync_xfullresync+1]

        $slave replicaof no one
        $slave config set gtid-enabled yes
    }

    # master: | (psync) set hello world_1 | (X) set hello world_2 set hello world_3 |
    # slave:  | (psync) set hello world_1  set hello world_2 |
    test "master(X) slave(P): offset > repl_mode.from => xfullresync" {
        $master config set gtid-enabled no
        $slave config set gtid-enabled no

        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $slave gtid_repl_mode] psync

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

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

        assert_equal [$slave get hello] world_3

        verify_log_message -1 "*Partial sync request from * rejected: request repl mode(psync) not match located repl mode(xsync) at offset*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_xfullresync] [expr $orig_psync_xfullresync+1]

        $slave replicaof no one
        $slave config set gtid-enabled yes
    }

    # master: | (P) set hello world_1 | (X) set hello world_2 set hello world_3 |
    # slave:  | (P) set hello world_1 |
    test "master(X) slave(P): offset = repl_mode.from => xcontinue" {
        assert_equal [status $master gtid_repl_mode] xsync
        assert_equal [status $slave gtid_repl_mode] xsync

        $master config set gtid-enabled no
        $slave config set gtid-enabled no

        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $slave gtid_repl_mode] psync

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        $master set hello world_1
        wait_for_ofs_sync $master $slave
        assert_equal [$slave get hello] world_1

        $slave replicaof 127.0.0.1 0

        $master config set gtid-enabled yes
        $master set hello world_2
        $master set hello world_3

        set orig_log_lines [count_log_lines -1]
        set orig_sync_partial_ok [status $master sync_partial_ok]
        set orig_psync_xcontinue [get_info_property $slave gtid gtid_sync_stat psync_xcontinue]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        assert_equal [$slave get hello] world_3
        verify_log_message -1 "*Partial sync request from * accepted: repl mode switch from psync to xsync*" $orig_log_lines
        assert_equal [status $master sync_partial_ok] [expr $orig_sync_partial_ok+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_xcontinue] [expr $orig_psync_xcontinue+1]

        $slave replicaof no one
        $slave config set gtid-enabled yes
    }

    # master: | (P) set hello world_1 set hello world_2 | (X) set hello world_3 | (P) set hello world_4 |
    # slave:  | (P) set hello world_1 |
    test "master(P) slave(P): offset < prev_repl_mode.from => fullresync" {
        $master config set gtid-enabled no
        $slave config set gtid-enabled no

        assert_equal [status $master gtid_repl_mode] psync
        assert_equal [status $slave gtid_repl_mode] psync

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

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

        assert_equal [$slave get hello] world_4

        verify_log_message -1 "*Partial sync request from * rejected: locate repl mode failed*" $orig_log_lines
        assert_equal [status $master sync_full] [expr $orig_sync_full+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat psync_fullresync] [expr $orig_psync_fullresync+1]

        $slave replicaof no one
        $slave config set gtid-enabled yes
    }

    # test "master(P) slave(P): offset < repl_mode.from => fullresync" {
        # $master config set gtid-enabled no
        # $slave config set gtid-enabled no

        # assert_equal [status $master gtid_repl_mode] psync
        # assert_equal [status $slave gtid_repl_mode] psync

        # $slave replicaof $master_host $master_port
        # wait_for_sync $slave

        # $master set hello world_1
        # wait_for_ofs_sync $master $slave
        # assert_equal [$slave get hello] world_1

        # $slave replicaof 127.0.0.1 0

        # $master config set gtid-enabled yes
        # $master set hello world_2

        # $master config set gtid-enabled no
        # $master set hello world_3

        # # master: | (P) set hello world_1 | (X) set hello world_2 | (P) set hello world_3 |
        # # slave:  | (P) set hello world_1 |

        # set orig_log_lines [count_log_lines -1]
        # set orig_sync_full [status $master sync_full]
        # set orig_psync_fullresync [get_info_property $slave gtid gtid_sync_stat psync_fullresync]

        # $slave replicaof $master_host $master_port
        # wait_for_sync $slave

        # assert_equal [$slave get hello] world_3

        # verify_log_message -1 "*Partial sync request from * rejected: locate repl mode failed*" $orig_log_lines
        # assert_equal [status $master sync_full] [expr $orig_sync_partial_ok+1]
        # assert_equal [get_info_property $slave gtid gtid_sync_stat psync_fullresync] [expr $orig_psync_fullresync+1]

        # $slave replicaof no one
        # $slave config set gtid-enabled yes
    # }

    # master: | (X) set hello world_1 set hello world_2 | (P) set hello world_3 |
    # slave:  | (X) set hello world_1 |
    test "master(P) slave(X): locate(X), gap <= maxgap => xcontinue" {
        $master config set gtid-enabled yes
        $slave config set gtid-enabled yes

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        $master set hello world_1
        wait_for_ofs_sync $master $slave
        assert_equal [$slave get hello] world_1

        $slave replicaof 127.0.0.1 0

        $master set hello world_2

        $master config set gtid-enabled no
        $master set hello world_3

        set orig_log_lines [count_log_lines -1]
        set orig_sync_partial_ok [status $master sync_partial_ok]
        set orig_xsync_xcontinue [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue]
        set orig_xsync_continue [get_info_property $slave gtid gtid_sync_stat xsync_continue]

        $slave replicaof $master_host $master_port
        wait_for_sync $slave
        after 100
        wait_for_sync $slave

        assert_equal [$slave get hello] world_3

        verify_log_message -1 "*Partial xsync request from * accepted: gap=0*" $orig_log_lines
        assert_equal [status $master sync_partial_ok] [expr $orig_sync_partial_ok+2]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_xcontinue] [expr $orig_xsync_xcontinue+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_continue] [expr $orig_xsync_continue+1]

        $slave replicaof no one
    }

    # master: | (X) set hello world_1 | (P) set hello world_2a |
    # slave:  | (X) set hello world_1 ; set hello world_2b |
    test "master(P) slave(X): locate(P), gap <= maxgap => continue" {
        $master config set gtid-enabled yes
        $slave config set gtid-enabled yes

        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        $master set hello world_1
        after 100
        assert_equal [$slave get hello] world_1

        $slave replicaof no one
        $master config set gtid-enabled no

        $master set hello world_2a
        $slave set hello world_2b

        set orig_log_lines [count_log_lines -1]
        set orig_sync_partial_ok [status $master sync_partial_ok]
        set orig_xsync_continue [get_info_property $slave gtid gtid_sync_stat xsync_continue]

        $slave replicaof $master_host $master_port
        # wait_for_ofs_sync $master $slave
        after 100

        assert_equal [$slave get hello] world_2a

        # verify_log_message -1 "*Partial xsync request from * accepted: gap=1*" $orig_log_lines
        assert_equal [status $master sync_partial_ok] [expr $orig_sync_partial_ok+1]
        assert_equal [get_info_property $slave gtid gtid_sync_stat xsync_continue] [expr $orig_xsync_continue+1]

        $slave replicaof no one
    }

}
}
