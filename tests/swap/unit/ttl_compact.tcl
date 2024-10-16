start_server {tags {"ttl compact"}} {
    r config set swap-debug-evict-keys 0
    r config set swap-ttl-compact-period 40
    r config set swap-sst-age-limit-refresh-period 20
    r config set swap-swap-info-slave-period 20

    test {ttl compact on expired keys} {

        for {set j 0} { $j < 100} {incr j} {
            set mybitmap "mybitmap-$j"

            # bitmap is spilt as subkey of 4KB by default
            r setbit $mybitmap 32768 1
            r expire $mybitmap 10

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        } 

        # sst in L0 is forced to be compacted to L1
        r swap compact 

        # 60s, bigger than swap-ttl-compact-period, ensure that ttl compact happen
        after 60000

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]

        # expire seconds -> milliseconds
        assert_lessthan $sst_age_limit 10000

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_equal {1} $request_sst_count

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_equal {1} $compact_times

        # set keys again, to check info
        for {set j 100} { $j < 200} {incr j} {
            set mybitmap "mybitmap-$j"

            r setbit $mybitmap 32768 1
            r expire $mybitmap 20

            r swap.evict $mybitmap
            wait_key_cold r $mybitmap
        } 

        # sst in L0 is forced to be compacted to L1
        r swap compact 

        # 60s, bigger than default ttl compact period, ensure that ttl compact happen
        after 60000

        set sst_age_limit [get_info_property r Swap swap_ttl_compact sst_age_limit]

        # expire seconds -> milliseconds
        assert_lessthan $sst_age_limit 20000

        set request_sst_count [get_info_property r Swap swap_ttl_compact request_sst_count]
        assert_equal {2} $request_sst_count

        set compact_times [get_info_property r Swap swap_ttl_compact times]
        assert_equal {2} $compact_times
    }

    r flushdb
}

start_server {tags {"master propagate expire test"} overrides {save ""}} {

    start_server {overrides {swap-repl-rordb-sync {no} swap-debug-evict-keys {0} swap-swap-info-slave-period {10} swap-sst-age-limit-refresh-period {10}}} {

        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set master [srv 0 client]
        set slave_host [srv -1 host]
        set slave_port [srv -1 port]
        set slave [srv -1 client]
        $slave slaveof $master_host $master_port
        wait_for_sync $slave
        test {ttl compact master slave propagate check} {

            for {set j 0} { $j < 100} {incr j} {
                set mybitmap "mybitmap-$j"

                # bitmap is spilt as subkey of 4KB by default
                $master setbit $mybitmap 32768 1
                $master expire $mybitmap 10

                $master swap.evict $mybitmap
                wait_key_cold $master $mybitmap
            } 

            # more than swap-swap-info-slave-period
            after 10000
            # wait_for_ofs_sync $master $slave

            set sst_age_limit1 [get_info_property $master Swap swap_ttl_compact sst_age_limit]
            set sst_age_limit2 [get_info_property $slave Swap swap_ttl_compact sst_age_limit]

            assert_lessthan $sst_age_limit1 10000
            assert_equal $sst_age_limit1 $sst_age_limit2
        }
    }
}
