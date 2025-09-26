start_server {tags {"swap_thread"} overrides {save ""}} {
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set master [srv 0 client]
    test {swap-threads-auto-scale set} {
        catch {$master config set swap-threads 15} error 
        assert_equal [string match {*ERR *} $error] 1

    
    }
    test {swap_thread} {
        
        # save 100 key
        for {set i 0} {$i < 100} {incr i} {
            $master set $i $i
            $master swap.evict $i
        }
        assert_equal [string match "*swap_thread_num:4*" [$master info swap]] 1
        $master config set swap-threads 8
        assert_equal [$master config get swap-threads] "swap-threads 8"

        # when swap > 16 , swap 2 batch
        $master mget 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 
        assert_equal [string match "*swap_thread_num:5*" [$master info swap]] 1
        # waiting for refresh times
        after 100
        # next create
        $master get 17
        assert_equal [string match "*swap_thread_num:6*" [$master info swap]] 1
        
        $master config set swap-threads 4
        assert_equal [$master config get swap-threads] "swap-threads 4"
        # test wait  2s not Scaling thread
        after 2000 
        assert_equal [$master swap.debug thread auto-scale-down check] 0
        assert_equal [string match "*swap_thread_num:6*" [$master info swap]] 1
        $master config set swap-threads-auto-scale-down-idle-seconds 1
        assert_equal [$master swap.debug thread auto-scale-down check] 1
        assert_equal [string match "*swap_thread_num:5*" [$master info swap]] 1
        assert_equal [string match "*swap_thread6:inflight_reqs=*" [$master swap.debug thread list]] 1

        assert_equal [$master swap.debug thread auto-scale-up check] 0
        assert_equal [string match "*swap_thread_num:5*" [$master info swap]] 1
        assert_equal [$master swap.debug thread auto-scale-up] 1
        assert_equal [string match "*swap_thread_num:6*" [$master info swap]] 1
        assert_equal [$master swap.debug thread auto-scale-down check] 0
        assert_equal [string match "*swap_thread_num:6*" [$master info swap]] 1
        assert_equal [$master swap.debug thread auto-scale-down ] 1
        assert_equal [string match "*swap_thread_num:5*" [$master info swap]] 1

        # up to max
        for {set i 5} {$i < 12} {incr i} {
            assert_equal [$master swap.debug thread auto-scale-up] 1
        }
        # up fail
        assert_equal [$master swap.debug thread auto-scale-up] 0
        # down to min
        for {set i 4} {$i < 12} {incr i} {
            assert_equal [$master swap.debug thread auto-scale-down] 1
        }
        # down fail
        assert_equal [$master swap.debug thread auto-scale-down] 0
        # check thread num 
        assert_equal [string match "*swap_thread_num:4*" [$master info swap]] 1
        # test function ok
        assert_equal [$master get 18] 18
    }


}