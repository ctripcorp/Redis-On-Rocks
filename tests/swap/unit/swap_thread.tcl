start_server {tags {"swap_thread"} overrides {save ""}} {
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set master [srv 0 client]

    test {swap_thread} {
        
        # save 100 key
        for {set i 0} {$i < 100} {incr i} {
            $master set $i $i
            $master swap.evict $i
        }
        
        assert_equal [string match "*swap_thread5:*" [$master info swap]] 1
        assert_equal [string match "*swap_thread6:*" [$master info swap]] 0
        $master config set swap-core-threads 8
        # when swap > 16 , swap 2 batch
        $master mget 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 
        assert_equal [string match "*swap_thread6:*" [$master info swap]] 1
        # create one thread
        assert_equal [string match "*swap_thread7:*" [$master info swap]] 0
        # waiting for refresh times
        after 100
        # next create
        $master get 17
        assert_equal [string match "*swap_thread7:*" [$master info swap]] 1
        
        $master config set swap-core-threads 4
        $master config set swap-check-threads-cycle 10
        # test wait  1.5s not Scaling thread
        after 1500 
        assert_equal [string match "*swap_thread7:*" [$master info swap]] 1
        $master config set swap-idle-thread-keep-alive-seconds 1
        #
        after 1500 
        assert_equal [string match "*swap_thread7:*" [$master info swap]] 0

        
    }


}