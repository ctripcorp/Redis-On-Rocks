start_server {tags "multi"} {
    r config set swap-debug-evict-keys 0; # evict manually

    test {transaction reserves cmd intention flag} {
        r set key val
        r swap.evict key
        wait_key_cold r key
        assert {[rio_get_meta r key] != ""}
        r multi
        r del key
        r exec
        assert {[rio_get_meta r key] == ""}
    }

    proc initList {r list cnt} {
        set i 0
        $r del $list
        while {$i < $cnt} {
            $r rpush $list $i
            incr i 1
        }
    }

    test {multi args rewrite} {
        # list len should gt SEGMENT_MAX_PADDING which defined in ctrip_swap_list.c
        initList r mlist1 64
        initList r mlist2 64
        initList r mlist3 64
        initList r mlist4 64
        r swap.evict mlist1 mlist2 mlist3 mlist4
        wait_key_cold r mlist1
        wait_key_cold r mlist2
        wait_key_cold r mlist3
        wait_key_cold r mlist4

        # query index should gt SEGMENT_MAX_PADDING and make args rewrite
        r multi
        r lindex mlist1 41
        r lindex mlist2 42
        r lindex mlist3 43
        r lindex mlist4 44
        assert_equal [r exec] {41 42 43 44}
    }

    test {multi bitmap arg rewrite} {
        r del mbitmap1
        r setbit mbitmap1 0 1
        r setbit mbitmap1 65536 1
        r swap.evict mbitmap1
        wait_key_cold r mbitmap1

        r multi
        r getbit mbitmap1 65536
        r setbit mbitmap1 65537 1
        r getbit mbitmap1 65537
        assert_equal [r exec] {1 0 1}
    }

    test {bitmap bitcount and bitpos with BIT unit on cold subkeys} {
        r del bittest
        r setbit bittest 0 1
        r setbit bittest 65536 1
        r swap.evict bittest
        wait_key_cold r bittest

        assert_equal [r bitcount bittest 65536 65536 bit] 1
        r swap.evict bittest
        wait_key_cold r bittest

        assert_equal [r bitpos bittest 1 65536 65536 bit] 65536
        r swap.evict bittest
        wait_key_cold r bittest

        assert_equal [r bitcount bittest 0 -1 bit] 2
    }

    test {cold subkey no-op write preserves data across eviction} {
        # ZADD NX on existing cold member
        r del ztest
        r zadd ztest 10 m1 20 m2
        r swap.evict ztest
        wait_key_cold r ztest
        assert_equal [r zadd ztest nx 30 m1] 0
        r swap.evict ztest
        wait_key_cold r ztest
        assert_equal [r zscore ztest m1] 10
        assert_equal [r zrange ztest 0 -1 withscores] {m1 10 m2 20}

        # SMOVE when src and dst are the same cold set
        r del ssrc
        r sadd ssrc a b
        r swap.evict ssrc
        wait_key_cold r ssrc
        assert_equal [r smove ssrc ssrc a] 1
        r swap.evict ssrc
        wait_key_cold r ssrc
        assert_equal [r sismember ssrc a] 1

        # HSETEX FNX on existing cold hash field
        r del htest
        r hset htest f1 v1 f2 v2
        r swap.evict htest
        wait_key_cold r htest
        assert_equal [r hsetex htest fnx ex 100 fields 1 f1 v1_new] 0
        r swap.evict htest
        wait_key_cold r htest
        assert_equal [r hget htest f1] v1
    }

    test {multi invalid select restores db and does not UAF} {
        r select 0
        r set mselect_key val0
        r select 1
        r set mselect_key val1
        r swap.evict mselect_key
        wait_key_cold r mselect_key
        r select 0
        r swap.evict mselect_key
        wait_key_cold r mselect_key

        r multi
        r get mselect_key
        r select 1
        r select -1
        r set mselect_key val1_new
        catch {r exec} e
        assert_match {*DB index is out of range*} $e
        r select 1
        assert_equal [r get mselect_key] val1_new
        r select 0
        assert_equal [r get mselect_key] val0
    }

    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    start_server {tags "multi-slave"} {
        set slave [srv 0 client]
        set slave_host [srv 0 host]
        set slave_port [srv 0 port]

        $slave slaveof $master_host $master_port

        test {multi arg rewrite and propagate correctly} {
            initList $master mlist1 64
            initList $master mlist2 64
            $master swap.evict mlist1 mlist2
            wait_key_cold $master mlist1
            wait_key_cold $master mlist2

            $master multi
            $master LSET mlist1 41 1
            $master LSET mlist2 42 2
            $master exec
            wait_for_ofs_sync $master $slave

            assert_equal [$slave lindex mlist1 41] {1}
            assert_equal [$slave lindex mlist2 42] {2}
        }
    }

}
