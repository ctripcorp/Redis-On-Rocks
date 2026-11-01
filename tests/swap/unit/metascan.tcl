start_server {tags "metascan"} {
    r config set swap-debug-evict-keys 0; # evict manually

    test {scan with invalid cursor} {
        catch {r scan a count 1} e
        assert_match {*invalid cursor*} $e
        catch {r scan 10a count 1} e
        assert_match {*invalid cursor*} $e
    }

    test {scan with unsuccessive cold cursor} {
        r mset a a b b c c
        r swap.evict a b c
        wait_key_cold r a
        wait_key_cold r b
        wait_key_cold r c

        lassign [r scan 0 count 1] cursor0 res0
        assert_equal $res0 {}
        lassign [r scan $cursor0 count 1] cursor1 res1
        assert_equal $res1 {a}
        catch {r scan $cursor0 count 1} e
        assert_match {*Swap fail*} $e

        # drain scan to release session
        r scan $cursor1 count 10
        r del a b c
    }

    test {scan switch from hot to cold rewinds cold cursor} {
        r mset a a b b c c
        r swap.evict a b c
        wait_key_cold r a
        wait_key_cold r b
        wait_key_cold r c

        lassign [r scan 0 count 1] cursor0 res0
        assert_equal $res0 {}
        lassign [r scan $cursor0 count 1] cursor1 res1
        assert_equal $res1 {a}
        lassign [r scan 0 count 1] cursor2 res2
        assert_equal $res2 {}

        # drain scans to release sessions
        r scan $cursor1 count 10
        r scan $cursor2 count 10
        r del a b c
    }

    test {scan} {
        r set key val
        lassign [r scan 0] cur0 res0
        assert_equal $res0 {key}
        assert_equal [r scan $cur0] {0 {}}
        r swap.evict key
        wait_key_cold r key
        lassign [r scan 0] cur1 res1
        assert_equal $res1 {}
        assert_equal [r scan $cur1] {0 key}
    }

    test {randomkey in multi} {
        r set key val
        r swap.evict key
        wait_key_cold r key
        r multi
        r randomkey
        r randomkey
        r del key
        r exec
    } {key key 1}

    test {scan in multi} {
        r set key val
        r swap.evict key
        wait_key_cold r key
        assert {[rio_get_meta r key] != ""}
        r multi
        r scan 1
        # not supported yet
        #assert_equal [lindex [r exec] 1] {key}
        catch {r exec} e
        assert_match {*EXECABORT*Swap fail*} $e
        r del key
    }
}


