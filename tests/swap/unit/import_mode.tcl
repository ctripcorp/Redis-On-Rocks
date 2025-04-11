
start_server {tags {"import mode"} overrides {}}  {

    test "import expire cold keys " {
        r flushdb

        r import start

        r setex key1 1 1
        r swap.evict key1
        wait_key_cold r key1

        after 1500

        assert_equal [r dbsize] {1}
        assert_equal [r get key1] {}
        assert_equal [r dbsize] {1}
        r import end

        assert_equal [r get key1] {}
        assert_equal [r dbsize] {0}
    }
}