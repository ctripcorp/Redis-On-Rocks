start_server {tags {"import mode"} overrides {}}  {
    test "import start end" {

        r import start
        assert_equal [r import status] {1}
        assert_range [r import get ttl] 3595 3600

        assert_equal [r import end] {OK}
        assert_equal [r import status] {0}

        r import start 120
        assert_equal [r import status] {1}
        assert_range [r import get ttl] 115 120

        r import end
        assert_equal [r import status] {0}

    }

    test "import mode restart" {

        r import start
        assert_equal [r import status] {1}

        # not yet ending, restarting directing 
        r import start 120
        assert_range [r import get ttl] 115 120

    }

    test "import set get " {

        r import start
        assert_equal [r import set ttl 300] {OK}
        assert_range [r import get ttl] 295 300

        assert_equal [r import get expire] {0}

        assert_equal [r import set expire 1] {OK}
        assert_equal [r import get expire] {1}

        # not ending, restart, options will be inherited
        r import start
        assert_equal [r import get expire] {1}

        # after ending, then starting, options are default value 
        r import end

        r import start
        assert_equal [r import get expire] {0}

        r import end
    }

    test "import passive-expire " {
        r flushdb

        r setex key1 1 k
        r setex key2 1 k
        r setex key3 1 k

        r import start


        after 1500

        assert_equal [r get key1] {}

        assert_equal [r dbsize] {3}

        assert_equal [r import set expire 1] {OK}

        assert_equal [r get key1] {}
        assert_equal [r get key2] {}
        assert_equal [r get key3] {}

        assert_equal [r dbsize] {0}

        assert_equal [r import set expire 0] {OK}

        r setex key1 1 k

        after 1500
        assert_equal [r get key1] {}

        assert_equal [r dbsize] {1}

        r import end
    }

    test "import active-expire " {
        r flushdb

        r import start

        assert_equal [r import set expire 1] {OK}
        assert_equal [r import get expire] {1}

        r setex key1 1 k
        r setex key2 1 k
        r setex key3 1 k

        after 1500

        assert_equal [r dbsize] {0}

        assert_equal [r get key1] {}

        assert_equal [r import set expire 0] {OK}

        r setex key1 1 k
        r setex key2 1 k
        r setex key3 1 k

        after 1500

        assert_equal [r get key1] {}

        assert_equal [r dbsize] {3}

        r import end
    }

    test "import end works " {
    
        r flushdb

        r setex key1 1 k
        r setex key2 1 k
        r setex key3 1 k

        r import start

        after 1500
        assert_equal [r dbsize] {3}

        r import end

        after 1500
        assert_equal [r dbsize] {0}
    }

    test "illegal operation " {

        r import start

        assert_equal [r import get expire] {0}

        assert_equal [r import set expire 1] {OK}

        assert_equal [r import set expire no] {OK}

        assert_error "*Invalid option*"  {r import get auto-compaction}

        assert_error "*not an integer*"  {r import set ttl no}

        assert_error "*Invalid option*"   {r import set auto-compaction ok}

        r import end

        assert_error "*already ended*"   {r import end}

        assert_error "*IMPORT GET must be*"   {r import get expire}

        assert_error "*IMPORT SET must be*"   {r import set expire 1}

        assert_error "*not an integer*"   {r import start six}

        assert_error "*Invalid subcommand*"   {r import set auto-compaction ok no 1}
    }
}