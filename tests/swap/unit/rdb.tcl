start_server {overrides {save ""} tags {"swap" "rdb"}} {
    test {keyspace and rocksdb obj encoding not binded} {
        r config set hash-max-ziplist-entries 5
        for {set i 0} {$i < 10} {incr i} {
            r hset hashbig field_$i val_$i
        }
        # saved as hashtable in rocksdb
        r swap.evict hashbig;
        assert_equal [r object encoding hashbig] hashtable

        # decoded as hashtable in rocksdb, loaded as ziplist
        r swap.evict hashbig;
        r config set hash-max-ziplist-entries 512
        assert_equal [r object encoding hashbig] ziplist

        # saved as ziplist rocksdb, loaded as ziplist
        r swap.evict hashbig
        assert_equal [r object encoding hashbig] ziplist
    }

    test {rdbsave and rdbload} {
        r config set swap-debug-evict-keys 0

        test {memkey} {
            r flushdb
            r hmset memkey1 a a b b 1 1 2 2 
            r hmset memkey2 a a b b 1 1 2 2 
            r hmset memkey3 a a b b 1 1 2 2 
            r debug reload
            assert_equal [r dbsize] 3
            assert_equal [r hmget memkey1 a b 1 2] {a b 1 2}
        }

        test {wholekey reload} {
            r flushdb
            r hmset wholekey_hot a a b b 1 1 2 2 
            r hmset wholekey_cold a a b b 1 1 2 2 
            r swap.evict wholekey_cold
            wait_key_cold r wholekey_cold
            assert_equal [object_meta_len r wholekey_hot] 0
            assert_equal [object_meta_len r wholekey_cold] 4

            r debug reload

            assert_equal [r dbsize] 2
            assert_equal [r hmget wholekey_hot a b 1 2] {a b 1 2}
            assert_equal [r hmget wholekey_cold a b 1 2] {a b 1 2}
        }

        test {bighash reload} {
            set old_entries [lindex [r config get hash-max-ziplist-entries] 1]
            r config set hash-max-ziplist-entries 1

            # init data
            r flushdb
            r hmset hot a a b b 1 1 2 2 
            r hmset warm a a 1 1
            r swap.evict warm
            r hmset warm b b 2 2
            r hmset cold a a b b 1 1 2 2 
            r swap.evict cold
            wait_key_cold r cold
            assert_equal [object_meta_len r hot] 0
            assert_equal [object_meta_len r warm] 2
            assert_equal [object_meta_len r cold] 4
            # reload
            r debug reload
            # check
            assert_equal [object_meta_len r hot] 4
            assert_equal [object_meta_len r warm] 4
            assert_equal [object_meta_len r cold] 4

            r config set hash-max-ziplist-entries $old_entries
        }

        test {bighash lazy del} {
            set old_entries [lindex [r config get hash-max-ziplist-entries] 1]
            r config set hash-max-ziplist-entries 1

            r hmset myhash a a b b 1 1 2 2 
            r swap.evict myhash
            wait_key_cold r myhash
            r del myhash
            after 100
            r debug reload
            assert_equal [r exists myhash] 0

            r hmset myhash a A 1 11
            assert_equal [r hlen myhash] 2
            r swap.evict myhash 
            r hmset myhash b B 2 22
            assert_equal [r hlen myhash] 4

            r debug reload
            assert_equal [r hlen myhash] 4
            assert_equal [r hmget myhash a b 1 2] {A B 11 22}

            r config set hash-max-ziplist-entries $old_entries
        }

        test {rdb save & load with different encodings} {
            set old_entries [lindex [r config get hash-max-ziplist-entries] 1]
            r flushdb
            # init small hash
            r hmset myhash a a b b 1 1 2 2 
            r swap.evict myhash
            wait_key_cold r myhash
            # init big hash
            r hmset myhash a A b B 1 11 2 22
            r config set hash-max-ziplist-entries 1
            assert_equal [r hmget myhash a b 1 2] {A B 11 22}
            r swap.evict myhash
            wait_key_cold r myhash
            # cold reload from bighash to wholekey
            r config set hash-max-ziplist-entries $old_entries
            r debug reload
            assert_equal [r hmget myhash a b 1 2] {A B 11 22}
            # hot reload from wholekey to bighash
            r config set hash-max-ziplist-entries 1
            r debug reload
            assert_equal [r hmget myhash a b 1 2] {A B 11 22}
        }
    }

    test {rdbSaveRocks hold more than one rocksIterKeyTypeValue cause memory tread} {
        for {set i 0} {$i < 10000} {incr i} {
            r set key_$i val_$i
        }
        r debug reload
    }

    test {rdbsave and rdbload} {
        r mset a a aa aa aaa aaa
        r debug reload
        assert_equal [r mget a aa aaa] {a aa aaa}
    }
}
