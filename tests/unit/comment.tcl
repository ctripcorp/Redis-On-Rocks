start_server {tags {"comment"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set master_socket [socket $master_host $master_port]

    test {comment capability} {
        $master config set swap-comment-enabled no
        assert_error "ERR unknown command `/*comment*/`*" {$master "/*comment*/" set k v}
    }

    test {replication with comment} {
        $master config set swap-comment-enabled yes
        set replconf "REPLCONF capa eof capa psync2 capa rordb capa comment"
        set repl [attach_to_replication_stream $replconf]

        assert_match OK [$master "/*comment*/" flushdb]
        assert_match OK [$master "/*comment*/" set k v]
        assert_match OK [$master multi]
        assert_match QUEUED [$master del k]
        assert_match 1 [$master exec]
        
        assert_replication_stream $repl {
            {select *}
            {/*comment*/ flushdb}
            {/*comment*/ set k v}
            {multi}
            {del k}
            {exec}
        }
    }

    test {replication without comment} {
        set replconf "REPLCONF capa eof capa psync2 capa rordb"
        set repl [attach_to_replication_stream $replconf]
        assert_match OK [$master "/*comment*/" flushdb]
        assert_match OK [$master "/*comment*/" set k v]
        assert_match OK [$master multi]
        assert_match QUEUED [$master del k]
        assert_match 1 [$master exec]
        
        assert_replication_stream $repl {
            {select *}
            {flushdb}
            {set k v}
            {multi}
            {del k}
            {exec}
        }
    }

    start_server {} {
        set slave [srv 0 client]
        set slave_host [srv 0 host]
        set slave_port [srv 0 port]

        set slave_socket [socket $slave_host $slave_port]

        test {set slave} {
            eval $slave "\"/* comment hello */\" SLAVEOF $master_host $master_port"
            wait_for_condition 50 100 {
                [lindex [$slave role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$slave info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }
        }
        
        test {no txn} {
            assert_match OK [$master "/*comment*/" flushdb]
            assert_match OK [$master "/*comment*/" set k v]
            assert_match 1 [$master "/*comment*/" hset myhash field1 hello]
            assert_match hello [$slave "/*comment*/" hget myhash field1]
            assert_match v [$slave "/*comment*/" get k]
        }

        test {txn} {
            assert_match OK [$master "/*comment*/" flushdb]
            assert_match OK [$master "/*comment*/" multi]
            assert_match QUEUED [$master "/*comment*/" set k v]
            assert_match QUEUED [$master "/*comment*/" hset myhash field1 hello]
            assert_match "OK 1" [$master "/*comment*/" exec]
            assert_match hello [$slave "/*comment*/" hget myhash field1]
            assert_match v [$slave "/*comment*/" get k]

            assert_match OK [$master "/*comment*/" flushdb]
            assert_match OK [$master "/*comment*/" multi]
            assert_match QUEUED [$master "/*comment*/" set k v]
            assert_match QUEUED [$master "/*comment*/" hset myhash field1 hello]
            assert_match "OK" [$master "/*comment*/" discard]
            assert_match "" [$slave "/*comment*/" hget myhash field1]
            assert_match "" [$slave "/*comment*/" get k]
        }

        test {lua notxn} {
            catch {[$master eval "return redis.call('/*comment*/', 'set','foo',12345)" 1 foo]} {err}
            assert_match "*Unknown Redis command called from Lua script*" $err
        }
    }
}