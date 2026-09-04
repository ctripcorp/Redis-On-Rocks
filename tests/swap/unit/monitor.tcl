set mport [find_available_port $::baseport $::portcount]
start_server [list overrides [list ctrip-monitor-port $mport] tags "ctrip_monitor"] {
    set redis_host [srv 0 host]
    set redis_port [srv 0 port]

    test {ignore accept after clients overflow} {
        r config set maxclients 1

        # trigger overflow
        set r2 [redis $redis_host $redis_port 0 0]
        catch {$r2 ping} error
        assert_match $error "ERR max number of clients reached"
        after 100

        # read timeout for clients overflow
        set s [socket $redis_host $redis_port]
        fconfigure $s -blocking 0
        puts $s ping
        flush $s
        after 100
        assert_match [read -nonewline $s] {}

        r config set maxclients 1000
        after 100
        assert_match [read -nonewline $s] +PONG
    }

    test {monitor port work when clients overflow} {
        r config set maxclients 1
        set r2 [redis $redis_host $redis_port 0 0]
        catch {$r2 ping} error
        assert_match $error "ERR max number of clients reached"

        set m [redis $redis_host $mport 0 0]
        assert_match [$m ping] "PONG"

        $m config set maxclients 1000
        set r3 [redis $redis_host $redis_port 0 0]
        assert_match [$r3 ping] "PONG"
    }

    test {CONFIG SET min-reserved-fds accepts a valid value at runtime} {
        # Boundary: a valid value can be set and CONFIG GET returns the same value
        # Monitor port still serves connections (verifies the change does not break existing behavior)
        r config set min-reserved-fds 64
        assert_equal 64 [lindex [r config get min-reserved-fds] 1]
        set m [redis $redis_host $mport 0 0]
        assert_match [$m ping] "PONG"
        # Restore
        r config set min-reserved-fds 32
    }

    test {CONFIG SET min-reserved-fds huge value rejected without killing server} {
        # Key regression (BUG 1): when CONFIG SET min-reserved-fds exceeds current RLIMIT_NOFILE,
        # it must return an error and the CONFIG framework must roll back to the previous value;
        # the server must NOT be killed by exit(1).
        # UINT_MAX (4294967295) is used to trigger the failure path;
        # catch + if-else keeps the test stable in the rare case where ulimit is unusually high.
        if {[catch {r config set min-reserved-fds 4294967295} err]} {
            # Failure path: server must still respond to PING and the previous value must remain
            assert_equal PONG [r ping]
            assert_equal 32 [lindex [r config get min-reserved-fds] 1]
        } else {
            # Rare environments where ulimit >= UINT_MAX: configuration succeeds and value must match
            assert_equal 4294967295 [lindex [r config get min-reserved-fds] 1]
            r config set min-reserved-fds 32
        }
    }

}