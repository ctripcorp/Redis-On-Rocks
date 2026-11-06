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

}

# default ctrip-monitor-port (framework always injects it, so omit it)
start_server [list omit [list ctrip-monitor-port] tags "ctrip_monitor"] {
    test {ctrip-monitor-port keeps its default value} {
        assert_equal 6380 [lindex [r config get ctrip-monitor-port] 1]
    }
}