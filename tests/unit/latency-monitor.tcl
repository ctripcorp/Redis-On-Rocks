start_server {tags {"latency-monitor"}} {
    # Set a threshold high enough to avoid spurious latency events.
    r config set latency-monitor-threshold 200
    r latency reset

    test {Test latency events logging} {
        r debug sleep 0.3
        after 1100
        r debug sleep 0.4
        after 1100
        r debug sleep 0.5
        assert {[r latency history command] >= 3}
    }

    test {LATENCY HISTORY output is ok} {
        set min 250
        set max 450
        foreach event [r latency history command] {
            lassign $event time latency
            if {!$::no_latency} {
                assert {$latency >= $min && $latency <= $max}
            }
            incr min 100
            incr max 100
            set last_time $time ; # Used in the next test
        }
    }

    test {LATENCY LATEST output is ok} {
        foreach event [r latency latest] {
            lassign $event eventname time latency max
            assert {$eventname eq "command"}
            if {!$::no_latency} {
                assert {$max >= 450 & $max <= 650}
                assert {$time == $last_time}
            }
            break
        }
    }

    test {LATENCY HISTORY / RESET with wrong event name is fine} {
        assert {[llength [r latency history blabla]] == 0}
        assert {[r latency reset blabla] == 0}
    }

    test {LATENCY DOCTOR produces some output} {
        assert {[string length [r latency doctor]] > 0}
    }

    test {LATENCY RESET is able to reset events} {
        assert {[r latency reset] > 0}
        assert {[r latency latest] eq {}}
    }

    tags {memonly} {
    test {LATENCY of expire events are correctly collected} {
        r config set latency-monitor-threshold 20
        r flushdb
        if {$::valgrind} {set count 100000} else {set count 1000000}
        r eval {
            local i = 0
            while (i < tonumber(ARGV[1])) do
                redis.call('sadd',KEYS[1],i)
                i = i+1
             end
        } 1 mybigkey $count
        r pexpire mybigkey 50
        wait_for_condition 5 100 {
            [r dbsize] == 0
        } else {
            fail "key wasn't expired"
        }
        assert_match {*expire-cycle*} [r latency latest]
    }
    }

    test {LATENCY HELP should not have unexpected options} {
        catch {r LATENCY help xxx} e
        assert_match "*Unknown subcommand or wrong number of arguments*" $e
    }
}
