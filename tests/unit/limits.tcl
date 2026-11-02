start_server {tags {"limits network"} overrides {maxclients 10}} {
    if {$::tls} {
        set expected_code "*I/O error*"
    } else {
        set expected_code "*ERR max*reached*"
    }
    test {Check if maxclients works refusing connections} {
        set c 0
        catch {
            while {$c < 50} {
                incr c
                set rd [redis_deferring_client]
                $rd ping
                $rd read
                after 100
            }
        } e
        assert {$c > 8 && $c <= 10}
        set e
    } $expected_code
}

start_server {tags {"limits network"} overrides {maxclients 100 min-reserved-fds 64}} {
    test {min-reserved-fds is loaded from the config file} {
        assert_equal [lindex [r config get min-reserved-fds] 1] 64
    }

    test {CONFIG SET min-reserved-fds same value and decrease keep the server up} {
        assert_equal [r config set min-reserved-fds 64] {OK}
        assert_equal [lindex [r config get min-reserved-fds] 1] 64
        assert_equal [r config set min-reserved-fds 32] {OK}
        assert_equal [lindex [r config get min-reserved-fds] 1] 32
        assert_equal [r ping] {PONG}
    }

    test {CONFIG SET min-reserved-fds increase applies when the fd budget allows} {
        set soft unlimited
        catch {set soft [string trim [exec sh -c {ulimit -Sn}]]}
        # 32 -> 64 stays inside the limit established at startup. A further
        # increase resizes the event loop and is skipped when the soft limit
        # cannot cover maxclients + the new reservation.
        if {$soft ne "unlimited" && $soft < (100 + 128)} {
            assert_equal [r config set min-reserved-fds 64] {OK}
            assert_equal [lindex [r config get min-reserved-fds] 1] 64
        } else {
            assert_equal [r config set min-reserved-fds 128] {OK}
            assert_equal [lindex [r config get min-reserved-fds] 1] 128
            assert_equal [r config set min-reserved-fds 64] {OK}
        }
        assert_equal [lindex [r config get min-reserved-fds] 1] 64
        assert_equal [r ping] {PONG}
    }

    test {CONFIG SET min-reserved-fds above the event-loop limit rolls back} {
        set cur [lindex [r config get min-reserved-fds] 1]
        assert_error "*not able to handle*" {
            r config set min-reserved-fds 2147483647
        }
        assert_equal [lindex [r config get min-reserved-fds] 1] $cur
        assert_equal [r ping] {PONG}
    }

    test {CONFIG SET min-reserved-fds UINT_MAX does not wrap around} {
        set cur [lindex [r config get min-reserved-fds] 1]
        assert_error "*not able to handle*" {r config set min-reserved-fds 4294967295}
        assert_equal [lindex [r config get min-reserved-fds] 1] $cur
        assert_equal [r ping] {PONG}
    }

    test {CONFIG SET min-reserved-fds below the minimum is rejected} {
        set cur [lindex [r config get min-reserved-fds] 1]
        assert_error "*between 32 and*" {r config set min-reserved-fds 31}
        assert_equal [lindex [r config get min-reserved-fds] 1] $cur
    }

    test {CONFIG SET maxclients to the same or a smaller value keeps the server up} {
        assert_equal [r config set maxclients 100] {OK}
        assert_equal [lindex [r config get maxclients] 1] 100
        assert_equal [r config set maxclients 50] {OK}
        assert_equal [lindex [r config get maxclients] 1] 50
        assert_equal [r ping] {PONG}
    }

    test {CONFIG SET maxclients increase applies when the fd budget allows} {
        set soft unlimited
        catch {set soft [string trim [exec sh -c {ulimit -Sn}]]}
        # Startup only clamps RLIMIT_NOFILE when the soft limit is below
        # maxclients + min-reserved-fds. Above that, raising back past the
        # original maxclients also resizes the event loop.
        if {$soft ne "unlimited" && $soft < (200 + 64)} {
            assert_equal [r config set maxclients 100] {OK}
            assert_equal [lindex [r config get maxclients] 1] 100
        } else {
            assert_equal [r config set maxclients 200] {OK}
            assert_equal [lindex [r config get maxclients] 1] 200
        }
        assert_equal [r ping] {PONG}
    }

    test {CONFIG SET maxclients above the event-loop limit rolls back} {
        set cur [lindex [r config get maxclients] 1]
        assert_error "*not able to handle the specified number of clients*" {
            r config set maxclients 2147483647
        }
        assert_equal [lindex [r config get maxclients] 1] $cur
        assert_equal [r ping] {PONG}
    }

    test {CONFIG SET maxclients UINT_MAX does not wrap around} {
        set cur [lindex [r config get maxclients] 1]
        assert_error "*not able to handle*" {r config set maxclients 4294967295}
        assert_equal [lindex [r config get maxclients] 1] $cur
        assert_equal [r ping] {PONG}
    }

    test {CONFIG SET maxclients below the minimum is rejected} {
        set cur [lindex [r config get maxclients] 1]
        assert_error "*between 1 and*" {r config set maxclients 0}
        assert_equal [lindex [r config get maxclients] 1] $cur
    }
}

# The tests below read /proc/<pid>/limits and are Linux only.
if {[file exists /proc/self/limits]} {

proc get_nofile {pid kind} {
    # kind: soft | hard
    set fd [open /proc/$pid/limits r]
    set content [read $fd]
    close $fd
    foreach line [split $content "\n"] {
        if {[string match "Max open files*" $line]} {
            set f [regexp -all -inline {\S+} $line]
            if {$kind eq "soft"} { return [lindex $f 3] }
            return [lindex $f 4]
        }
    }
    return ""
}

# Uses its own server: prlimit lowers the hard limit, which can't be undone.
start_server {tags {"limits network"} overrides {maxclients 100 min-reserved-fds 64}} {
    set pid [srv 0 pid]

    test {CONFIG SET maxclients beyond what setrlimit allows fails and rolls back} {
        set cur [lindex [r config get maxclients] 1]
        set soft_before [get_nofile $pid soft]
        # 2000000 + 64 exceeds fs.nr_open (1048576 by default), so setrlimit
        # fails even for root.
        assert_error "*increase 'ulimit -n'*" {r config set maxclients 2000000}
        assert_equal [lindex [r config get maxclients] 1] $cur
        assert_equal [get_nofile $pid soft] $soft_before
        assert_equal [r ping] {PONG}
    }

    test {CONFIG SET min-reserved-fds beyond what setrlimit allows fails and rolls back} {
        set cur [lindex [r config get min-reserved-fds] 1]
        set soft_before [get_nofile $pid soft]
        assert_error "*increase 'ulimit -n'*" {r config set min-reserved-fds 2000000}
        assert_equal [lindex [r config get min-reserved-fds] 1] $cur
        assert_equal [get_nofile $pid soft] $soft_before
        assert_equal [r ping] {PONG}
    }

    # Raising the hard limit or modifying another process needs privileges
    # (CAP_SYS_RESOURCE, same uid, seccomp may block prlimit64). So never raise
    # the hard limit: use at most the current one, and probe that prlimit
    # really works on the server pid before running the tests below.
    set test_hard [get_nofile $pid hard]
    if {$test_hard eq "unlimited" || $test_hard > 100000} {
        set test_hard 100000
    }
    set can_prlimit 0
    if {$test_hard >= 1128 && ![catch {exec prlimit --version}]} {
        if {![catch {exec prlimit --pid $pid --nofile=200:$test_hard 2>@1} err]} {
            set can_prlimit 1
        } else {
            puts "prlimit unusable in this environment, skipping: $err"
        }
    }

    if {$can_prlimit} {
        test {CONFIG SET raises only the soft NOFILE limit and keeps the hard limit} {
            exec prlimit --pid $pid --nofile=200:$test_hard 2>@1
            assert_equal [r config set maxclients 1000] {OK}
            assert_equal [get_nofile $pid soft] 1064
            assert_equal [get_nofile $pid hard] $test_hard
            assert_equal [r config set min-reserved-fds 128] {OK}
            assert_equal [get_nofile $pid soft] 1128
            assert_equal [get_nofile $pid hard] $test_hard
            assert_equal [r ping] {PONG}
        }

        test {CONFIG SET fails and rolls back when the hard NOFILE limit is too low} {
            exec prlimit --pid $pid --nofile=1128:1128 2>@1
            assert_error "*increase 'ulimit -n'*" {r config set maxclients 5000}
            assert_equal [lindex [r config get maxclients] 1] 1000
            assert_error "*increase 'ulimit -n'*" {r config set min-reserved-fds 5000}
            assert_equal [lindex [r config get min-reserved-fds] 1] 128
            # Decreasing never needs more fds and leaves the limit untouched.
            assert_equal [r config set maxclients 500] {OK}
            assert_equal [get_nofile $pid soft] 1128
            assert_equal [get_nofile $pid hard] 1128
            assert_equal [r ping] {PONG}
        }

        test {Raised maxclients actually accepts more clients} {
            assert_equal [r config set maxclients 300] {OK}
            set clients {}
            for {set i 0} {$i < 250} {incr i} {
                set rd [redis_deferring_client]
                $rd ping
                assert_equal [$rd read] {PONG}
                lappend clients $rd
            }
            foreach rd $clients { $rd close }
        }
    }
}

tags {"limits"} {
    test {Server exits at startup when NOFILE can't cover maxclients + min-reserved-fds} {
        set log [file join [tmpdir server.nofile] stdout]
        catch {
            exec sh -c "ulimit -n 1024; exec src/redis-server --port 0 --maxclients 10000 --save '' > $log 2>&1"
        }
        assert_match "*Can't set maximum open files to 10032*Exiting*" [exec cat $log]
    }
}

}
