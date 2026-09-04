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

    # === min-reserved-fds boundary tests ===
    # Purpose: cover input validation and runtime regression points for CONFIG SET min-reserved-fds
    # Defaults: default 32, lower_bound 32, upper_bound UINT_MAX

    test {CONFIG SET min-reserved-fds accepts a valid value} {
        # Boundary 1: a valid value can be set and CONFIG GET returns the same value
        assert_equal OK [r config set min-reserved-fds 64]
        assert_equal 64 [lindex [r config get min-reserved-fds] 1]
        # Restore to default to avoid polluting subsequent tests
        r config set min-reserved-fds 32
    }

    test {CONFIG SET min-reserved-fds rejects value below lower bound (32)} {
        # Boundary 2: values below 32 must be rejected and the existing value must remain
        assert_error "*inclusive*" {r config set min-reserved-fds 16}
        assert_equal 32 [lindex [r config get min-reserved-fds] 1]
    }

    test {CONFIG SET min-reserved-fds rejects non-integer value} {
        # Boundary 3: non-integer values must be rejected ("argument couldn't be parsed into an integer")
        assert_error "*integer*" {r config set min-reserved-fds abc}
    }

    test {CONFIG SET min-reserved-fds does not modify maxclients} {
        # Boundary 4: changing min-reserved-fds must not silently change maxclients
        # Regression point: the original adjustOpenFilesLimit path used to mutate maxclients
        set original_maxclients [lindex [r config get maxclients] 1]
        r config set min-reserved-fds 64
        assert_equal $original_maxclients [lindex [r config get maxclients] 1]
        # Restore
        r config set min-reserved-fds 32
    }

    test {CONFIG SET min-reserved-fds shrink succeeds} {
        # Boundary 5: grow then shrink; CONFIG stays consistent (does not depend on ae setsize shrinking)
        r config set min-reserved-fds 96
        assert_equal OK [r config set min-reserved-fds 32]
        assert_equal 32 [lindex [r config get min-reserved-fds] 1]
    }

    test {CONFIG SET min-reserved-fds huge value rejected without killing server} {
        # Key regression (BUG 1): when CONFIG SET min-reserved-fds exceeds current RLIMIT_NOFILE,
        # it must return an error and the CONFIG framework must roll back to the previous value;
        # the server must NOT be killed by exit(1).
        # UINT_MAX (4294967295) is used to trigger the failure path (far beyond any common ulimit);
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

    # === maxclients boundary tests ===
    # Purpose: cover the same risk classes that min-reserved-fds guards against,
    # because updateMaxclients and updateMinReservedFds both rely on adjustOpenFilesLimit
    # and share tryResizeSetAeSize.

    test {CONFIG SET maxclients huge value rejected without silently reducing maxclients} {
        # Regression for the same BUG 1 class as min-reserved-fds:
        # adjustOpenFilesLimit may silently shrink server.maxclients when ulimit is too small,
        # and (in extreme cases) call exit(1) when bestlimit <= server.min_reserved_fds.
        # updateMaxclients guards against silent shrinkage by rolling server.maxclients back to prev,
        # and the CONFIG framework also rolls back when update_fn returns 0.
        # UINT_MAX (4294967295) triggers the failure path on any common ulimit;
        # catch + if-else keeps the test stable in the rare case where ulimit is unusually high.
        set original [lindex [r config get maxclients] 1]
        if {[catch {r config set maxclients 4294967295} err]} {
            # Failure path: maxclients must remain at its previous value
            assert_equal $original [lindex [r config get maxclients] 1]
            assert_match "*not able to handle*" $err
        } else {
            # Rare environments where ulimit >= UINT_MAX: configuration succeeds and value must match
            assert_equal 4294967295 [lindex [r config get maxclients] 1]
            r config set maxclients $original
        }
        # Server must still be alive
        assert_equal PONG [r ping]
    }

    test {CONFIG SET maxclients smaller value accepted} {
        # val < prev path: updateMaxclients returns 1 without touching adjustOpenFilesLimit
        # or tryResizeSetAeSize (the ae array is not shrunk — same behavior as min-reserved-fds shrink).
        # Verify CONFIG consistency and that the server still responds.
        assert_equal OK [r config set maxclients 5]
        assert_equal 5 [lindex [r config get maxclients] 1]
        # Restore
        r config set maxclients 10
    }
}
