proc log_file_matches {log pattern} {
    set fp [open $log r]
    set content [read $fp]
    close $fp
    string match $pattern $content
}

proc assert_no_master_link {r own_port peer where} {
    assert_equal "master" [status $r role]
    set masterlinks {}
    foreach line [split [$r client list] "\n"] {
        if {[string match "*flags=M*" $line]} { lappend masterlinks $line }
    }
    if {[llength $masterlinks] != 0} {
        fail "$where: role is master but a master link still exists: $masterlinks"
    }
    # A ghost self replica advertises our own listening port (this is the shape
    # seen in production, where master and replica both listen on 6379).
    foreach line [split [$r info replication] "\n"] {
        if {[string match "slave*port=$own_port,*" $line]} {
            fail "$where: we registered ourselves as our own replica: [string trim $line]"
        }
    }
    # A ghost link to the old master shows up as an extra replica on the peer.
    if {$peer ne ""} {
        set peer_slaves [status $peer connected_slaves]
        if {$peer_slaves != 0} {
            fail "$where: peer still counts $peer_slaves replica(s), we are replicating from it while claiming to be a master"
        }
    }
}

start_server {tags {"repl swap"} overrides {}} {
    start_server {} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]

        set slave [srv 0 client]
        set slave_port [srv 0 port]
        set slave_log [srv 0 stdout]

        set keycount 300

        test {ratelimit repro: setup cold keyspace} {
            $slave slaveof $master_host $master_port
            wait_for_sync $slave
            for {set i 0} {$i < $keycount} {incr i} {
                $master set key_$i val_$i
            }
            wait_keyspace_cold $master
            wait_keyspace_cold $slave
            assert_equal [status $slave role] slave
        }

        test {SLAVEOF NO ONE while master client is ratelimit paused must not replicate from itself} {
            # Arm the swap ratelimiter so that replicated commands pause the
            # master client for SWAP_RATELIMIT_PAUSE_MAX_MS (200ms).
            # maxmemory must be set in one step from 0, otherwise
            # maxmemory_scale_from only scales down gradually and never arms.
            # Note that the ratelimiter compares against ctrip_getUsedMemory(),
            # which is well below INFO's used_memory (cold filters excluded).
            $slave config set swap-ratelimit-policy pause
            $slave config set swap-ratelimit-maxmemory-pause-growth-rate 1
            $slave config set maxmemory 0
            $slave config set maxmemory 3mb

            set master_rd [redis_deferring_client -1]
            set pause_observed_rounds 0
            set rounds 10

            for {set round 0} {$round < $rounds} {incr round} {
                if {[status $slave role] eq "master"} {
                    $slave slaveof $master_host $master_port
                }
                wait_for_sync $slave

                set pauses_before [getInfoProperty [$slave info swap] \
                        swap_ratelimit_client_pause_count]

                # Keep the replica's master client swapping so that the
                # ratelimiter pauses it.
                for {set i 0} {$i < $keycount} {incr i} {
                    $master_rd append key_$i x
                }

                # Deterministic precondition: don't guess with a fixed sleep,
                # wait until the ratelimiter actually paused a client.
                set pause_observed 0
                for {set i 0} {$i < 100} {incr i} {
                    if {[getInfoProperty [$slave info swap] \
                            swap_ratelimit_client_pause_count] > $pauses_before} {
                        set pause_observed 1
                        incr pause_observed_rounds
                        break
                    }
                    after 20
                }

                $slave slaveof no one
                for {set i 0} {$i < $keycount} {incr i} { $master_rd read }

                if {!$pause_observed} continue

                # replicationCron dials once per second: give it time to expose
                # a broken repl_state, then assert on the state itself.
                after 1500
                assert_no_master_link $slave $slave_port $master "round $round"
                if {[log_file_matches $slave_log "*Connecting to MASTER (null):*"]} {
                    fail "round $round: replicationCron dialed MASTER (null): repl_state == REPL_STATE_CONNECT while masterhost == NULL"
                }
            }

            $master_rd close

            # Guard against a vacuous pass: if the ratelimiter never paused a
            # client, SLAVEOF NO ONE never ran inside a pause window and the race
            # was not exercised at all. That would happen for instance if the
            # ratelimiter thresholds, config names or arming rules change.
            if {$pause_observed_rounds == 0} {
                fail "swap ratelimiter never paused any client in $rounds rounds, the test did not exercise the race"
            }
        }

        test {swap ratelimit pause must not defer freeClient} {
            $master config set swap-ratelimit-policy pause
            $master config set swap-ratelimit-maxmemory-pause-growth-rate 1
            $master config set maxmemory 0
            $master config set maxmemory 3mb
            wait_keyspace_cold $master

            set victim [redis_deferring_client -1]
            $victim client setname ratelimit_victim
            $victim read

            set pauses_before [getInfoProperty [$master info swap] \
                    swap_ratelimit_client_pause_count]

            # A cold read submits key requests and then gets ratelimit paused.
            $victim get key_0

            set pause_observed 0
            for {set i 0} {$i < 100} {incr i} {
                if {[getInfoProperty [$master info swap] \
                        swap_ratelimit_client_pause_count] > $pauses_before} {
                    set pause_observed 1
                    break
                }
                after 20
            }
            if {!$pause_observed} {
                catch {$victim close}
                fail "ratelimiter did not pause the victim client, nothing was exercised"
            }
            # Let the swap itself finish (sub-millisecond) so that the client has
            # no in flight key requests: those are deferred by a different
            # mechanism (deferFreeClient) which we are not testing here.
            after 20

            set victim_line ""
            foreach line [split [$master client list] "\n"] {
                if {[string match "*name=ratelimit_victim*" $line]} { set victim_line $line }
            }
            assert {$victim_line ne ""}
            regexp {id=(\d+)} $victim_line _ victim_id

            assert_equal 1 [$master client kill id $victim_id]

            # No sleep, no timing threshold: a synchronous free already happened
            # while CLIENT KILL was being executed.
            set still_there 0
            foreach line [split [$master client list] "\n"] {
                if {[string match "*name=ratelimit_victim*" $line]} { set still_there 1 }
            }
            catch {$victim close}
            if {$still_there} {
                fail "a ratelimit paused client survived CLIENT KILL: freeClient() was deferred, so pausing must not be implemented with protectClient()"
            }

            $master config set maxmemory 0
        }

        # Since pausing no longer defers the free, a paused master client can be
        # unlinked while paused: unlinkClient() cancels the resume time event.
        # When the link is merely broken (not SLAVEOF NO ONE) the client object
        # is cached (replicationCacheMaster) and resurrected after a successful
        # PSYNC (replicationResurrectCachedMaster). If the pause state were not
        # dropped together with the time event, the resurrected master client
        # would keep CLIENT_SWAP_RATELIMIT_PAUSED forever, and
        # handleClientsWithPendingWrites() would skip it: REPLCONF ACKs would
        # never reach the master, so the lag the master sees keeps growing
        # (breaking WAIT and min-replicas-max-lag), until the next ratelimit
        # pause/resume cycle happens to clear the flag.
        test {ratelimit paused master client resurrected after PSYNC keeps sending ACKs} {
            if {[status $slave role] eq "master"} {
                $slave slaveof $master_host $master_port
            }
            wait_for_sync $slave

            set master_rd [redis_deferring_client -1]
            set rounds 3

            for {set round 0} {$round < $rounds} {incr round} {
                # Arm the ratelimiter on the replica.
                $slave config set swap-ratelimit-policy pause
                $slave config set swap-ratelimit-maxmemory-pause-growth-rate 1
                $slave config set maxmemory 0
                $slave config set maxmemory 3mb

                set pauses_before [getInfoProperty [$slave info swap] \
                        swap_ratelimit_client_pause_count]
                set partial_before [status $master sync_partial_ok]

                for {set i 0} {$i < $keycount} {incr i} {
                    $master_rd append key_$i z
                }
                wait_for_condition 200 5 {
                    [getInfoProperty [$slave info swap] \
                            swap_ratelimit_client_pause_count] > $pauses_before
                } else {
                    fail "round $round: ratelimiter never paused the master client, nothing was exercised"
                }

                # Break the link while the master client is paused, then disarm
                # the ratelimiter so that no later pause/resume cycle can clear
                # a stale flag behind our back.
                assert_equal 1 [$slave client kill type master]
                $slave config set maxmemory 0
                for {set i 0} {$i < $keycount} {incr i} { $master_rd read }

                wait_for_condition 100 100 {
                    [status $slave master_link_status] eq "up"
                } else {
                    fail "round $round: replication link did not come back"
                }
                # The cached master client must have been resurrected, otherwise
                # this round did not exercise the code path at all.
                wait_for_condition 50 100 {
                    [status $master sync_partial_ok] > $partial_before
                } else {
                    fail "round $round: reconnection was not a partial resync, the cached master client was not resurrected"
                }

                # The replica sends REPLCONF ACK every second, so the lag seen
                # by the master normally stays at 0 or 1. A stale pause flag
                # makes it grow by one every second.
                set max_lag 0
                for {set s 0} {$s < 4} {incr s} {
                    after 1000
                    set line [lsearch -inline [split [$master info replication] "\r\n"] "slave0:*"]
                    if {![regexp {lag=(\d+)} $line _ lag]} {
                        fail "round $round: cannot parse replica lag from '$line'"
                    }
                    if {$lag > $max_lag} { set max_lag $lag }
                }
                if {$max_lag > 2} {
                    fail "round $round: master side lag reached $max_lag seconds after the paused master client was resurrected: REPLCONF ACKs are not sent, the ratelimit pause flag survived unlinkClient()"
                }
            }

            $master_rd close
            $slave config set maxmemory 0
        }
    }
}
