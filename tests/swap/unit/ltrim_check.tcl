# Reproduction of CI crash: LTRIM error causing LSET panic
#
#   Eventually master issues LSET at an index valid on master but 
#   out of range on the replica -> panic:
#
# REPRODUCTION RATE (unfixed): ~60% panic within 10 seconds of load.

start_server {tags {"list" "repl"} overrides {save ""}} {
    start_server {overrides {save ""}} {
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        set master [srv 0 client]
        set slave  [srv -1 client]

        $slave slaveof $master_host $master_port
        wait_for_sync $slave

        # Eviction on BOTH sides is essential for reproduction.
        $master config set swap-debug-evict-keys -1
        $slave  config set swap-debug-evict-keys -1

        # Panic on replication error to detect the divergence immediately.
        $slave config set propagation-error-behavior panic-on-replicas

        test {swap-list LTRIM cold-segment repl divergence: no panic and master/replica consistent} {
            set num_keys 4
            set duration 10
            set num_loaders 4

            # Seed each key with 80 elements so there is plenty of cold data
            # once eviction starts.
            for {set i 0} {$i < $num_keys} {incr i} {
                $master del "ltrim-cold-guard-$i"
                for {set j 0} {$j < 80} {incr j} {
                    $master rpush "ltrim-cold-guard-$i" "seed:$i:$j"
                }
            }
            wait_for_ofs_sync $master $slave

            # Concurrent load: LMOVE, LTRIM, LSET, LPUSH, RPOP on shared keys.
            # LTRIM with indices spanning cold segments triggers the bug.
            # LSET on the highest valid index is what panics the replica.
            set handles {}
            for {set loader 0} {$loader < $num_loaders} {incr loader} {
                lappend handles [start_run_load $master_host $master_port $duration 0 {
                    set mykey "ltrim-cold-guard-[randomInt 4]"
                    set mylen [$r1 llen $mykey]
                    randpath {
                        $r1 LMOVE $mykey $mykey RIGHT LEFT
                    } {
                        if {$mylen > 3} {
                            $r1 LTRIM $mykey 1 [expr {$mylen - 2}]
                        } else {
                            $r1 LTRIM $mykey 0 $mylen
                        }
                    } {
                        if {$mylen > 0} {
                            catch {$r1 LSET $mykey [expr {$mylen - 1}] "v-[pid]"}
                        }
                    } {
                        $r1 LPUSH $mykey [pid] [pid]
                    } {
                        $r1 RPOP $mykey
                    }
                }]
            }

            after [expr {$duration * 1000}]
            wait_load_handlers_disconnected

            # Sync and verify master/replica content is identical.
            wait_for_ofs_sync $master $slave

            for {set i 0} {$i < $num_keys} {incr i} {
                set key "ltrim-cold-guard-$i"
                set md [$master debug digest-value $key]
                set sd [$slave  debug digest-value $key]
                assert_equal $md $sd \
                    "key $key: master/slave digest mismatch (replica list diverged)"
            }
        }
    }
}

start_server {tags {"swap" "ltrim_err_on_single_server"}} {
    test {LTRIM on warm key meta drift - single server Bug reproduction} {
        # Bug : When a key is warm [HOT(h), COLD(c)] with c > 32,
        # LTRIM's swapAna triggers an incomplete swap-in that
        # leaves the key warm. ltrimCommand then computes llen from only
        # HOT elements, missing the COLD tail trim. Meta drifts.
        #
        # Trigger conditions:
        # 1. List with > 34 elements (cold segment > 32 for PADDING limit)
        # 2. Evict to COLD, then warm up with LLEN (creates [HOT(1), COLD(n)])
        # 3. Disable eviction so warm state persists
        # 4. Run LTRIM that trims from both ends
        #
        # Detection: LLEN returns wrong value (meta.len drifted)

        set total 35
        set trim_start 1
        set trim_end 33
        set expected [expr {$trim_end - $trim_start + 1}]

        r del mylist

        # Seed list
        for {set i 0} {$i < $total} {incr i} {
            r rpush mylist "elem-$i"
        }
        assert_equal $total [r llen mylist]

        # Evict to fully COLD
        r config set swap-debug-evict-keys -1
        after 2000
        wait_key_cold r mylist

        # Disable eviction BEFORE warming up
        r config set swap-debug-evict-keys 0

        # LLEN warms the key: meta becomes [HOT(1), COLD(n-1)]
        assert_equal $total [r llen mylist]

        # LTRIM should keep elements [1, 33] = 33 elements
        r ltrim mylist $trim_start $trim_end

        # Verify: LLEN must equal expected, not total-1 or total
        set result [r llen mylist]
        assert_equal $expected $result \
            "LTRIM meta drift: expected $expected but got $result"

        # Re-enable eviction for cleanup
        r config set swap-debug-evict-keys -1
        after 1000
    }

    test {LTRIM meta drift accumulates over multiple operations} {
        # Repeated LTRIM on warm keys causes cumulative drift.
        # Bug triggers when LTRIM end ≤ 33 (left_padding = end-1 ≤ 32).
        # So we need lists > 34 elements with LTRIM end ≤ 33.
        set total 36

        r del mylist
        for {set i 0} {$i < $total} {incr i} {
            r rpush mylist "elem-$i"
        }

        # Round 1: LTRIM 1 33 on 36-element list (keep 33)
        r config set swap-debug-evict-keys -1
        after 2000
        wait_key_cold r mylist
        r config set swap-debug-evict-keys 0
        r llen mylist
        r ltrim mylist 1 33
        set len1 [r llen mylist]
        assert_equal 33 $len1 "Round 1: expected 33 got $len1"

        # Replenish to trigger again: add elements to exceed 34
        for {set i 0} {$i < 3} {incr i} {
            r rpush mylist "extra-$i"
        }
        # Now list has 33+3 = 36 elements (if round 1 was correct)
        # or 34+3 = 37 (if buggy)

        # Round 2: LTRIM 1 33 again
        r config set swap-debug-evict-keys -1
        after 2000
        wait_key_cold r mylist
        r config set swap-debug-evict-keys 0
        r llen mylist
        r ltrim mylist 1 33
        set len2 [r llen mylist]
        assert_equal 33 $len2 "Round 2: expected 33 got $len2"

        # Cleanup
        r config set swap-debug-evict-keys -1
        after 1000
    }

    test {LTRIM on fully COLD key works correctly} {
        # When key is fully COLD, swap-in computes non-overlapping segments
        # and LTRIM works correctly. This is the control case.
        set total 35

        r del mylist
        for {set i 0} {$i < $total} {incr i} {
            r rpush mylist "elem-$i"
        }

        # Evict to fully COLD
        r config set swap-debug-evict-keys -1
        after 2000
        wait_key_cold r mylist

        # Disable eviction but DON'T warm up — key stays cold
        r config set swap-debug-evict-keys 0

        # LTRIM directly on cold key
        r ltrim mylist 1 33
        set result [r llen mylist]
        assert_equal 33 $result \
            "LTRIM on cold key: expected 33 got $result"

        # Cleanup
        r config set swap-debug-evict-keys -1
        after 1000
    }
}
