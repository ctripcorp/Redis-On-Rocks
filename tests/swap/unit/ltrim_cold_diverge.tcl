# Reproduction of CI crash: LTRIM cold-segment replica divergence causing LSET panic
#
# BUG:
#   When a list key is fully hot on the replica at LTRIM dispatch time
#   (swapAna returns SWAP_NOP, no meta exists), but eviction makes it warm
#   (meta with cold segments) BEFORE the LTRIM is actually applied,
#   two things go wrong:
#
#   1. listBeforeCall rewrites argv indices from logical to physical (memory)
#      indices.  For a logical index falling in a COLD segment,
#      listMetaGetMidx returns the wrong memory index, so the wrong
#      elements are retained.
#
#   2. swapListMetaDelRange only trims HOT segments from the head/tail of
#      the meta.  Cold segments logically outside the retained range are
#      left intact.
#
#   Result: replica meta.len >> master list len.  Over many operations the
#   divergence compounds.  Eventually master issues LSET at an index valid
#   on master but out of range on the replica -> panic:
#     "Guru Meditation: after processing command 'lset' #networking.c:615"
#
# KEY TRIGGER:
#   Eviction on BOTH master AND slave (`swap-debug-evict-keys -1`).
#   This causes the key to transition from HOT -> WARM between dispatch
#   and apply on the replica.
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
        # Master eviction causes keys to cycle through HOT/WARM/COLD states.
        # Slave eviction creates the hot-at-dispatch / warm-at-apply race.
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
