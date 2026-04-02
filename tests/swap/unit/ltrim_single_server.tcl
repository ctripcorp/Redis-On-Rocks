start_server {tags {"swap" "ltrim_single_server"}} {
    test {LTRIM on warm key meta drift - single server Bug 1 reproduction} {
        # Bug 1: When a key is warm [HOT(h), COLD(c)] with c > 32,
        # LTRIM's swapAna triggers an incomplete swap-in (Bug 2) that
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

        # Disable eviction BEFORE warming up (critical ordering!)
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
        # Bug 2 triggers when LTRIM end ≤ 33 (left_padding = end-1 ≤ 32).
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

    test {LTRIM on fully COLD key works correctly (no Bug 2)} {
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
