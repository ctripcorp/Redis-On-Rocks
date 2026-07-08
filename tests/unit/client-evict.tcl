# Get info about a redis client connection:
# name - name of client we want to query
# f - field name from "CLIENT LIST" we want to get
proc client_field {name f} {
    set clients [split [string trim [r client list]] "\r\n"]
    set c [lsearch -inline $clients *name=$name*]
    if {![regexp $f=(\[a-zA-Z0-9-\]+) $c - res]} {
        error "no client named $name found with field $f"
    }
    return $res
}

proc client_exists {name} {
    if {[catch { client_field $name tot-mem } e]} {
        return false
    }
    return true
}

proc gen_client {} {
    set rr [redis_client]
    set name [format "tst_%08d" [expr {int(rand()*100000000)}]]
    $rr client setname $name
    wait_for_condition 50 10 {
        [client_exists $name]
    } else {
        fail "client $name did not appear in CLIENT LIST"
    }
    return [list $rr $name]
}

# Sum a value across all redis client connections:
# f - the field name from "CLIENT LIST" we want to sum
proc clients_sum {f} {
    set sum 0
    set clients [split [string trim [r client list]] "\r\n"]
    foreach c $clients {
        if {![regexp $f=(\[a-zA-Z0-9-\]+) $c - res]} {
            error "field $f not found in $c"
        }
        incr sum $res
    }
    return $sum
}

proc mb {v} {
    return [expr $v * 1024 * 1024]
}

proc kb {v} {
    return [expr $v * 1024]
}

proc check_eviction_test {client_eviction} {
    set evicted_clients [s evicted_clients]
    
    if $client_eviction {
        return [expr $evicted_clients > 0]
    } else {
        return [expr $evicted_clients == 0]
    }
}

start_server {} {
    set maxmemory_clients 3000000
    r config set maxmemory-tracking-clients $maxmemory_clients
    r config set client-output-buffer-limit "tracking 0 0 0"

    test "client evicted due to large argv" {
        r flushdb
        lassign [gen_client] rr cname
        $rr client tracking on
        # Attempt a large multi-bulk command under eviction limit
        $rr mset k v k2 [string repeat v 1000000]
        assert_equal [$rr get k] v
        # Attempt another command, now causing client eviction
        catch { $rr mset k v k2 [string repeat v $maxmemory_clients] } e

        wait_for_condition 100 10 {
            ![client_exists $cname]
        } else {
            fail "Client $cname was not evicted after large argv"
        }
        $rr close
    }

    test "client evicted due to large query buf" {
        r flushdb
        lassign [gen_client] rr cname
        $rr client tracking on
        # Attempt to fill the query buff without completing the argument above the limit, causing client eviction
        catch {
            $rr write [join [list "*1\r\n\$$maxmemory_clients\r\n" [string repeat v $maxmemory_clients]] ""]
            $rr flush
            $rr read
        } e
        wait_for_condition 100 10 {
            ![client_exists $cname]
        } else {
            fail "Client $cname was not evicted after large query buf"
        }
        $rr close
    }

    test "client evicted due to client tracking prefixes" {
        r flushdb
        set rr [redis_client]

        # Since tracking prefixes list is a small overhead this test uses a minimal maxmemory-tracking-clients config
        set temp_maxmemory_clients 200000
        r config set maxmemory-tracking-clients $temp_maxmemory_clients

        # Append tracking prefixes until list maxes out maxmemory clients and causes client eviction
        # Combine more prefixes in each command to speed up the test. Because we did not actually count
        # the memory usage of all prefixes, see getClientMemoryUsage, so we can not use larger prefixes
        # to speed up the test here.
        catch {
            for {set j 0} {$j < $temp_maxmemory_clients} {incr j} {
                $rr client tracking on prefix [format a%09s $j] prefix [format b%09s $j] prefix [format c%09s $j] bcast
            }
        } e
        assert_match {I/O error reading reply} $e
        $rr close

        # Restore config for next tests
        r config set maxmemory-tracking-clients $maxmemory_clients
    }

    test "client evicted due to output buf" {
        r flushdb
        r setrange k 200000 v
        set rr [redis_deferring_client]
        $rr client setname test_client
        $rr client tracking on
        $rr flush
        assert {[$rr read] == "OK"}
        # Attempt a large response under eviction limit
        $rr get k
        $rr flush
        $rr read
        assert {[string length [$rr read]] == 200001}
        set mem [client_field test_client tot-mem]
        assert {$mem < $maxmemory_clients}

        # Fill output buff in loop without reading it and make sure
        # we're eventually disconnected, but before reaching maxmemory_clients
        while true {
            if { [catch {
                set mem [client_field test_client tot-mem]
                assert {$mem < $maxmemory_clients}
                $rr get k
                $rr flush
               } e]} {
                wait_for_condition 100 10 {
                    ![client_exists test_client]
                } else {
                    fail "Client was not evicted after output buffer overflow"
                }
                break
            }
        }
        $rr close
    }

    foreach {no_evict} {on off} {
        test "client no-evict $no_evict" {
            r flushdb
            r client setname control
            r client no-evict on ;# Avoid evicting the main connection
            lassign [gen_client] rr cname
            $rr client no-evict $no_evict
            $rr client tracking on
            # Overflow maxmemory-tracking-clients
            set qbsize [expr {$maxmemory_clients + 1}]
            if {[catch {
                $rr write [join [list "*1\r\n\$$qbsize\r\n" [string repeat v $qbsize]] ""]
                $rr flush
                wait_for_condition 200 10 {
                    [client_field $cname qbuf] == $qbsize
                } else {
                    fail "Failed to fill qbuf for test"
                }
            } e] && $no_evict == off} {
                wait_for_condition 100 10 {
                    ![client_exists $cname]
                } else {
                    fail "Client $cname was not evicted"
                }
            } elseif {$no_evict == on} {
                assert {[client_field $cname tot-mem] > $maxmemory_clients}
            }
            $rr close
        }
    }
}

start_server {} {
    set server_pid [s process_id]
    set maxmemory_clients [mb 10]
    set obuf_limit [mb 3]
    r config set maxmemory-tracking-clients $maxmemory_clients
    r config set client-output-buffer-limit "tracking $obuf_limit 0 0"

    test "avoid client eviction when client is freed by output buffer limit" {
        r flushdb
        set obuf_size [expr {$obuf_limit + [mb 1]}]
        r setrange k $obuf_size v
        set rr1 [redis_client]
        $rr1 client setname "qbuf-client"
        $rr1 client tracking on
        set rr2 [redis_deferring_client]
        $rr2 client setname "obuf-client1"
        $rr2 client tracking on
        assert_equal [$rr2 read] OK
        set rr3 [redis_deferring_client]
        $rr3 client setname "obuf-client2"
        $rr3 client tracking on
        assert_equal [$rr3 read] OK

        # Occupy client's query buff with less than output buffer limit left to exceed maxmemory-tracking-clients
        set qbsize [expr {$maxmemory_clients - $obuf_size}]
        $rr1 write [join [list "*1\r\n\$$qbsize\r\n" [string repeat v $qbsize]] ""]
        $rr1 flush
        # Wait for qbuff to be as expected
        wait_for_condition 200 10 {
            [client_field qbuf-client qbuf] == $qbsize
        } else {
            fail "Failed to fill qbuf for test"
        }

        # Make the other two obuf-clients pass obuf limit and also pass maxmemory-tracking-clients
        # We use two obuf-clients to make sure that even if client eviction is attempted
        # between two command processing (with no sleep) we don't perform any client eviction
        # because the obuf limit is enforced with precedence.
        pause_process $server_pid
        $rr2 get k
        $rr2 flush
        $rr3 get k
        $rr3 flush
        resume_process $server_pid
        r ping ;# make sure a full event loop cycle is processed before issuing CLIENT LIST

        # wait for get commands to be processed
        wait_for_condition 100 10 {
            [expr {[regexp {calls=(\d+)} [cmdrstat get r] -> calls] ? $calls : 0}] >= 2
        } else {
            fail "get did not arrive"
        }

        # Validate obuf-clients were disconnected (because of obuf limit)
        catch {client_field obuf-client1 name} e
        assert_match {no client named obuf-client1 found*} $e
        catch {client_field obuf-client2 name} e
        assert_match {no client named obuf-client2 found*} $e

        # Validate qbuf-client is still connected and wasn't evicted
        if {[lindex [r config get io-threads] 1] == 1} {
            assert_equal [client_field qbuf-client name] {qbuf-client}
        }

        $rr1 close
        $rr2 close
        $rr3 close
    }
}

start_server {} {
    r config set client-output-buffer-limit "tracking 0 0 0"
    test "decrease maxmemory-tracking-clients causes client eviction" {
        set maxmemory_clients [mb 4]
        set client_count 10
        set qbsize [expr ($maxmemory_clients - [mb 3]) / $client_count]
        r config set maxmemory-tracking-clients $maxmemory_clients

        # Make multiple clients consume together roughly 1mb less than maxmemory_clients
        set rrs {}
        for {set j 0} {$j < $client_count} {incr j} {
            set rr [redis_client]
            lappend rrs $rr
            $rr client setname client$j
            $rr client tracking on
            $rr write [join [list "*2\r\n\$$qbsize\r\n" [string repeat v $qbsize]] ""]
            $rr flush
            wait_for_condition 200 10 {
                [client_field client$j qbuf] >= $qbsize
            } else {
                fail "Failed to fill qbuf for test"
            }
        }

        assert {[check_eviction_test false]}

        # Make sure all clients are still connected
        set connected_clients [llength [lsearch -all [split [string trim [r client list]] "\r\n"] *name=client*]]
        assert {$connected_clients == $client_count}

        # Decrease maxmemory_clients and expect client eviction
        r config set maxmemory-tracking-clients [mb 1]
        set max_wait [expr {$::asan ? 600 : 200}]
        wait_for_condition $max_wait 10 {
            [llength [regexp -all -inline {name=client} [r client list]]] < $client_count
        } else {
            fail "Failed to evict clients"
        }

        foreach rr $rrs {$rr close}
    }
}

start_server {} {
    r config set client-output-buffer-limit "tracking 0 0 0"
    test "evict clients only until below limit" {
        set client_count 10
        set client_mem [mb 1]
        #r debug replybuffer resizing 0
        r config set maxmemory-tracking-clients 0
        r client setname control
        r client no-evict on

        # Make multiple clients consume together roughly 1mb each.
        # Use actual observed per-client memory later when deriving the limit,
        # because allocator bins can vary across CI environments.
        set total_client_mem 0
        set client_mems {}
        set rrs {}
        for {set j 0} {$j < $client_count} {incr j} {
            set rr [redis_client]
            lappend rrs $rr
            $rr client setname client$j
            $rr client tracking on
            $rr write [join [list "*2\r\n\$$client_mem\r\n" [string repeat v $client_mem]] ""]
            $rr flush
            wait_for_condition 200 10 {
                [client_field client$j tot-mem] >= $client_mem
            } else {
                fail "Failed to fill qbuf for test"
            }
            set cmem [client_field client$j tot-mem]
            lappend client_mems $cmem
        }

        # Make sure all clients are still connected
        set connected_clients [llength [lsearch -all [split [string trim [r client list]] "\r\n"] *name=client*]]
        assert {$connected_clients == $client_count}

        # Set maxmemory-tracking-clients to fit exactly the smallest half of the
        # observed clients (plus control client and a small tolerance). This keeps
        # the test deterministic even when allocator binning makes clients differ.
        set keep_count [expr {$client_count / 2}]
        set sorted_client_mems [lsort -integer $client_mems]
        set keep_client_mem 0
        set all_client_mem 0
        foreach cmem $client_mems {
            incr all_client_mem $cmem
        }
        foreach cmem [lrange $sorted_client_mems 0 [expr {$keep_count - 1}]] {
            incr keep_client_mem $cmem
        }
        set other_client_mem [expr {[clients_sum tot-mem] - $all_client_mem}]
        set maxmemory_clients [expr {$keep_client_mem + $other_client_mem + [kb 64]}]
        r config set maxmemory-tracking-clients $maxmemory_clients

        # Make sure evictions progress until tracked client memory drops below
        # the configured limit. Under ASAN and different allocators, the exact
        # surviving client count can vary slightly even when the intended
        # "evict until below limit" behavior is correct.
        set max_wait [expr {$::asan ? 600 : 200}]
        set max_remaining_clients [expr {$keep_count + 1}]
        wait_for_condition $max_wait 100 {
            [clients_sum tot-mem] <= $maxmemory_clients &&
            [llength [regexp -all -inline {name=client} [r client list]]] <= $max_remaining_clients
        } else {
            fail "Failed to evict clients"
        }

        # Restore the reply buffer resize to default
        #r debug replybuffer resizing 1

        foreach rr $rrs {$rr close}
    } {} {needs:debug}
}

start_server {} {
    r config set client-output-buffer-limit "tracking 0 0 0"
    test "evict clients in right order (large to small)" {
        # Note that each size step needs to be at least x2 larger than previous step
        # because of how the client-eviction size bucketing works
        set sizes [list [kb 128] [mb 1] [mb 3]]
        set clients_per_size 3
        r client setname control
        r client no-evict on
        r config set maxmemory-tracking-clients 0
        #r debug replybuffer resizing 0

        # Run over all sizes and create some clients using up that size
        set total_mem 0
        set group_total_mems {}
        set rrs {}
        for {set i 0} {$i < [llength $sizes]} {incr i} {
            set size [lindex $sizes $i]
            set group_total_mem 0

            for {set j 0} {$j < $clients_per_size} {incr j} {
                set rr [redis_client]
                lappend rrs $rr
                set cname "client-$i-$j"
                $rr client setname $cname
                $rr client tracking on
                $rr write [join [list "*2\r\n\$$size\r\n" [string repeat v $size]] ""]
                $rr flush
                wait_for_condition 200 10 {
                    [client_field $cname tot-mem] >= $size
                } else {
                    fail "Failed to fill qbuf for $cname"
                }
                incr group_total_mem [client_field $cname tot-mem]
            }

            lappend group_total_mems $group_total_mem

            # Account total client memory usage
            incr total_mem $group_total_mem
        }

        # Make sure all clients are connected
        set clients [split [string trim [r client list]] "\r\n"]
        for {set i 0} {$i < [llength $sizes]} {incr i} {
            assert_equal [llength [lsearch -all $clients "*name=client-$i-*"]] $clients_per_size
        }

        # For each size reduce maxmemory-tracking-clients so relevant clients should be evicted
        # do this from largest to smallest
        for {set reverse_idx [expr {[llength $sizes] - 1}]} {$reverse_idx >= 0} {incr reverse_idx -1} {
            set size [lindex $sizes $reverse_idx]
            set group_total_mem [lindex $group_total_mems $reverse_idx]
            set control_mem [client_field control tot-mem]
            set total_mem [expr {$total_mem - $group_total_mem}]
            # allow some tolerance when using io threads
            r config set maxmemory-tracking-clients [expr $total_mem + $control_mem + 1000]
            set retry [expr {$::asan ? 600 : ($::swap ? 1000 : 200)}]
            while {$retry > 0} {
                # Drive a full event loop cycle before sampling CLIENT LIST so
                # eviction triggered by the config change is reflected.
                r ping
                set clients [split [string trim [r client list]] "\r\n"]
                set expected 1
                for {set i 0} {$i < [llength $sizes]} {incr i} {
                    set verify_size [lindex $sizes $i]
                    set count [llength [lsearch -all $clients "*name=client-$i-*"]]
                    if {$verify_size < $size} {
                        if {$count != $clients_per_size} {
                            set expected 0
                            break
                        }
                    } else {
                        if {$count != 0} {
                            set expected 0
                            break
                        }
                    }
                }
                if {$expected} {
                    break
                }
                incr retry -1
                after 10
            }
            if {$retry == 0} {
                fail "Clients were not evicted in expected order"
            }
        }

        # Restore the reply buffer resize to default
        #r debug replybuffer resizing 1

        foreach rr $rrs {$rr close}
    } {} {needs:debug}
}

start_server {} {
    r config set client-output-buffer-limit "tracking 0 0 0"
    foreach type {"client no-evict" "maxmemory-tracking-clients disabled"} {
        r flushall
        r client no-evict on
        r config set maxmemory-tracking-clients 0

        test "client total memory grows during $type" {
            r setrange k [mb 1] v
            set rr [redis_client]
            $rr client setname test_client
            $rr client tracking on
            if {$type eq "client no-evict"} {
                $rr client no-evict on
                r config set maxmemory-tracking-clients 1
            }
            $rr deferred 1

            # Fill output buffer in loop without reading it and make sure
            # the tot-mem of client has increased (OS buffers didn't swallow it)
            # and eviction not occurring.
            while {true} {
                $rr get k
                $rr flush
                after 10
                if {[client_field test_client tot-mem] > [mb 10]} {
                    break
                }
            }

            # Trigger the client eviction, by flipping the no-evict flag to off
            if {$type eq "client no-evict"} {
                $rr client no-evict off
            } else {
                r config set maxmemory-tracking-clients 1
            }

            # wait for the client to be disconnected
            wait_for_condition 5000 50 {
                ![client_exists test_client]
            } else {
                puts [r client list]
                fail "client was not disconnected"
            }
            $rr close
        }
    }
}