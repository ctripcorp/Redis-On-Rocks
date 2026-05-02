

start_server {tags {"swap_thread_race_scaledown"} overrides {
    save ""
    swap-threads-auto-scale-max 5
}} {
    set host [srv 0 host]
    set port [srv 0 port]
    set r1 [srv 0 client]
    set r2 [redis $host $port]

    proc wait_keys_cold {r prefix count {wait_ms 8000}} {
        set deadline [expr {[clock milliseconds] + $wait_ms}]
        while {[clock milliseconds] < $deadline} {
            set cold 0
            for {set i 0} {$i < $count} {incr i} {
                set info ""
                catch {set info [$r swap object "$prefix:$i"]}
                if {[string match "*value: <nil>*" $info] && \
                    [string match "*cold_meta: swap_type=*" $info]} {
                    incr cold
                }
            }
            if {$cold == $count} { return $cold }
            after 100
        }
        return 0
    }

    test {scale-down race: assert inflight_reqs==0 crash via serverCron} {
        $r1 config set swap-threads-auto-scale-down-idle-seconds 300
        $r1 swap.debug thread auto-scale-up
        after 200

        set info [$r1 info swap]
        set tn 0
        regexp {swap_thread_num:(\d+)} $info -> tn
        assert {$tn >= 5}

        after 1500


        $r1 config set swap-batch-limit "IN 1 1048576 OUT 1 1048576 DEL 1 1048576"
        $r1 config set swap-threads-auto-scale-up-threshold 1


        set key_count 5
        set prefix "scalerace"
        for {set i 0} {$i < $key_count} {incr i} {
            $r1 set "$prefix:$i" [string repeat "v" 4096]
            $r1 swap.evict "$prefix:$i"
        }
        set cold [wait_keys_cold $r1 $prefix $key_count 8000]
        assert_equal $cold $key_count


        set warmup_conns {}
        for {set i 0} {$i < $key_count} {incr i} {
            lappend warmup_conns [redis $host $port]
        }
        set i 0
        foreach rc $warmup_conns {
            $rc deferred 1
            $rc get "$prefix:$i"
            incr i
        }

        foreach rc $warmup_conns { catch {$rc read} }


        for {set i 0} {$i < $key_count} {incr i} {
            catch {$r1 swap.evict "$prefix:$i"}
        }
        set cold [wait_keys_cold $r1 $prefix $key_count 5000]
        assert_equal $cold $key_count


        after 1200


        $r1 config set swap-debug-scale-down-delay-micro 500000

        set t0 [clock milliseconds]

      
        $r1 config set swap-threads-auto-scale-down-idle-seconds 0


        set race_conns {}
        for {set i 0} {$i < $key_count} {incr i} {
            lappend race_conns [redis $host $port]
        }
        set i 0
        foreach rc $race_conns {
            $rc deferred 1
            $rc get "$prefix:$i"
            incr i
        }


        after 50


        set tp0 [clock milliseconds]
        catch {$r1 ping}
        set tp1 [clock milliseconds]

        after 100

    
        set crash_detected 0
        set check_deadline [expr {[clock milliseconds] + 2000}]
        while {[clock milliseconds] < $check_deadline} {
            after 50
            if {[catch {$r2 ping}]} {
                set crash_detected 1
                break
            }
        }

        set elapsed [expr {[clock milliseconds] - $t0}]

        catch {$r1 config set swap-debug-scale-down-delay-micro 0}
        catch {$r1 config set swap-threads-auto-scale-down-idle-seconds 300}
        catch {$r1 config set swap-threads-auto-scale-up-threshold 32}
        catch {$r1 config set swap-batch-limit "IN 64 67108864 OUT 64 67108864 DEL 64 67108864"}

        
        assert_equal [$r2 ping] "PONG"
        
    }
}
