set defaults { appendonly {yes} appendfilename {appendonly.aof} }
set server_path [tmpdir server.aof]
set aof_path "$server_path/appendonly.aof"

proc append_to_aof {str} {
    upvar fp fp
    puts -nonewline $fp $str
}

proc create_aof {code} {
    upvar fp fp aof_path aof_path
    set fp [open $aof_path w+]
    uplevel 1 $code
    close $fp
}

proc start_server_aof {overrides code} {
    upvar defaults defaults srv srv server_path server_path
    set config [concat $defaults $overrides]
    set srv [start_server [list overrides $config]]
    uplevel 1 $code
    kill_server $srv
}

tags {"aof" "memonly"} {
    ## Server can start when aof-load-truncated is set to yes and AOF
    ## is truncated, with an incomplete MULTI block.
    create_aof {
        append_to_aof [formatCommand set foo hello]
        append_to_aof [formatCommand multi]
        append_to_aof [formatCommand set bar world]
    }

    start_server_aof [list dir $server_path aof-load-truncated yes] {
        test "Unfinished MULTI: Server should start if load-truncated is yes" {
            assert_equal 1 [is_alive $srv]
        }
    }

    ## Should also start with truncated AOF without incomplete MULTI block.
    create_aof {
        append_to_aof [formatCommand incr foo]
        append_to_aof [formatCommand incr foo]
        append_to_aof [formatCommand incr foo]
        append_to_aof [formatCommand incr foo]
        append_to_aof [formatCommand incr foo]
        append_to_aof [string range [formatCommand incr foo] 0 end-1]
    }

    start_server_aof [list dir $server_path aof-load-truncated yes] {
        test "Short read: Server should start if load-truncated is yes" {
            assert_equal 1 [is_alive $srv]
        }

        test "Truncated AOF loaded: we expect foo to be equal to 5" {
            set client [redis [dict get $srv host] [dict get $srv port] 0 $::tls]
            wait_done_loading $client
            assert {[$client get foo] eq "5"}
        }

        test "Append a new command after loading an incomplete AOF" {
            $client incr foo
        }
    }

    # Now the AOF file is expected to be correct
    start_server_aof [list dir $server_path aof-load-truncated yes] {
        test "Short read + command: Server should start" {
            assert_equal 1 [is_alive $srv]
        }

        test "Truncated AOF loaded: we expect foo to be equal to 6 now" {
            set client [redis [dict get $srv host] [dict get $srv port] 0 $::tls]
            wait_done_loading $client
            assert {[$client get foo] eq "6"}
        }
    }

    ## Test that the server exits when the AOF contains a format error
    create_aof {
        append_to_aof [formatCommand set foo hello]
        append_to_aof "!!!"
        append_to_aof [formatCommand set foo hello]
    }

    start_server_aof [list dir $server_path aof-load-truncated yes] {
        test "Bad format: Server should have logged an error" {
            set pattern "*Bad file format reading the append only file*"
            set retry 10
            while {$retry} {
                set result [exec tail -1 < [dict get $srv stdout]]
                if {[string match $pattern $result]} {
                    break
                }
                incr retry -1
                after 1000
            }
            if {$retry == 0} {
                error "assertion:expected error not found on config file"
            }
        }
    }

    ## Test the server doesn't start when the AOF contains an unfinished MULTI
    create_aof {
        append_to_aof [formatCommand set foo hello]
        append_to_aof [formatCommand multi]
        append_to_aof [formatCommand set bar world]
    }

    start_server_aof [list dir $server_path aof-load-truncated no] {
        test "Unfinished MULTI: Server should have logged an error" {
            set pattern "*Unexpected end of file reading the append only file*"
            set retry 10
            while {$retry} {
                set result [exec tail -1 < [dict get $srv stdout]]
                if {[string match $pattern $result]} {
                    break
                }
                incr retry -1
                after 1000
            }
            if {$retry == 0} {
                error "assertion:expected error not found on config file"
            }
        }
    }

    ## Test that the server exits when the AOF contains a short read
    create_aof {
        append_to_aof [formatCommand set foo hello]
        append_to_aof [string range [formatCommand set bar world] 0 end-1]
    }

    start_server_aof [list dir $server_path aof-load-truncated no] {
        test "Short read: Server should have logged an error" {
            set pattern "*Unexpected end of file reading the append only file*"
            set retry 10
            while {$retry} {
                set result [exec tail -1 < [dict get $srv stdout]]
                if {[string match $pattern $result]} {
                    break
                }
                incr retry -1
                after 1000
            }
            if {$retry == 0} {
                error "assertion:expected error not found on config file"
            }
        }
    }

    ## Test that redis-check-aof indeed sees this AOF is not valid
    test "Short read: Utility should confirm the AOF is not valid" {
        catch {
            exec src/redis-check-aof $aof_path
        } result
        assert_match "*not valid*" $result
    }

    test "Short read: Utility should show the abnormal line num in AOF" {
        create_aof {
            append_to_aof [formatCommand set foo hello]
            append_to_aof "!!!"
        }

        catch {
            exec src/redis-check-aof $aof_path
        } result
        assert_match "*ok_up_to_line=8*" $result
    }

    test "Short read: Utility should be able to fix the AOF" {
        set result [exec src/redis-check-aof --fix $aof_path << "y\n"]
        assert_match "*Successfully truncated AOF*" $result
    }

    ## Test that the server can be started using the truncated AOF
    start_server_aof [list dir $server_path aof-load-truncated no] {
        test "Fixed AOF: Server should have been started" {
            assert_equal 1 [is_alive $srv]
        }

        test "Fixed AOF: Keyspace should contain values that were parseable" {
            set client [redis [dict get $srv host] [dict get $srv port] 0 $::tls]
            wait_done_loading $client
            assert_equal "hello" [$client get foo]
            assert_equal "" [$client get bar]
        }
    }

    ## Test that SPOP (that modifies the client's argc/argv) is correctly free'd
    create_aof {
        append_to_aof [formatCommand sadd set foo]
        append_to_aof [formatCommand sadd set bar]
        append_to_aof [formatCommand spop set]
    }

    start_server_aof [list dir $server_path aof-load-truncated no] {
        test "AOF+SPOP: Server should have been started" {
            assert_equal 1 [is_alive $srv]
        }

        test "AOF+SPOP: Set should have 1 member" {
            set client [redis [dict get $srv host] [dict get $srv port] 0 $::tls]
            wait_done_loading $client
            assert_equal 1 [$client scard set]
        }
    }

    ## Uses the alsoPropagate() API.
    create_aof {
        append_to_aof [formatCommand sadd set foo]
        append_to_aof [formatCommand sadd set bar]
        append_to_aof [formatCommand sadd set gah]
        append_to_aof [formatCommand spop set 2]
    }

    start_server_aof [list dir $server_path] {
        test "AOF+SPOP: Server should have been started" {
            assert_equal 1 [is_alive $srv]
        }

        test "AOF+SPOP: Set should have 1 member" {
            set client [redis [dict get $srv host] [dict get $srv port] 0 $::tls]
            wait_done_loading $client
            assert_equal 1 [$client scard set]
        }
    }

    ## Test that EXPIREAT is loaded correctly
    create_aof {
        append_to_aof [formatCommand rpush list foo]
        append_to_aof [formatCommand expireat list 1000]
        append_to_aof [formatCommand rpush list bar]
    }

    start_server_aof [list dir $server_path aof-load-truncated no] {
        test "AOF+EXPIRE: Server should have been started" {
            assert_equal 1 [is_alive $srv]
        }

        test "AOF+EXPIRE: List should be empty" {
            set client [redis [dict get $srv host] [dict get $srv port] 0 $::tls]
            wait_done_loading $client
            assert_equal 0 [$client llen list]
        }
    }

    start_server {overrides {appendonly {yes} appendfilename {appendonly.aof}}} {
        test {Redis should not try to convert DEL into EXPIREAT for EXPIRE -1} {
            r set x 10
            r expire x -1
        }
    }

    start_server {overrides {appendonly {yes} appendfilename {appendonly.aof} appendfsync always}} {
        test {AOF fsync always barrier issue} {
            set rd [redis_deferring_client]
            # Set a sleep when aof is flushed, so that we have a chance to look
            # at the aof size and detect if the response of an incr command
            # arrives before the data was written (and hopefully fsynced)
            # We create a big reply, which will hopefully not have room in the
            # socket buffers, and will install a write handler, then we sleep
            # a big and issue the incr command, hoping that the last portion of
            # the output buffer write, and the processing of the incr will happen
            # in the same event loop cycle.
            # Since the socket buffers and timing are unpredictable, we fuzz this
            # test with slightly different sizes and sleeps a few times.
            for {set i 0} {$i < 10} {incr i} {
                r debug aof-flush-sleep 0
                r del x
                r setrange x [expr {int(rand()*5000000)+10000000}] x
                r debug aof-flush-sleep 500000
                set aof [file join [lindex [r config get dir] 1] appendonly.aof]
                set size1 [file size $aof]
                $rd get x
                after [expr {int(rand()*30)}]
                $rd incr new_value
                $rd read
                $rd read
                set size2 [file size $aof]
                assert {$size1 != $size2}
            }
        }
    }

    start_server {overrides {appendonly {yes} appendfilename {appendonly.aof}}} {
        test {GETEX should not append to AOF} {
            set aof [file join [lindex [r config get dir] 1] appendonly.aof]
            r set foo bar
            set before [file size $aof]
            r getex foo
            set after [file size $aof]
            assert_equal $before $after
        }
    }
}
