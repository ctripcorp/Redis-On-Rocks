source tests/support/cli.tcl

if {$::singledb} {
    set ::dbnum 0
} else {
    set ::dbnum $::target_db
}

start_server {tags {"cli"}} {
    proc open_cli {{opts ""} {infile ""}} {
        if {$opts eq ""} {
            set opts "-n $::dbnum"
        }
        set ::env(TERM) dumb
        set cmdline [rediscli [srv host] [srv port] $opts]
        if {$infile ne ""} {
            set cmdline "$cmdline < $infile"
            set mode "r"
        } else {
            set mode "r+"
        }
        set fd [open "|$cmdline" $mode]
        fconfigure $fd -buffering none
        fconfigure $fd -blocking false
        fconfigure $fd -translation binary
        set _ $fd
    }

    proc close_cli {fd} {
        close $fd
    }

    set ::read_cli_max_empty_reads 5

    proc cli_timeout_ms {base_ms} {
        set timeout $base_ms
        if {$::asan} {
            set timeout [expr {$timeout * 4}]
        }
        if {$::swap} {
            set timeout [expr {$timeout * 2}]
        }
        return $timeout
    }

    proc try_read_cli {fd {timeout_ms 100}} {
        set was_blocking [fconfigure $fd -blocking]
        fconfigure $fd -blocking false

        set ret ""
        set deadline [expr {[clock milliseconds] + $timeout_ms}]
        while {[string length $ret] == 0} {
            set ret [read $fd]
            if {[string length $ret] != 0 || [eof $fd]} {
                break
            }
            if {[clock milliseconds] >= $deadline} {
                fconfigure $fd -blocking $was_blocking
                return ""
            }
            after 10
        }

        # We may have a short read, try to read some more.
        set empty_reads 0
        while {$empty_reads < $::read_cli_max_empty_reads} {
            set buf [read $fd]
            if {[string length $buf] == 0} {
                if {[eof $fd]} {
                    break
                }
                after 10
                incr empty_reads
            } else {
                append ret $buf
                set empty_reads 0
            }
        }
        fconfigure $fd -blocking $was_blocking
        return $ret
    }

    proc read_cli {fd {timeout_ms ""}} {
        if {$timeout_ms eq ""} {
            set timeout_ms [cli_timeout_ms 5000]
        }
        set ret [try_read_cli $fd $timeout_ms]
        if {[string length $ret] == 0} {
            if {[eof $fd]} {
                error "redis-cli exited before producing output"
            }
            error "Timed out waiting for redis-cli output"
        }
        return $ret
    }

    proc read_cli_to_eof {fd {timeout_ms ""}} {
        if {$timeout_ms eq ""} {
            set timeout_ms [cli_timeout_ms 60000]
        }

        set was_blocking [fconfigure $fd -blocking]
        fconfigure $fd -blocking false

        set ret ""
        set deadline [expr {[clock milliseconds] + $timeout_ms}]
        while {1} {
            set buf [read $fd]
            if {[string length $buf] != 0} {
                append ret $buf
                continue
            }
            if {[eof $fd]} {
                break
            }
            if {[clock milliseconds] >= $deadline} {
                fconfigure $fd -blocking $was_blocking
                error "Timed out waiting for redis-cli process to exit"
            }
            after 10
        }

        fconfigure $fd -blocking $was_blocking
        return $ret
    }

    proc cli_output_contains {fd output_var pattern {timeout_ms 100}} {
        upvar 1 $output_var output
        append output [try_read_cli $fd [cli_timeout_ms $timeout_ms]]
        return [string match $pattern $output]
    }

    proc write_cli {fd buf} {
        puts $fd $buf
        flush $fd
    }

    # Helpers to run tests in interactive mode

    proc format_output {output} {
        set _ [string trimright [regsub -all "\r" $output ""] "\n"]
    }

    proc run_command {fd cmd} {
        write_cli $fd $cmd
        set _ [format_output [read_cli $fd]]
    }

    proc test_interactive_cli {name code} {
        set ::env(FAKETTY) 1
        set fd [open_cli]
        test "Interactive CLI: $name" $code
        close_cli $fd
        unset ::env(FAKETTY)
    }

    # Helpers to run tests where stdout is not a tty
    proc write_tmpfile {contents} {
        set tmp [tmpfile "cli"]
        set tmpfd [open $tmp "w"]
        puts -nonewline $tmpfd $contents
        close $tmpfd
        set _ $tmp
    }

    proc _run_cli {host port db opts args} {
        set cmd [rediscli $host $port [list -n $db {*}$args]]
        foreach {key value} $opts {
            if {$key eq "pipe"} {
                set cmd "sh -c \"$value | $cmd\""
            }
            if {$key eq "path"} {
                set cmd "$cmd < $value"
            }
        }

        set fd [open "|$cmd" "r"]
        fconfigure $fd -buffering none
        fconfigure $fd -translation binary
        set resp [read_cli_to_eof $fd]
        close $fd
        set _ [format_output $resp]
    }

    proc run_cli {args} {
        _run_cli [srv host] [srv port] $::dbnum {} {*}$args
    }

    proc run_cli_with_input_pipe {cmd args} {
        _run_cli [srv host] [srv port] $::dbnum [list pipe $cmd] -x {*}$args
    }

    proc run_cli_with_input_file {path args} {
        _run_cli [srv host] [srv port] $::dbnum [list path $path] -x {*}$args
    }

    proc run_cli_host_port_db {host port db args} {
        _run_cli $host $port $db {} {*}$args
    }

    proc test_nontty_cli {name code} {
        test "Non-interactive non-TTY CLI: $name" $code
    }

    # Helpers to run tests where stdout is a tty (fake it)
    proc test_tty_cli {name code} {
        set ::env(FAKETTY) 1
        test "Non-interactive TTY CLI: $name" $code
        unset ::env(FAKETTY)
    }

    test_interactive_cli "INFO response should be printed raw" {
        set lines [split [run_command $fd info] "\n"]
        foreach line $lines {
            assert [regexp {^$|^#|^[^#:]+:} $line]
        }
    }

    test_interactive_cli "Status reply" {
        assert_equal "OK" [run_command $fd "set key foo"]
    }

    test_interactive_cli "Integer reply" {
        assert_equal "(integer) 1" [run_command $fd "incr counter"]
    }

    test_interactive_cli "Bulk reply" {
        r set key foo
        assert_equal "\"foo\"" [run_command $fd "get key"]
    }

    test_interactive_cli "Multi-bulk reply" {
        r rpush list foo
        r rpush list bar
        assert_equal "1) \"foo\"\n2) \"bar\"" [run_command $fd "lrange list 0 -1"]
    }

    test_interactive_cli "Parsing quotes" {
        assert_equal "OK" [run_command $fd "set key \"bar\""]
        assert_equal "bar" [r get key]
        assert_equal "OK" [run_command $fd "set key \" bar \""]
        assert_equal " bar " [r get key]
        assert_equal "OK" [run_command $fd "set key \"\\\"bar\\\"\""]
        assert_equal "\"bar\"" [r get key]
        assert_equal "OK" [run_command $fd "set key \"\tbar\t\""]
        assert_equal "\tbar\t" [r get key]

        # invalid quotation
        assert_equal "Invalid argument(s)" [run_command $fd "get \"\"key"]
        assert_equal "Invalid argument(s)" [run_command $fd "get \"key\"x"]

        # quotes after the argument are weird, but should be allowed
        assert_equal "OK" [run_command $fd "set key\"\" bar"]
        assert_equal "bar" [r get key]
    }

    test_tty_cli "Status reply" {
        assert_equal "OK" [run_cli set key bar]
        assert_equal "bar" [r get key]
    }

    test_tty_cli "Integer reply" {
        r del counter
        assert_equal "(integer) 1" [run_cli incr counter]
    }

    test_tty_cli "Bulk reply" {
        r set key "tab\tnewline\n"
        assert_equal "\"tab\\tnewline\\n\"" [run_cli get key]
    }

    test_tty_cli "Multi-bulk reply" {
        r del list
        r rpush list foo
        r rpush list bar
        assert_equal "1) \"foo\"\n2) \"bar\"" [run_cli lrange list 0 -1]
    }

    test_tty_cli "Read last argument from pipe" {
        assert_equal "OK" [run_cli_with_input_pipe "echo foo" set key]
        assert_equal "foo\n" [r get key]
    }

    test_tty_cli "Read last argument from file" {
        set tmpfile [write_tmpfile "from file"]
        assert_equal "OK" [run_cli_with_input_file $tmpfile set key]
        assert_equal "from file" [r get key]
        file delete $tmpfile
    }

    test_nontty_cli "Status reply" {
        assert_equal "OK" [run_cli set key bar]
        assert_equal "bar" [r get key]
    }

    test_nontty_cli "Integer reply" {
        r del counter
        assert_equal "1" [run_cli incr counter]
    }

    test_nontty_cli "Bulk reply" {
        r set key "tab\tnewline\n"
        assert_equal "tab\tnewline" [run_cli get key]
    }

    test_nontty_cli "Multi-bulk reply" {
        r del list
        r rpush list foo
        r rpush list bar
        assert_equal "foo\nbar" [run_cli lrange list 0 -1]
    }

if {!$::tls} { ;# fake_redis_node doesn't support TLS
    test_nontty_cli "ASK redirect test" {
        # Set up two fake Redis nodes.
        set tclsh [info nameofexecutable]
        set script "tests/helpers/fake_redis_node.tcl"
        set port1 [find_available_port $::baseport $::portcount]
        set port2 [find_available_port $::baseport $::portcount]
        set p1 [exec $tclsh $script $port1 \
                "SET foo bar" "-ASK 12182 127.0.0.1:$port2" &]
        set p2 [exec $tclsh $script $port2 \
                "ASKING" "+OK" \
                "SET foo bar" "+OK" &]
        # Make sure both fake nodes have started listening
        wait_for_condition 50 50 {
            [catch {close [socket "127.0.0.1" $port1]}] == 0 && \
            [catch {close [socket "127.0.0.1" $port2]}] == 0
        } else {
            fail "Failed to start fake Redis nodes"
        }
        # Run the cli
        assert_equal "OK" [run_cli_host_port_db "127.0.0.1" $port1 0 -c SET foo bar]
    }
}

    test_nontty_cli "Quoted input arguments" {
        r set "\x00\x00" "value"
        assert_equal "value" [run_cli --quoted-input get {"\x00\x00"}]
    }

    test_nontty_cli "No accidental unquoting of input arguments" {
        run_cli --quoted-input set {"\x41\x41"} quoted-val
        run_cli set {"\x41\x41"} unquoted-val
        assert_equal "quoted-val" [r get AA]
        assert_equal "unquoted-val" [r get {"\x41\x41"}]
    }

    test_nontty_cli "Invalid quoted input arguments" {
        catch {run_cli --quoted-input set {"Unterminated}} err
        assert_match {*exited abnormally*} $err

        # A single arg that unquotes to two arguments is also not expected
        catch {run_cli --quoted-input set {"arg1" "arg2"}} err
        assert_match {*exited abnormally*} $err
    }

    test_nontty_cli "Read last argument from pipe" {
        assert_equal "OK" [run_cli_with_input_pipe "echo foo" set key]
        assert_equal "foo\n" [r get key]
    }

    test_nontty_cli "Read last argument from file" {
        set tmpfile [write_tmpfile "from file"]
        assert_equal "OK" [run_cli_with_input_file $tmpfile set key]
        assert_equal "from file" [r get key]
        file delete $tmpfile
    }

    proc test_redis_cli_rdb_dump {} {
        r flushdb

        set dir [lindex [r config get dir] 1]

        assert_equal "OK" [r debug populate 100000 key 1000]
        catch {run_cli --rdb "$dir/cli.rdb"} output
        assert_match {*Transfer finished with success*} $output

        file delete "$dir/dump.rdb"
        file rename "$dir/cli.rdb" "$dir/dump.rdb"

        assert_equal "OK" [r set should-not-exist 1]
        assert_equal "OK" [r debug reload nosave]
        assert_equal {} [r get should-not-exist]
    }

    test "Dumping an RDB" {
        # Disk-based master
        assert_match "OK" [r config set repl-diskless-sync no]
        test_redis_cli_rdb_dump

        # Disk-less master
        assert_match "OK" [r config set repl-diskless-sync yes]
        assert_match "OK" [r config set repl-diskless-sync-delay 0]
        test_redis_cli_rdb_dump
    }

    test "Scan mode" {
        r flushdb
        populate 1000 key: 1

        # basic use - SCAN may return duplicates, so count unique keys
        set scan_output [run_cli --scan]
        set scan_split [split $scan_output]
        set unique_keys [lsort -unique $scan_split]
        assert_equal 1000 [llength $unique_keys]

        # pattern
        assert_equal {key:2} [run_cli --scan --pattern "*:2"]

        # pattern matching with a quoted string
        assert_equal {key:2} [run_cli --scan --quoted-pattern {"*:\x32"}]
    }

    proc test_redis_cli_repl {} {
        set fd [open_cli "--replica"]
        set repl_output ""
        wait_for_condition 500 100 {
            [string match {*slave0:*state=online*} [r info]]
        } else {
            fail "redis-cli --replica did not connect"
        }

        for {set i 0} {$i < 100} {incr i} {
           r set test-key test-value-$i
        }

        wait_for_condition 500 100 {
            [cli_output_contains $fd repl_output {*test-value-99*}]
        } else {
            fail "redis-cli --replica didn't read commands: $repl_output"
        }

        fconfigure $fd -blocking true
        r client kill type slave
        catch { close_cli $fd } err
        assert_match {*Server closed the connection*} $err
    }

    test "Connecting as a replica" {
        # Disk-based master
        assert_match "OK" [r config set repl-diskless-sync no]
        test_redis_cli_repl

        # Disk-less master
        assert_match "OK" [r config set repl-diskless-sync yes]
        assert_match "OK" [r config set repl-diskless-sync-delay 0]
        test_redis_cli_repl
    } {}

    test "Piping raw protocol" {
        set cmds [tmpfile "cli_cmds"]
        set cmds_fd [open $cmds "w"]

        set cmds_count 2101

        if {!$::singledb} {
            puts $cmds_fd [formatCommand select $::target_db]
            incr cmds_count
        }
        puts $cmds_fd [formatCommand del test-counter]

        for {set i 0} {$i < 1000} {incr i} {
            puts $cmds_fd [formatCommand incr test-counter]
            puts $cmds_fd [formatCommand set large-key [string repeat "x" 20000]]
        }

        for {set i 0} {$i < 100} {incr i} {
            puts $cmds_fd [formatCommand set very-large-key [string repeat "x" 512000]]
        }
        close $cmds_fd

        set cli_fd [open_cli "--pipe" $cmds]
        fconfigure $cli_fd -blocking true
        # --pipe emits its summary only after all data is transferred; under
        # swap+asan that can exceed read_cli's short first-chunk timeout.
        set ::read_cli_max_empty_reads [expr {$::asan ? 100 : ($::swap ? 50 : 10)}]
        set output [read_cli_to_eof $cli_fd]
        set ::read_cli_max_empty_reads 5

        assert_equal {1000} [r get test-counter]
        assert_match "*All data transferred*errors: 0*replies: ${cmds_count}*" $output

        file delete $cmds
    }
}
