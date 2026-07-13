set system_name [string tolower [exec uname -s]]
set system_supported 0

# We only support darwin or Linux with glibc
if {$system_name eq {darwin}} {
    set system_supported 1
} elseif {$system_name eq {linux}} {
    # Avoid the test on libmusl, which does not support backtrace
    set ldd [exec ldd src/redis-server]
    if {![string match {*libc.musl*} $ldd]} {
        set system_supported 1
    }
}

if {$system_supported} {
    set server_path [tmpdir server.log]
    start_server [list overrides [list dir $server_path]] {
        test "Server is able to generate a stack trace on selected systems" {
            r config set watchdog-period 200
            r debug sleep 1
            wait_for_log_messages 0 {"*debugCommand*"} 0 1000 10
        }
    }

    # Valgrind will complain that the process terminated by a signal, skip it.
    # Sanitizer will cause unknown crash, which seems a problem of gcc or sanitizer, skip it.
    if {!$::valgrind} {
        set server_path [tmpdir server1.log]
        start_server [list overrides [list dir $server_path] tags {"nosanitizer"}] {
            test "Crash report generated on SIGABRT" {
                set pid [s process_id]
                r deferred 1
                r debug sleep 10
                r flush
                after 100
                exec kill -SIGABRT $pid
                wait_for_log_messages 0 {"*STACK TRACE*"} 0 100 50
            }
        }
    }

}
