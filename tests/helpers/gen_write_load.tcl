source tests/support/redis.tcl

set ::tlsdir "tests/tls"

# Continuously sends SET commands to the server. If key is omitted, a random key
# is used for every SET command. The value is always random.
proc gen_write_load {host port seconds tls db {key ""} {size 0} {sleep 0}} {
    set start_time [clock seconds]
    set r [redis $host $port 1 $tls]
    $r client setname LOAD_HANDLER
    $r select $db

    # fixed size value
    if {$size != 0} {
        set value [string repeat "x" $size]
    }

    while 1 {
        if {$size == 0} {
            set value [expr rand()]
        }

        if {$key == ""} {
            if {[catch {$r set [expr rand()] $value} err]} {
                exit 0
            }
        } else {
            if {[catch {$r set $key $value} err]} {
                exit 0
            }
        }
        if {[clock seconds]-$start_time > $seconds} {
            exit 0
        }
        if {$sleep ne 0} {
            after $sleep
        }
    }
}

if {[llength $argv] >= 8} {
    gen_write_load [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [lindex $argv 4] [lindex $argv 5] [lindex $argv 6] [lindex $argv 7]
} else {
    gen_write_load [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [lindex $argv 4]
}
