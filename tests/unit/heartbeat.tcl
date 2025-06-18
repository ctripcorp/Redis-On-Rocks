start_server {tags {"client heartbeat"}} {
    set rd1 [redis_client]
    set rd2 [redis_client]
    set r [redis_client] 

    $rd1 HELLO 3
    $rd2 HELLO 3
    $r HELLO 3

    test {heartbeat info is correct} {

        $rd1 CLIENT heartbeat on systime 1
        $rd2 CLIENT heartbeat on systime 1

        set info [r info]
        regexp "\r\nheartbeat_clients:(.*?)\r\n" $info _ heartbeat_clients
        assert {$heartbeat_clients == 2}
    }

    test {Client heartbeat on without options} {

        $r CLIENT heartbeat off
        $r HELLO 3
        catch {$r CLIENT heartbeat on} output
        assert_match "ERR* enable heartbeart without options" $output
    }

    test {Client heartbeat only works with RESP3} {

        r CLIENT heartbeat off
        catch {r CLIENT heartbeat on systime 1} output
        assert_match "ERR* Heartbeat is only supported for RESP3" $output
    }

    test {Client heartbeat works with systime} {

        $r CLIENT heartbeat off
        $r HELLO 3
        $r CLIENT heartbeat on systime 1
        after 1500
        assert_match "systime 17*" [$r read]
    }

    test {Client heartbeat works with mkps} {

        $r CLIENT heartbeat off
        $r HELLO 3
        $r CLIENT heartbeat on mkps 1

        after 1500
        assert_match "mkps *" [$r read]
    }
    

    test {Client heartbeat works with two actions} {

        $r CLIENT heartbeat off
        $r HELLO 3
        $r CLIENT heartbeat on mkps 1 systime 10

        after 1500
        assert_match "mkps *" [$r read]
    }

    test {Client can switch mkps and systime mode without disable heartbeat} {

        $r CLIENT heartbeat off
        $r HELLO 3
        $r CLIENT heartbeat on mkps 1 systime 3

        catch {$r CLIENT heartbeat ON mkps 1} output
        assert_match "OK" $output

        catch {$r CLIENT heartbeat ON systime 3} output
        assert_match "OK" $output

        catch {$r CLIENT heartbeat ON systime 1 mkps 3} output
        assert_match "OK" $output

        after 1500
        assert_match "systime 17*" [$r read]
    }

    $rd1 close
    $rd2 close
    $r close
}
