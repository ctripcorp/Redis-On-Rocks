start_server {tags {"client tracking heartbeat"}} {
    set rd1 [redis_client]
    set rd2 [redis_client]
    set r [redis_client] 

    $rd1 HELLO 3
    $rd2 HELLO 3
    $r HELLO 3

    test {heartbeat info is correct} {

        $rd1 CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat systime 1
        $rd2 CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat mkps 1

        set info [r info]
        regexp "\r\nheartbeat_clients:(.*?)\r\n" $info _ heartbeat_clients
        assert {$heartbeat_clients == 2}

        $rd1 CLIENT tracking off
        $rd2 CLIENT tracking on bcast PREFIX prefix3 PREFIX prefix4
        set info [r info]
        regexp "\r\nheartbeat_clients:(.*?)\r\n" $info _ heartbeat_clients
        assert {$heartbeat_clients == 0}
    }

    test {Client heartbeat without wrong options} {

        $r HELLO 3
        catch {$r CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat mkps} output
        assert_match "ERR* syntax error" $output

        catch {$r CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat} output
        assert_match "ERR* syntax error" $output

        catch {$r CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat mkps 1 systime} output
        assert_match "ERR* syntax error" $output

        catch {$r CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat mkps 1 mkps 2} output
        assert_match "ERR* Duplicate mkps option in heartbeat" $output

        catch {$r CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat mkps 1 systime 2 systime 3} output
        assert_match "ERR* Duplicate systime option in heartbeat" $output

        catch {$r CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat mkps 1 sys 2 systime 3} output
        assert_match "ERR* syntax error" $output
    }

    test {Client heartbeat works with systime} {

        $r HELLO 3
        $r CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat systime 1
        after 1500
        assert_match "systime 17*" [$r read]
    }

    test {Client heartbeat works with mkps} {

        $r CLIENT tracking off
        $r HELLO 3
        $r CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat mkps 1

        after 1500
        assert_match "mkps *" [$r read]
    }
    

    test {Client heartbeat works with two actions} {

        $r CLIENT tracking off
        $r HELLO 3
        $r CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat mkps 1 systime 10

        after 1500
        assert_match "mkps *" [$r read]
    }

    test {Client can switch mkps and systime mode without disable heartbeat} {

        $r CLIENT tracking off
        $r HELLO 3
        $r CLIENT tracking on bcast PREFIX prefix1 PREFIX prefix2 heartbeat mkps 1 systime 10

        catch {$r CLIENT tracking on bcast heartbeat mkps 1} output
        assert_match "OK" $output

        catch {$r CLIENT tracking on bcast heartbeat systime 3} output
        assert_match "OK" $output

        catch {$r CLIENT tracking ON bcast heartbeat systime 1 mkps 3} output
        assert_match "OK" $output

        after 1500
        assert_match "systime 17*" [$r read]
    }

    $rd1 close
    $rd2 close
    $r close
}
