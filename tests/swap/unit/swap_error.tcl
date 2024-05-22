start_server {tags {"swap error"}} {
    start_server {} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]

        $master config set swap-debug-evict-keys 0
        $slave config set swap-debug-evict-keys 0
        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        test {swap error if rio failed} {
            $master hmset myhash a a b b c c
            assert_error "WRONGTYPE Operation against a key holding the wrong kind of value" {$master strlen myhash}
            assert {[get_info $master swap swap_error_count] eq 0}

            $master multi
            $master scan 0
            assert_error "EXECABORT Transaction discarded because of: Swap failed: scan not supported in multi." {$master exec}
            assert {[get_info $master swap swap_error_count] eq 0}

            $master set key value

            $master swap rio-error 1
            $master swap.evict key
            after 100
            assert ![object_is_cold $master key]
            assert {[get_info $master swap swap_error_count] eq 1}

            $slave swap rio-error 1
            $slave swap.evict key
            after 100
            assert ![object_is_cold $slave key]
            assert {[get_info $slave swap swap_error_count] eq 1}

            $master swap.evict key
            wait_key_cold $master key
            assert [object_is_cold $master key]
            assert {[get_info $master swap swap_error_count] eq 1}

            $slave swap.evict key
            wait_key_cold $slave key
            assert [object_is_cold $slave key]
            assert {[get_info $slave swap swap_error_count] eq 1}

            $master swap rio-error 1
            catch {$master get key} {e}
            assert_match {*Swap failed*} $e
            assert_equal [$master get key] value

            $slave swap rio-error 1
            catch {$slave get key} {e}
            assert_match {*Swap failed*} $e
            assert_equal [$slave get key] value

            $master swap rio-error 1
            catch {$master del key} {e}
            assert_match {*Swap failed*} $e
            after 100
            assert_equal [$master get key] value
            assert_equal [$slave get key] value

            $master del key
            after 100
            assert_equal [$master get key] {}
            assert_equal [$slave get key] {}
        }
    }
}
