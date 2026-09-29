start_server {tags {"swap-unsupport-cmd"}} {
    r config set swap-soft-block-cmd-enabled yes

    test {stream write rejected when soft block enabled} {
        catch {r xadd s * f v} err
        assert_match "*not supported by swap*" $err
        assert_equal 0 [r exists s]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 1
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 0
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
    }

    test {swap_cmd_stats fields appear in info} {
        set info [r info swap]
        assert_match "*swap_cmd_stats:*" $info
        assert_match "*reject_blocked=*" $info
        assert_match "*allowed_blocked=*" $info
        assert_match "*master_allowed=*" $info
    }

    test {lua redis.call xadd rejected when soft block enabled} {
        catch {r eval {return redis.call('xadd','s','*','f','v')} 0} err
        assert_match "*not supported by swap*" $err
        assert_equal 0 [r exists s]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 2
    }

    test {lua redis.pcall xadd is blocked and the script can continue} {
        assert_equal ok [r eval {redis.pcall('xadd','s','*','f','v'); redis.call('set','k','1'); return 'ok'} 0]
        assert_equal 0 [r exists s]
        assert_equal 1 [r exists k]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 3
    }

    test {lua redis.call stops the script before the blocked xadd} {
        catch {r eval {redis.call('set','k2','1'); return redis.call('xadd','s','*','f','v')} 0} err
        assert_match "*not supported by swap*" $err
        assert_equal 1 [r exists k2]
        assert_equal 0 [r exists s]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 4
    }

    test {lua evalsha xadd rejected when soft block enabled} {
        set sha [r script load {return redis.call('xadd',KEYS[1],'*','f','v')}]
        catch {r evalsha $sha 1 s} err
        assert_match "*not supported by swap*" $err
        assert_equal 0 [r exists s]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 5
    }

    test {lua command without the soft-block flag is not counted} {
        assert_equal PONG [r eval {return redis.call('ping')} 0]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 5
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 0
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
    }

    test {multi xadd rejected when soft block enabled} {
        r multi
        catch {r xadd s * f v} err
        assert_match "*not supported by swap*" $err
        r discard
        assert_equal 0 [r exists s]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 6
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 0
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
    }

    test {multi command without the soft-block flag is not counted} {
        r multi
        r set k3 1
        r exec
        assert_equal 1 [r get k3]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 6
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 0
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
    }
}

start_server {tags {"swap-unsupport-cmd"}} {
    r config set swap-soft-block-cmd-enabled no

    test {stream write allowed when soft block disabled is counted} {
        r xadd s * f v
        r xadd s * f v
        assert_equal [r xlen s] 2
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 2
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 0
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
    }

    test {lua stream write allowed when soft block disabled is counted} {
        r eval {return redis.call('xadd','slua','*','f','v')} 0
        r eval {return redis.pcall('xadd','slua','*','f','v')} 0
        assert_equal [r xlen slua] 2
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 4
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 0
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
    }

    test {multi stream write allowed when soft block disabled is counted} {
        r multi
        r xadd smulti * f v
        r xadd smulti * f v
        r exec
        assert_equal [r xlen smulti] 2
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 6
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 0
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
    }
}

start_server {tags {"swap-unsupport-cmd"} overrides {save ""}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    start_server {tags {"swap-unsupport-cmd"} overrides {save ""}} {
        set slave [srv 0 client]
        $slave slaveof $master_host $master_port
        wait_for_sync $slave

        # Master accepts the write so it is replicated.
        # Slave rejects the same write from a local client.
        $master config set swap-soft-block-cmd-enabled no
        $slave config set swap-soft-block-cmd-enabled yes
        $slave config set replica-read-only no

        test {master stream write is applied on slave and counted as master_allowed} {
            $master xadd s * f v
            wait_for_ofs_sync $master $slave

            assert_equal [$slave xlen s] 1
            assert_equal [get_info_property $slave Swap swap_cmd_stats master_allowed] 1
            assert_equal [get_info_property $slave Swap swap_cmd_stats reject_blocked] 0
            assert_equal [get_info_property $slave Swap swap_cmd_stats allowed_blocked] 0
            assert_equal [get_info_property $master Swap swap_cmd_stats allowed_blocked] 1
            assert_equal [get_info_property $master Swap swap_cmd_stats reject_blocked] 0
            assert_equal [get_info_property $master Swap swap_cmd_stats master_allowed] 0

            catch {$slave xadd s * f local} err
            assert_match "*not supported by swap*" $err
            assert_equal [get_info_property $slave Swap swap_cmd_stats reject_blocked] 1
            assert_equal [get_info_property $slave Swap swap_cmd_stats allowed_blocked] 0
            assert_equal [get_info_property $slave Swap swap_cmd_stats master_allowed] 1
            assert_equal [$slave xlen s] 1
        }

        test {lua xadd from master is applied on slave and counted as master_allowed} {
            $master eval {return redis.call('xadd','slua','*','f','v')} 0
            wait_for_ofs_sync $master $slave

            assert_equal [$slave xlen slua] 1
            assert_equal [get_info_property $slave Swap swap_cmd_stats master_allowed] 2
            assert_equal [get_info_property $slave Swap swap_cmd_stats reject_blocked] 1
            assert_equal [get_info_property $slave Swap swap_cmd_stats allowed_blocked] 0
            assert_equal [get_info_property $master Swap swap_cmd_stats allowed_blocked] 2
            assert_equal [get_info_property $master Swap swap_cmd_stats reject_blocked] 0
            assert_equal [get_info_property $master Swap swap_cmd_stats master_allowed] 0

            catch {$slave eval {return redis.call('xadd','slua','*','f','local')} 0} err
            assert_match "*not supported by swap*" $err
            assert_equal [get_info_property $slave Swap swap_cmd_stats reject_blocked] 2
            assert_equal [get_info_property $slave Swap swap_cmd_stats allowed_blocked] 0
            assert_equal [get_info_property $slave Swap swap_cmd_stats master_allowed] 2
            assert_equal [$slave xlen slua] 1
        }

        test {multi xadd from master is applied on slave and counted as master_allowed} {
            $master multi
            $master xadd smulti * f v
            $master exec
            wait_for_ofs_sync $master $slave

            assert_equal [$slave xlen smulti] 1
            assert_equal [get_info_property $slave Swap swap_cmd_stats master_allowed] 3
            assert_equal [get_info_property $slave Swap swap_cmd_stats reject_blocked] 2
            assert_equal [get_info_property $slave Swap swap_cmd_stats allowed_blocked] 0
            assert_equal [get_info_property $master Swap swap_cmd_stats allowed_blocked] 3
            assert_equal [get_info_property $master Swap swap_cmd_stats reject_blocked] 0
            assert_equal [get_info_property $master Swap swap_cmd_stats master_allowed] 0

            $slave multi
            catch {$slave xadd smulti * f local} err
            assert_match "*not supported by swap*" $err
            $slave discard
            assert_equal [get_info_property $slave Swap swap_cmd_stats reject_blocked] 3
            assert_equal [get_info_property $slave Swap swap_cmd_stats allowed_blocked] 0
            assert_equal [get_info_property $slave Swap swap_cmd_stats master_allowed] 3
            assert_equal [$slave xlen smulti] 1
        }
    }
}
