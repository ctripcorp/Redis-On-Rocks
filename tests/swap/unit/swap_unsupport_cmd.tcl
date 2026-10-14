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

        test {hard blocked hexpire is rejected on master and is not replicated} {
            $master hset h f v
            wait_for_ofs_sync $master $slave
            assert_equal v [$slave hget h f]

            catch {$master hexpire h 100 FIELDS 1 f} err
            assert_match "*not supported by swap*" $err
            assert_equal [get_info_property $master Swap swap_cmd_stats reject_blocked] 1
            assert_equal [get_info_property $master Swap swap_cmd_stats allowed_blocked] 3
            assert_equal [get_info_property $master Swap swap_cmd_stats master_allowed] 0
            assert_equal v [$master hget h f]
            assert_equal v [$slave hget h f]
            assert_equal [get_info_property $slave Swap swap_cmd_stats master_allowed] 3
            assert_equal [get_info_property $slave Swap swap_cmd_stats reject_blocked] 3
            assert_equal [get_info_property $slave Swap swap_cmd_stats allowed_blocked] 0

            catch {$slave hexpire h 100 FIELDS 1 f} err
            assert_match "*not supported by swap*" $err
            assert_equal [get_info_property $slave Swap swap_cmd_stats reject_blocked] 4
            assert_equal [get_info_property $slave Swap swap_cmd_stats allowed_blocked] 0
            assert_equal [get_info_property $slave Swap swap_cmd_stats master_allowed] 3
            assert_equal v [$slave hget h f]
        }
    }
}

# Hash field expiry commands are hard blocked: a local client is rejected
# whether or not swap-soft-block-cmd-enabled is on. Replication from master
# and AOF replay are the only paths that still apply them.
proc assert_hash_field_expiry_hard_blocked {r key} {
    set cmds [list \
        [list hexpire $key 100 FIELDS 1 f] \
        [list hpexpire $key 100000 FIELDS 1 f] \
        [list hexpireat $key 2000000000 FIELDS 1 f] \
        [list hpexpireat $key 2000000000000 FIELDS 1 f] \
        [list hpersist $key FIELDS 1 f] \
        [list hsetex $key EX 10 FIELDS 1 f v2] \
        [list hgetex $key EX 10 FIELDS 1 f]]
    set before [get_info_property $r Swap swap_cmd_stats reject_blocked]
    foreach cmd $cmds {
        catch {$r {*}$cmd} err
        assert_match "*not supported by swap*" $err
    }
    assert_equal [expr {$before + [llength $cmds]}] \
        [get_info_property $r Swap swap_cmd_stats reject_blocked]
    assert_equal v [$r hget $key f]
}

start_server {tags {"swap-unsupport-cmd"}} {
    r config set swap-soft-block-cmd-enabled yes

    test {hash field expiry commands are hard blocked when soft block enabled} {
        r hset h f v
        assert_hash_field_expiry_hard_blocked r h
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 7
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 0
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
    }

    test {lua redis.call hexpire rejected when soft block enabled} {
        r hset hlua f v
        catch {r eval {return redis.call('hexpire','hlua','100','FIELDS','1','f')} 0} err
        assert_match "*not supported by swap*" $err
        assert_equal v [r hget hlua f]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 8
    }

    test {lua redis.pcall hsetex is blocked and the script can continue} {
        r hset hpcall f v
        assert_equal ok [r eval {redis.pcall('hsetex','hpcall','EX','10','FIELDS','1','f','v2'); redis.call('set','kpcall','1'); return 'ok'} 0]
        assert_equal v [r hget hpcall f]
        assert_equal 1 [r get kpcall]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 9
    }

    test {lua redis.call stops the script before the blocked hexpire} {
        r hset hstop f v
        catch {r eval {redis.call('set','kstop','1'); return redis.call('hpersist','hstop','FIELDS','1','f')} 0} err
        assert_match "*not supported by swap*" $err
        assert_equal 1 [r get kstop]
        assert_equal v [r hget hstop f]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 10
    }

    test {multi hexpire rejected when soft block enabled} {
        r hset hmulti f v
        r multi
        catch {r hexpire hmulti 100 FIELDS 1 f} err
        assert_match "*not supported by swap*" $err
        r discard
        assert_equal v [r hget hmulti f]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 11
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 0
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
    }
}

start_server {tags {"swap-unsupport-cmd"}} {
    r config set swap-soft-block-cmd-enabled no

    test {hash field expiry commands stay rejected when soft block disabled} {
        r hset h f v
        r xadd s * f v
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 1
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 0

        assert_hash_field_expiry_hard_blocked r h
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 7
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 1
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
        assert_equal [r xlen s] 1
    }

    test {lua hexpire stays rejected when soft block disabled} {
        catch {r eval {return redis.call('hexpire','h','100','FIELDS','1','f')} 0} err
        assert_match "*not supported by swap*" $err
        assert_equal ok [r eval {redis.pcall('hgetex','h','EX','10','FIELDS','1','f'); return 'ok'} 0]
        assert_equal v [r hget h f]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 9
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 1
    }

    test {multi hexpire stays rejected when soft block disabled} {
        r multi
        catch {r hsetex h EX 10 FIELDS 1 f v2} err
        assert_match "*not supported by swap*" $err
        r discard
        assert_equal v [r hget h f]
        assert_equal [get_info_property r Swap swap_cmd_stats reject_blocked] 10
        assert_equal [get_info_property r Swap swap_cmd_stats allowed_blocked] 1
        assert_equal [get_info_property r Swap swap_cmd_stats master_allowed] 0
    }
}