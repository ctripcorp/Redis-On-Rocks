start_server {tags {"swap-unsupport-cmd"}} {
    test "stream write rejected when opt-in disabled" {
        r config set swap-opt-in-cmd-enabled no
        r swap reset-cmd-stats

        catch {r xadd s * f v} err
        assert_match "*not supported by swap*" $err
        assert_equal [get_info_property r Swap swap_cmd_stats opt_in_blocked] 1
    }

    test "stream write allowed when opt-in enabled" {
        r config set swap-opt-in-cmd-enabled yes
        r swap reset-cmd-stats

        r xadd s * f v
        assert_equal [r xlen s] 1
        assert_equal [get_info_property r Swap swap_cmd_stats opt_in_allowed] 1
    }

    test "swap reset-cmd-stats clears all cmd counters" {
        r config set swap-opt-in-cmd-enabled yes
        r xadd s1 * f v
        r xadd s2 * f v

        r swap reset-cmd-stats
        assert_equal [get_info_property r Swap swap_cmd_stats unsupport_total] 0
    }

    test "swap_cmd_stats appears in swap.info" {
        r swap reset-cmd-stats
        r xadd s * f v

        set info [r info swap]
        assert_match "*swap_cmd_stats:*" $info
        assert_match "*opt_in_allowed=*" $info
    }
}