start_server {} {

    test {check swap_inprogress_count after swapping not exist keys} {
        assert_equal [getInfoProperty [{*}r info swap] swap_inprogress_count] 0
        r hset h1 k1 v1 k2 v2 k3 v3
        assert_equal [getInfoProperty [{*}r info swap] swap_inprogress_count] 0
    }

}
start_server {} {

    test {swap_full_compact info fields exist} {
        set line [get_info r Swap swap_full_compact]
        assert_match {*times=*} $line
        assert_match {*request_cf_count=*} $line
        assert_match {*compacted_data_size=*} $line
        assert_match {*compact_took_us=*} $line
        assert_match {*blob_list_pending_cf_count=*} $line

        # list gc is off, so no cf is waiting for a blob list rebuild
        assert_equal 0 [get_info_property r Swap swap_full_compact blob_list_pending_cf_count]
        assert_equal 0 [get_info_property r Swap swap_full_compact times]
        assert_equal 0 [get_info_property r Swap swap_full_compact request_cf_count]
    }

    test {swap_ttl_compact gained compact_took_us} {
        set line [get_info r Swap swap_ttl_compact]
        assert_match {*compact_took_us=*} $line
        # adding a field must not push the existing ones out
        assert_match {*times=*} $line
        assert_match {*request_sst_count=*} $line
        assert_match {*expired_sst_count=*} $line
        assert_match {*compacted_data_size=*} $line
        assert_match {*sst_age_limit=*} $line
    }

}
