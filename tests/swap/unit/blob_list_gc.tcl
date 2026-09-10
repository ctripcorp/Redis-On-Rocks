# List based blob gc: config constraints and deferred push down.
#
# The feature separates "intent" from "what rocksdb actually runs with".
# Turning it on via CONFIG SET only records the intent; the option is pushed
# down later by the cron, once it has confirmed the blob list of that cf is
# complete (every sst referencing a blob file carries a blob file set record).
#
# Parking the cron:
#   The pending state can be very short lived. If the blob list is already
#   complete, genServerBlobListRebuildTask pushes the option down right away
#   without scheduling any compaction, so pending is cleared within
#   milliseconds of the cron firing. Asserting on that state without stopping
#   the cron first is inherently racy.
#
#   swap-ttl-compact-period is MODIFIABLE and capped at 86400. run_with_period
#   only fires when cronloops % (period_ms / (1000/hz)) == 0, so a period of
#   86400 means the next firing is a day away. Setting it back to 1 lets the
#   cron run again within a second.

proc blob_list_gc_applied {r cf} {
    lindex [$r swap blob-list-gc-applied $cf] 1
}

proc blob_list_pending_count {r} {
    get_info_property $r Swap swap_full_compact blob_list_pending_cf_count
}

proc park_compact_cron {r} {
    $r config set swap-ttl-compact-period 86400
}

proc unpark_compact_cron {r} {
    $r config set swap-ttl-compact-period 1
}

# swap flush is asynchronous, so wait until the blob files really exist before
# drawing conclusions from cf metadata.
proc wait_blob_files {r} {
    wait_for_condition 100 100 {
        [$r swap rocksdb-property-int rocksdb.total-blob-file-size "default"] > 0
    } else {
        fail "no blob file was created for the default cf"
    }
}

proc wait_blob_list_gc_applied {r cf} {
    wait_for_condition 100 100 {
        [blob_list_gc_applied $r $cf] eq {on}
    } else {
        fail "list blob gc of $cf cf never got pushed down"
    }
}

proc wait_blob_list_pending_count {r expected} {
    wait_for_condition 100 100 {
        [blob_list_pending_count $r] == $expected
    } else {
        fail "blob_list_pending_cf_count never reached $expected"
    }
}

proc write_blob_data {r count} {
    for {set j 0} {$j < $count} {incr j} {
        $r set blobkey-$j [string repeat A 1024]
    }
}

# ---------------------------------------------------------------------------
# 1. The two way constraint between enable_blob_file_set_record and
#    enable_blob_list_garbage_collection. Pure config layer, cron plays no part.
# ---------------------------------------------------------------------------
start_server {tags {"swap blob-list-gc"} overrides {
    swap-ttl-compact-period {86400}
}} {

    test {blob list gc: defaults} {
        assert_equal {rocksdb.data.enable_blob_file_set_record yes} \
            [r config get rocksdb.data.enable_blob_file_set_record]
        assert_equal {rocksdb.meta.enable_blob_file_set_record yes} \
            [r config get rocksdb.meta.enable_blob_file_set_record]
        assert_equal {rocksdb.data.enable_blob_list_garbage_collection no} \
            [r config get rocksdb.data.enable_blob_list_garbage_collection]
        assert_equal {rocksdb.meta.enable_blob_list_garbage_collection no} \
            [r config get rocksdb.meta.enable_blob_list_garbage_collection]
    }

    test {blob list gc: cannot be enabled while file set record is off} {
        r config set rocksdb.data.enable_blob_file_set_record no
        assert_error "*rocksdb.data.enable_blob_file_set_record must be enabled first*" \
            {r config set rocksdb.data.enable_blob_list_garbage_collection yes}
        # value must be left untouched after the rejection
        assert_equal {rocksdb.data.enable_blob_list_garbage_collection no} \
            [r config get rocksdb.data.enable_blob_list_garbage_collection]

        r config set rocksdb.meta.enable_blob_file_set_record no
        assert_error "*rocksdb.meta.enable_blob_file_set_record must be enabled first*" \
            {r config set rocksdb.meta.enable_blob_list_garbage_collection yes}
        assert_equal {rocksdb.meta.enable_blob_list_garbage_collection no} \
            [r config get rocksdb.meta.enable_blob_list_garbage_collection]

        r config set rocksdb.data.enable_blob_file_set_record yes
        r config set rocksdb.meta.enable_blob_file_set_record yes
    }

    test {blob list gc: file set record cannot be taken away while gc is on} {
        r config set rocksdb.data.enable_blob_list_garbage_collection yes
        assert_error "*rocksdb.data.enable_blob_list_garbage_collection must be disabled first*" \
            {r config set rocksdb.data.enable_blob_file_set_record no}
        assert_equal {rocksdb.data.enable_blob_file_set_record yes} \
            [r config get rocksdb.data.enable_blob_file_set_record]

        r config set rocksdb.meta.enable_blob_list_garbage_collection yes
        assert_error "*rocksdb.meta.enable_blob_list_garbage_collection must be disabled first*" \
            {r config set rocksdb.meta.enable_blob_file_set_record no}
        assert_equal {rocksdb.meta.enable_blob_file_set_record yes} \
            [r config get rocksdb.meta.enable_blob_file_set_record]

        # dropping gc first and the record second must be allowed
        r config set rocksdb.data.enable_blob_list_garbage_collection no
        r config set rocksdb.data.enable_blob_file_set_record no
        assert_equal {rocksdb.data.enable_blob_file_set_record no} \
            [r config get rocksdb.data.enable_blob_file_set_record]

        r config set rocksdb.meta.enable_blob_list_garbage_collection no
        r config set rocksdb.meta.enable_blob_file_set_record no
        assert_equal {rocksdb.meta.enable_blob_file_set_record no} \
            [r config get rocksdb.meta.enable_blob_file_set_record]

        r config set rocksdb.data.enable_blob_file_set_record yes
        r config set rocksdb.meta.enable_blob_file_set_record yes
    }

    test {blob list gc: aliases resolve} {
        assert_equal {rocksdb.enable_blob_list_garbage_collection no} \
            [r config get rocksdb.enable_blob_list_garbage_collection]
        assert_equal {rocksdb.enable_blob_file_set_record yes} \
            [r config get rocksdb.enable_blob_file_set_record]
    }
}

# ---------------------------------------------------------------------------
# 2. Enabling records the intent only; disabling reaches rocksdb at once; the
#    cron is what eventually pushes the enable down.
#
#    The blob list here is complete from the start (the record option has been
#    on all along), so the cron takes the shortcut and never schedules a
#    compaction. Section 3 covers the compaction path.
# ---------------------------------------------------------------------------
start_server {tags {"swap blob-list-gc"} overrides {
    swap-ttl-compact-period {86400}
    rocksdb.data.enable_blob_files {yes}
    rocksdb.data.min_blob_size {64}
}} {

    test {blob list gc: enabling only records the intent} {
        write_blob_data r 50
        r swap flush
        wait_blob_files r

        assert_equal {default off} [r swap blob-list-gc-applied "default"]
        assert_equal 0 [blob_list_pending_count r]

        r config set rocksdb.data.enable_blob_list_garbage_collection yes

        # the data knob covers both default and score cf
        assert_equal 2 [blob_list_pending_count r]
        assert_equal {default off} [r swap blob-list-gc-applied "default"]
        assert_equal {score off} [r swap blob-list-gc-applied "score"]
        # meta has its own knob and is not affected
        assert_equal {meta off} [r swap blob-list-gc-applied "meta"]

        # With the cron parked it has to stay pending. This is the actual proof
        # that the push is deferred; the snapshot right after CONFIG SET alone
        # would not tell us who does the pushing.
        after 500
        assert_equal 2 [blob_list_pending_count r]
        assert_equal {default off} [r swap blob-list-gc-applied "default"]
    }

    test {blob list gc: the cron pushes the pending intent down} {
        unpark_compact_cron r

        wait_blob_list_gc_applied r "default"
        wait_blob_list_gc_applied r "score"
        wait_blob_list_pending_count r 0

        # the blob list was already complete, so no full compact was needed
        assert_equal 0 [get_info_property r Swap swap_full_compact request_cf_count]

        park_compact_cron r
    }

    test {blob list gc: disabling takes effect immediately} {
        r config set rocksdb.data.enable_blob_list_garbage_collection no
        assert_equal {default off} [r swap blob-list-gc-applied "default"]
        assert_equal {score off} [r swap blob-list-gc-applied "score"]
        assert_equal 0 [blob_list_pending_count r]
    }

    test {blob list gc: meta knob only puts meta cf into pending} {
        r config set rocksdb.meta.enable_blob_list_garbage_collection yes
        assert_equal 1 [blob_list_pending_count r]
        assert_equal {meta off} [r swap blob-list-gc-applied "meta"]
        assert_equal {default off} [r swap blob-list-gc-applied "default"]
        assert_equal {score off} [r swap blob-list-gc-applied "score"]

        r config set rocksdb.meta.enable_blob_list_garbage_collection no
        assert_equal 0 [blob_list_pending_count r]
    }

    test {blob list gc: data and score are still readable with gc applied} {
        unpark_compact_cron r
        r config set rocksdb.data.enable_blob_list_garbage_collection yes
        wait_blob_list_gc_applied r "default"

        r set foo bar
        r swap.evict foo
        wait_key_cold r foo
        assert_equal bar [r get foo]

        r zadd myzset 1 a 2 b 3 c
        r swap.evict myzset
        wait_key_cold r myzset
        assert_equal {a b c} [r zrange myzset 0 -1]
    }
}

# ---------------------------------------------------------------------------
# 3. The blob list is incomplete, so the cron has to run a full compact before
#    it can turn list based gc on.
#
#    The db starts with the record option off, so the ssts written below
#    reference blob files without carrying a blob file set record. Turning the
#    record back on does not fix the ssts already on disk; only rewriting them
#    does. This is the only path that exercises blobListRebuildConsumeTask and
#    blobListRebuildClearPending.
# ---------------------------------------------------------------------------
start_server {tags {"swap blob-list-gc"} overrides {
    swap-ttl-compact-period {86400}
    rocksdb.data.enable_blob_files {yes}
    rocksdb.data.min_blob_size {64}
    rocksdb.data.enable_blob_file_set_record {no}
}} {

    test {blob list gc: incomplete blob list is reported as pending} {
        write_blob_data r 100
        r swap flush
        wait_blob_files r

        # blob-list-pending skips cf whose record option is off, there is
        # nothing to build there
        assert_equal {} [r swap blob-list-pending "default"]

        r config set rocksdb.data.enable_blob_file_set_record yes

        # the ssts already on disk still lack the record
        assert_equal {default} [r swap blob-list-pending "default"]
    }

    test {blob list gc: full compact rebuilds the list then gc is turned on} {
        r config set rocksdb.data.enable_blob_list_garbage_collection yes
        assert_equal 2 [blob_list_pending_count r]

        unpark_compact_cron r

        wait_blob_list_gc_applied r "default"
        wait_blob_list_gc_applied r "score"
        wait_blob_list_pending_count r 0

        # a full compact really was scheduled this time
        assert_morethan [get_info_property r Swap swap_full_compact request_cf_count] 0
        assert_morethan [get_info_property r Swap swap_full_compact times] 0

        # and the rewrite closed the gap the pending report was complaining about
        assert_equal {} [r swap blob-list-pending "default"]
    }
}

# ---------------------------------------------------------------------------
# 4. Enabled straight from the config file. rocksOpen always opens with list
#    based gc off, swapBlobListPendingInit puts every intended cf back into
#    pending, and the cron converges on its own.
# ---------------------------------------------------------------------------
start_server {tags {"swap blob-list-gc"} overrides {
    swap-ttl-compact-period {1}
    rocksdb.data.enable_blob_file_set_record {yes}
    rocksdb.data.enable_blob_list_garbage_collection {yes}
}} {

    test {blob list gc: enabled from config file applies without help} {
        assert_equal {rocksdb.data.enable_blob_list_garbage_collection yes} \
            [r config get rocksdb.data.enable_blob_list_garbage_collection]

        wait_blob_list_gc_applied r "default"
        wait_blob_list_gc_applied r "score"
        wait_blob_list_pending_count r 0

        r set foo bar
        r swap.evict foo
        wait_key_cold r foo
        assert_equal bar [r get foo]
    }
}
