start_server {tags "debug"} {
    test {rocksdb-property-value for rocksdb.stats} {
        assert_match {*[default]*[meta]*[score]*} [r swap rocksdb-property-value rocksdb.stats]
        assert_equal {} [r swap rocksdb-property-value rocksdb.stats "wrongcf"]
        assert_equal {} [r swap rocksdb-property-value rocksdb.stats "wrongcf,meta"]
        assert_match {*[meta]*} [r swap rocksdb-property-value rocksdb.stats "meta"]
        assert_match {*[default]*} [r swap rocksdb-property-value rocksdb.stats "default"]
        assert_match {*[default]*[meta]*[score]*} [r swap rocksdb-property-value rocksdb.stats "default,meta,score,default,meta,score"]
    }

    test {rocksdb-property-value for wrong-prop-name} {
        assert_match {} [r swap rocksdb-property-value wrong-prop-name]
        assert_equal {} [r swap rocksdb-property-value wrong-prop-name "wrongcf"]
        assert_equal {} [r swap rocksdb-property-value wrong-prop-name "wrongcf,meta"]
        assert_match {} [r swap rocksdb-property-value wrong-prop-name "meta"]
        assert_match {} [r swap rocksdb-property-value wrong-prop-name "default"]
        assert_match {} [r swap rocksdb-property-value wrong-prop-name "default,meta,score,default,meta,score"]
    }

    test {rocksdb-property-int for rocksdb.block-cache-usage} {
        r swap rocksdb-property-int rocksdb.block-cache-usage
        r swap rocksdb-property-int rocksdb.block-cache-usage "wrongcf"
        r swap rocksdb-property-int rocksdb.block-cache-usage "wrongcf,meta"
        r swap rocksdb-property-int rocksdb.block-cache-usage "meta"
        r swap rocksdb-property-int rocksdb.block-cache-usage "default"
        r swap rocksdb-property-int rocksdb.block-cache-usage "default,meta,score,default,meta,score"
    }

    test {rocksdb-property-int for wrong-prop-name} {
        assert_equal 0 [r swap rocksdb-property-int wrong-prop-name]
        assert_equal 0 [r swap rocksdb-property-int wrong-prop-name "wrongcf"]
        assert_equal 0 [r swap rocksdb-property-int wrong-prop-name "wrongcf,meta"]
        assert_equal 0 [r swap rocksdb-property-int wrong-prop-name "meta"]
        assert_equal 0 [r swap rocksdb-property-int wrong-prop-name "default"]
        assert_equal 0 [r swap rocksdb-property-int wrong-prop-name "default,meta,score,default,meta,score"]
    }
}

start_server {tags "debug"} {
    # All three blob-list subcommands share swapBlobListResolveCfs for cf name
    # parsing. With enable_blob_list_garbage_collection off by default and no
    # blob file in the db, pending and orphan are both empty and gc-applied
    # reports off everywhere, which makes the expected output deterministic.

    test {blob-list-gc-applied cf name parsing} {
        # no cf argument => every cf, as flat name/state pairs
        assert_equal {default off meta off score off} [r swap blob-list-gc-applied]

        assert_equal {default off} [r swap blob-list-gc-applied "default"]
        assert_equal {meta off} [r swap blob-list-gc-applied "meta"]
        assert_equal {score off} [r swap blob-list-gc-applied "score"]
        assert_equal {default off meta off} [r swap blob-list-gc-applied "default,meta"]
        assert_equal {meta off score off} [r swap blob-list-gc-applied "meta, score"]
        assert_equal {default off meta off score off} [r swap blob-list-gc-applied "default,meta,score"]

        # parsing stops at CF_COUNT, so a repeated name is only taken 3 times
        assert_equal {default off default off default off} \
            [r swap blob-list-gc-applied "default,default,default,default,default"]
    }

    test {blob-list-pending and blob-list-orphan on a db without blob files} {
        assert_equal {} [r swap blob-list-pending]
        assert_equal {} [r swap blob-list-pending "default"]
        assert_equal {} [r swap blob-list-pending "default,meta,score"]

        # orphan only looks at cf already running list based gc, none by default
        assert_equal {} [r swap blob-list-orphan]
        assert_equal {} [r swap blob-list-orphan "default"]
        assert_equal {} [r swap blob-list-orphan "default,meta,score"]
    }

    test {blob-list subcommands reject invalid cf name} {
        foreach subcmd {blob-list-pending blob-list-orphan blob-list-gc-applied} {
            assert_error "*Invalid cf name*" {r swap $subcmd "wrongcf"}
            assert_error "*Invalid cf name*" {r swap $subcmd "wrongcf,meta"}
            assert_error "*Invalid cf name*" {r swap $subcmd "meta,wrongcf"}
            # an empty string means "no cf argument", not an invalid name
            assert_equal 0 [catch {r swap $subcmd ""}]
        }
    }

    test {blob-list subcommands reject wrong arity} {
        foreach subcmd {blob-list-pending blob-list-orphan blob-list-gc-applied} {
            assert_error "*" {r swap $subcmd default meta}
        }
    }
}
