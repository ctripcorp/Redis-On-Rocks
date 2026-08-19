tags {"rdb"} {

    # This fixture is an OLD-version (6.2.6-based ROR, commit 042f23c9f) generated
    # rordb file. It carries six keys covering three different hot/cold shapes:
    #
    #   ziphash   : warm hash - 2 hot fields in legacy *ziplist* encoding + 3 cold subkeys
    #   zipzset   : warm zset - 2 hot members in legacy *ziplist* encoding + 3 cold subkeys
    #   coldhash  : fully cold hash - 3 subkeys, nothing in memory
    #   coldzset  : fully cold zset - 3 subkeys, nothing in memory
    #   emptyhash : hash whose in-memory object is EMPTY - 3 subkeys, all cold
    #   emptyzset : zset whose in-memory object is EMPTY - 3 subkeys, all cold
    #
    # Encodings: every hot part above is *ziplist* encoded on the old server (the
    # two empty ones are an empty ziplist, 11 bytes) and must come back as
    # *listpack* after the load.
    #
    # Three things are guarded here:
    #
    # 1) ziplist->listpack conversion on rordb load. When loading a rordb
    #    (SkipEmptyCheckFlag = 1), rdbLoadObject() used to skip the
    #    ziplist->listpack data conversion (ziplistPairsConvertAndValidateIntegrity),
    #    leaving an empty object and silently dropping every hot field.
    #    With the fix, the conversion always runs, so the fields survive the load.
    #
    # 2) The hot/cold shape survives the load. A fully cold key emits *no* KV
    #    record and *no* object-meta record in the rordb at all - it lives only in
    #    the embedded SST files plus the cold key count / cuckoo filter. So it must
    #    come back without ever going through rdbLoadObject().
    #
    # 3) An EMPTY in-memory object is a legal ROR state and must not be dropped on
    #    load. Commands declared SWAP_IN_META (hlen/scard/llen/exists/type/ttl/
    #    expire/...) swap in only the *meta* of a cold key，and mock an empty key hashSwapIn
    #   On load, ziplistPairsConvertAndValidateIntegrity() accepts it (an empty ziplist is
    #    well-formed) and rdbLoadObjectGetSkipEmptyCheckFlag() suppresses the
    #    `hashTypeLength(o,0)==0 -> goto emptykey` check. 
    #
    # How to regenerate the rordb file (needs the old server; NOT needed to run this test):
    #   1. start the old redis-server with: swap-mode disk (it defaults to `memory`,
    #      where swap.evict simply drops the data), swap-debug-evict-keys 0,
    #      swap-cuckoo-filter-estimated-keys 10000 (the default 32M would add a 32MB
    #      zero-filled filter blob to the file), hash/zset-max-ziplist-entries 128.
    #   2. warm keys:  hset ziphash f1 v1 .. f5 v5 ; swap.evict ziphash ;
    #                  hget ziphash f1 ; hget ziphash f2      (same shape for zipzset)
    #   3. cold keys:  hset coldhash c1 w1 c2 w2 c3 w3 ; swap.evict coldhash
    #                                                          (same for coldzset)
    #   4. empty keys: hset emptyhash e1 x1 e2 x2 e3 x3 ; swap.evict emptyhash ;
    #                  hlen emptyhash                        <- SWAP_IN_META
    #                  zadd emptyzset 100 n1 200 n2 300 n3 ; swap.evict emptyzset ;
    #                  ttl emptyzset                         <- SWAP_IN_META
    #      (TTL, not ZCARD, for the zset: zcard is declared SWAP_IN,0 in the old
    #       server.c command table, so it would swap in every member instead.)
    #   5. swap.debug rordb bgsave
    #   Inspect the shapes with `swap object <key>` only: OBJECT ENCODING / HGETALL /
    #   ZRANGE would swap the whole key in and destroy the shapes just built.

    set server_path [tmpdir "server.rordb-ziplist-load"]
    exec cp tests/assets/swap/rordb-ziplist.rordb $server_path

    start_server [list overrides [list "dir" $server_path "dbfilename" "rordb-ziplist.rordb" "swap-debug-evict-keys" 0]] {

        test "load rordb preserves hot/cold shape of hash/zset" {
            assert_equal 6 [r dbsize]

            # warm: hot value present + hot meta describing the remaining cold subkeys
            assert_equal 1 [object_is_warm r ziphash]
            assert_equal 1 [object_is_warm r zipzset]
            assert_equal 3 [object_meta_len r ziphash]
            assert_equal 3 [object_meta_len r zipzset]

            # fully cold: no value in memory at all, only the cold meta
            assert_equal 1 [object_is_cold r coldhash]
            assert_equal 1 [object_is_cold r coldzset]
            assert_equal 3 [object_meta_len r coldhash]
            assert_equal 3 [object_meta_len r coldzset]

            # The hot part arrived as a legacy *ziplist* and must have been
            # converted on load. Read the encoding out of `swap object`'s value
            # section: OBJECT ENCODING would swap the whole key in (see the last
            # test), this does not.
            assert_equal hash [swap_object_property [r swap object ziphash] value type]
            assert_equal zset [swap_object_property [r swap object zipzset] value type]
            assert_equal listpack [swap_object_property [r swap object ziphash] value encoding]
            assert_equal listpack [swap_object_property [r swap object zipzset] value encoding]
        }

        test "load rordb keeps the empty in-memory hash/zset (SWAP_IN_META shape)" {
            # There *is* an object in the keyspace: not cold, and object_is_warm
            # requires the `value` section of `swap object` to be present.
            assert_equal 0 [object_is_cold r emptyhash]
            assert_equal 0 [object_is_cold r emptyzset]
            assert_equal 1 [object_is_warm r emptyhash]
            assert_equal 1 [object_is_warm r emptyzset]

            # And that object is empty: the whole logical length comes from the cold
            # subkeys, so the hot part holds 0 elements.
            assert_equal 3 [r hlen emptyhash]
            assert_equal 3 [object_meta_len r emptyhash]
            assert_equal 0 [expr {[r hlen emptyhash] - [object_meta_len r emptyhash]}]

            assert_equal 3 [r zcard emptyzset]
            assert_equal 3 [object_meta_len r emptyzset]
            assert_equal 0 [expr {[r zcard emptyzset] - [object_meta_len r emptyzset]}]

            # Encoding of the empty container: the old server held it as an empty
            # *ziplist* (11 bytes: zlbytes=11, zltail=10, zllen=0, 0xff) and that is
            # what the rordb carries. On load ziplistPairsConvertAndValidateIntegrity()
            # walks zero entries and hands back an empty *listpack*, which stays
            # listpack since 0 <= hash/zset-max-listpack-entries.
            assert_equal hash [swap_object_property [r swap object emptyhash] value type]
            assert_equal zset [swap_object_property [r swap object emptyzset] value type]
            assert_equal listpack [swap_object_property [r swap object emptyhash] value encoding]
            assert_equal listpack [swap_object_property [r swap object emptyzset] value encoding]
        }

        test "load rordb with legacy ziplist-encoded hash/zset preserves all fields" {
            # hash: 2 hot (ziplist) + 3 cold fields must all survive
            assert_equal 5 [r hlen ziphash]
            assert_equal [lsort [r hgetall ziphash]] [lsort {f1 v1 f2 v2 f3 v3 f4 v4 f5 v5}]
            assert_equal v1 [r hget ziphash f1]
            assert_equal v5 [r hget ziphash f5]

            # zset: 2 hot (ziplist) + 3 cold members must all survive
            assert_equal 5 [r zcard zipzset]
            assert_equal {a 1 b 2 c 3 d 4 e 5} [r zrange zipzset 0 -1 withscores]

            # full read triggers swap-in merge; must not crash / underflow
            assert_equal {PONG} [r ping]
        }

        test "load rordb restores fully cold hash/zset" {
            assert_equal 3 [r hlen coldhash]
            assert_equal [lsort [r hgetall coldhash]] [lsort {c1 w1 c2 w2 c3 w3}]

            assert_equal 3 [r zcard coldzset]
            assert_equal {m1 10 m2 20 m3 30} [r zrange coldzset 0 -1 withscores]

            assert_equal {PONG} [r ping]
        }

        test "load rordb restores the cold subkeys behind an empty in-memory object" {
            assert_equal [lsort [r hgetall emptyhash]] [lsort {e1 x1 e2 x2 e3 x3}]
            assert_equal {n1 100 n2 200 n3 300} [r zrange emptyzset 0 -1 withscores]

            assert_equal {PONG} [r ping]
        }

        test "keys turn hot after being fully swapped in" {
            foreach key {ziphash zipzset coldhash coldzset emptyhash emptyzset} {
                assert_equal 1 [object_is_hot r $key]
            }

            # OBJECT ENCODING itself swaps the whole key in, so it is only safe
            # down here, after the reads above already turned every key hot.
            foreach key {ziphash zipzset coldhash coldzset emptyhash emptyzset} {
                assert_equal listpack [r object encoding $key]
            }
        }
    }
}
