#include "ctrip_storage_filter.h"
#include "ctrip_storage_metric.h"
#include <assert.h>

#include <stddef.h>
#include "zmalloc.h"
#define cuckoo_malloc zmalloc
#define cuckoo_realloc zrealloc
#define cuckoo_calloc zcalloc
#define cuckoo_free zfree

#define CUCKOO_FILTER_MAX_ITERATION 500
#define CUCKOO_FILTER_TAGS_PER_BUCKET  4
#define CUCKOO_FILTER_BUCKETS_EXPANSION 4
#define CUCKOO_FILTER_MAX_TABLES 4
#define CUCKOO_TAG_NULL 0
#define CUCKOO_FILTER_TABLE_MIN_BUCKETS  16
static int isLittleEndian() { int n = 1; return (*(char *)&n == 1); }
static int isPowOf2(uint64_t n) { return (n & (n - 1)) == 0 && n != 0; }

static uint8_t cuckoo_hash_function_seed[16] =
{15, 228, 29, 66, 3, 163, 118, 182, 101, 208, 229, 232, 2, 74, 115, 47};

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t cuckooGenHashFunction(const void *key, int len) {
    return siphash(key,len,cuckoo_hash_function_seed);
}

static inline uint64_t upperPowOf2(uint64_t n) {
	n--;
	n |= n >> 1;
	n |= n >> 2;
	n |= n >> 4;
	n |= n >> 8;
	n |= n >> 16;
	n |= n >> 32;
	n++;
	return n;
}

static size_t cuckooEstimateBuckets(size_t estimated_keys) {
    size_t nbuckets = upperPowOf2(estimated_keys)/CUCKOO_FILTER_TAGS_PER_BUCKET;
    return nbuckets < CUCKOO_FILTER_TABLE_MIN_BUCKETS ? CUCKOO_FILTER_TABLE_MIN_BUCKETS : nbuckets;
}

static inline int cuckooGetBitsPerTag(int bits_per_tag_type) {
    assert(bits_per_tag_type < CUCKOO_FILTER_BITS_PER_TAG_TYPES);
    int bits_per_tag_array[CUCKOO_FILTER_BITS_PER_TAG_TYPES] = {8,12,16,32};
    return bits_per_tag_array[bits_per_tag_type];
}




static inline
void coldFilterInitCuckooFilter(coldFilter *filter) {
    if (server.storage.swap_cuckoo_filter_enabled && filter->filter == NULL) {
        filter->filter = cuckooFilterNew(
                cuckooGenHashFunction,
                server.storage.swap_cuckoo_filter_bit_type,
                server.storage.swap_cuckoo_filter_estimated_keys);
    }
}

static inline uint64_t cuckooFilterGenerateHash(cuckooFilter *filter,
        const char *key, size_t klen) {
    return filter->hash_fn(key,klen);
}



static inline void cuckooTableIndexTag(cuckooTable *table, uint64_t hv,
        size_t *i1, uint32_t *tag) {
    *i1 = (hv >> 32) & (table->nbuckets - 1);
    *tag = (hv & 0xFFFFFFFF) & ((1ULL << table->bits_per_tag) - 1);
    *tag += *tag == 0;
}

static inline size_t cuckooTableAltIndex(cuckooTable *table, size_t i1,
        uint32_t tag) {
    return (i1 ^ ((size_t)tag * 0x5bd1e995)) & (table->nbuckets - 1);
}


/* works only for little-endian */
static inline
uint32_t cuckooTableReadTag(cuckooTable *table, size_t i, size_t j) {
    assert(i < table->nbuckets && j < CUCKOO_FILTER_TAGS_PER_BUCKET);
    uint32_t tag = 0;
    uint8_t *p = table->data + i*table->bytes_per_bucket;
    if (table->bits_per_tag == 8) {
        tag = ((uint8_t*)p)[j];
    } else if (table->bits_per_tag == 12) {
        p += j + (j >> 1);
        tag = (*((uint16_t *)p) >> ((j & 1) << 2)) & 0xFFF;
    } else if (table->bits_per_tag == 16) {
        tag = ((uint16_t*)p)[j];
    } else if (table->bits_per_tag == 32) {
        tag = ((uint32_t*)p)[j];
    }
    return tag;
}





int coldFilterMayContainKey(coldFilter *filter, sds key, int *filt_by) {
    /* cuckoo filter are lazily created to save memory */
    coldFilterInitCuckooFilter(filter);

    if (filter->filter) {
        filter->filter_stat.lookup_count++;
        if (cuckooFilterContains(filter->filter,key,sdslen(key)) == CUCKOO_ERR) {
            *filt_by = COLDFILTER_FILT_BY_CUCKOO_FILTER;
            return 0;
        }
    }

    if (filter->absents && absentCacheGetKey(filter->absents,key)) {
        *filt_by = COLDFILTER_FILT_BY_ABSENT_CACHE;
        return 0;
    }

    return 1;
}

void coldFilterKeyNotFound(coldFilter *filter, sds key) {
    if (filter->absents) absentCachePutKey(filter->absents,key);
    if (filter->filter) filter->filter_stat.false_positive_count++;
}

static void coldFilterDisableCuckooFilters() {
    for (int i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db+i;
        if (db->storage.cold_filter->filter == NULL) continue;
        cuckooFilterFree(db->storage.cold_filter->filter);
        db->storage.cold_filter->filter = NULL;
    }
    server.storage.swap_cuckoo_filter_enabled = 0;
}


void coldFilterAddKey(coldFilter *filter, sds key) {
    /* cuckoo filter are lazily created to save memory */
    coldFilterInitCuckooFilter(filter);

    if (filter->filter) {
        if (cuckooFilterInsert(filter->filter,key,sdslen(key)) == CUCKOO_ERR) {
            cuckooFilterStat stat;
            cuckooFilterGetStat(filter->filter,&stat);

            coldFilterDisableCuckooFilters();

            serverLog(LL_WARNING,
                    "Insert key(%s) to cuckoo filter(ntables=%ld,ntags=%ld,used_memory=%ld,load_factor=%.2f) failed, cuckoo filter turned off.",
                    key,stat.ntables,stat.ntags,stat.used_memory,stat.load_factor);
        }
    }

    if (filter->absents) absentCacheDelete(filter->absents,key);
}


/* Invalidate key from absent cache when subkey added to rocksdb so that
 * we don't need to save evicted subkeys (cant maintain absent cache
 * from swap thread) and delete every subkey in main thread.  */
void coldFilterSubkeyAdded(coldFilter *filter, sds key) {
    if (server.storage.swap_absent_cache_include_subkey && filter->absents)
        absentCacheDelete(filter->absents,key);
}

void coldFilterSubkeyNotFound(coldFilter *filter, sds key, sds subkey) {
    if (server.storage.swap_absent_cache_include_subkey && filter->absents)
        absentCachePutSubkey(filter->absents,key,subkey);
}

int coldFilterMayContainSubkey(coldFilter *filter, sds key, sds subkey) {
    if (server.storage.swap_absent_cache_include_subkey && filter->absents) {
        atomicIncr(server.storage.swap_hit_stats->stat_absent_subkey_query_count,1);
        if (absentCacheGetSubkey(filter->absents,key,subkey)) {
            atomicIncr(server.storage.swap_hit_stats->stat_absent_subkey_filt_count,1);
            return 0;
        } else {
            return 1;
        }
    } else {
        return 1;
    }
}


void coldFilterDeleteKey(coldFilter *filter, sds key) {
    if (filter->filter) {
        serverAssert(cuckooFilterDelete(filter->filter,key,sdslen(key)) == CUCKOO_OK);
    }
}

void cuckooFilterDump(cuckooFilter *filter) {
    serverLog(LL_NOTICE, "==== cuckoo filter(%p) ====", (void*)filter);
    serverLog(LL_NOTICE, "  bits_per_tag: %d", filter->bits_per_tag);
    serverLog(LL_NOTICE, "  ntables: %d", filter->ntables);
    for (int i = 0; i < filter->ntables; i++) {
        cuckooTable *table = filter->tables+i;
        serverLog(LL_NOTICE, "  table(%d):", i);
        serverLog(LL_NOTICE, "    bits_per_tag: %lu", table->bits_per_tag);
        serverLog(LL_NOTICE, "    bytes_per_bucket: %lu", table->bytes_per_bucket);
        serverLog(LL_NOTICE, "    nbuckets: %lu", table->nbuckets);
        serverLog(LL_NOTICE, "    victim: used=%d,tag=%u,index=%lu", table->victim.used, table->victim.tag, table->victim.index);
        serverLog(LL_NOTICE, "    ntags: %ld", table->ntags);
        serverLogHexDump(LL_NOTICE, "    data:",table->data,table->bytes_per_bucket*table->nbuckets);
    }
    serverLog(LL_NOTICE, "===========================");
}

static inline
void coldFilterInitAbsentCache(coldFilter *filter) {
    if (server.storage.swap_absent_cache_enabled) {
        filter->absents = absentCacheNew(server.storage.swap_absent_cache_capacity);
    }
}

coldFilter *coldFilterCreate() {
    coldFilter *filter = zcalloc(sizeof(coldFilter));
    coldFilterInitAbsentCache(filter);
    return filter;
}