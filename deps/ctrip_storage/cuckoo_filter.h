#ifndef __CUCKOO_FILTER_H__
#define __CUCKOO_FILTER_H__

#include <stdint.h>
#include <stddef.h>

#define CUCKOO_OK 0
#define CUCKOO_ERR -1

#define CUCKOO_UNUSED(V) ((void) V)

#define CUCKOO_FILTER_MAX_ITERATION 500
#define CUCKOO_FILTER_TAGS_PER_BUCKET  4
#define CUCKOO_FILTER_BUCKETS_EXPANSION 4
#define CUCKOO_FILTER_MAX_TABLES 4
#define CUCKOO_TAG_NULL 0
#define CUCKOO_FILTER_TABLE_MIN_BUCKETS  16

#define CUCKOO_FILTER_BITS_PER_TAG_8  0
#define CUCKOO_FILTER_BITS_PER_TAG_12 1
#define CUCKOO_FILTER_BITS_PER_TAG_16 2
#define CUCKOO_FILTER_BITS_PER_TAG_32 3
#define CUCKOO_FILTER_BITS_PER_TAG_TYPES 4

typedef uint64_t (*cuckoo_hash_fn)(const void *key, int klen);

typedef struct cuckooVictimCache {
  int used;
  uint32_t tag;
  size_t index;
} cuckooVictimCache;

typedef struct cuckooTable {
  size_t bits_per_tag;
  size_t bytes_per_bucket;
  size_t nbuckets;
  cuckooVictimCache victim;
  size_t ntags;
  uint8_t *data;
} cuckooTable;

typedef struct cuckooFilterStat {
  size_t ntags;
  size_t used_memory;
  size_t ntables;
  double load_factor;
  double load_factors[CUCKOO_FILTER_MAX_TABLES];
} cuckooFilterStat;

/* Cuckoo filter consists of cuckoo table with N, 4N, 16N... buckets. */
typedef struct cuckooFilter {
  cuckoo_hash_fn hash_fn;
  int bits_per_tag;
  int ntables;
  cuckooTable *tables;
} cuckooFilter;

/* hash function with static seed(so that cuckoo filter can reload) */
uint64_t cuckooGenHashFunction(const void *key, int len);
/* Create cockoo filter */
cuckooFilter *cuckooFilterNew(cuckoo_hash_fn hash_fn, int bits_per_tag_type, size_t estimated_keys);
/* Destroy cockoo filter */
void cuckooFilterFree(cuckooFilter *filter);
/* Add an key to the filter. */
int cuckooFilterInsert(cuckooFilter *filter, const char *key, size_t klen);
/* Report if the item is inserted, with false positive rate. */
int cuckooFilterContains(cuckooFilter *filter, const char *key, size_t klen);
/* Delete an key from the filter (Note that key MUST added previously). */
int cuckooFilterDelete(cuckooFilter *filter, const char *key, size_t klen);
/* Get filter stats. */
void cuckooFilterGetStat(cuckooFilter *filter, cuckooFilterStat *stat);
/* Get filter used memory */
size_t cuckooFilterUsedMemory(cuckooFilter *filter);

void cuckooFilterDump(cuckooFilter *filter);


#endif