
#ifndef __CTRIP_STORAGE_FILTER_H__
#define __CTRIP_STORAGE_FILTER_H__

#include "server.h"
#include "absent_cache.h"
#include "cuckoo_filter.h"
#include "ctrip_storage_adlist.h"

#define CUCKOO_FILTER_BITS_PER_TAG_8  0
#define CUCKOO_FILTER_BITS_PER_TAG_12 1
#define CUCKOO_FILTER_BITS_PER_TAG_16 2
#define CUCKOO_FILTER_BITS_PER_TAG_32 3
#define CUCKOO_FILTER_BITS_PER_TAG_TYPES 4




typedef uint64_t (*cuckoo_hash_fn)(const void *key, int klen);



typedef struct swapCuckooFilterStat {
  long long lookup_count;
  long long false_positive_count;
} swapCuckooFilterStat;

typedef struct coldFilter {
  absentCache *absents;
  cuckooFilter *filter;
  swapCuckooFilterStat filter_stat;
} coldFilter;


#define CUCKOO_OK 0
#define CUCKOO_ERR -1

#define COLDFILTER_FILT_BY_CUCKOO_FILTER 1
#define COLDFILTER_FILT_BY_ABSENT_CACHE 2

void cuckooFilterDump(cuckooFilter *filter);

int coldFilterMayContainKey(coldFilter *filter, sds key, int *filt_by);
void coldFilterKeyNotFound(coldFilter *filter, sds key);
void coldFilterAddKey(coldFilter *filter, sds key);
void coldFilterDeleteKey(coldFilter *filter, sds key);
void coldFilterSubkeyAdded(coldFilter *filter, sds key);
void coldFilterSubkeyNotFound(coldFilter *filter, sds key, sds subkey);
int coldFilterMayContainSubkey(coldFilter *filter, sds key, sds subkey);
#endif