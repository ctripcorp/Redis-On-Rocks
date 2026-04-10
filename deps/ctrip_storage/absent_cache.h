
#ifndef __ABSENT_CACHE_H__
#define __ABSENT_CACHE_H__ 
#include "server.h"
#include "ctrip_storage_adlist.h"
/* absent cache */
typedef struct absentKeyMapEntry {
  dict *subkeys;
  listNode *ln;
} absentKeyMapEntry;

typedef struct absentListEntry {
  sds key; /* ref */
  sds subkey; /* ref */
} absentListEntry;

typedef struct absentCache {
  size_t capacity;
  dict *map;
  list *list;
} absentCache;

absentCache *absentCacheNew(size_t capacity);
void absentCacheFree(absentCache *absent);
int absentCacheDelete(absentCache *absent, sds key);
int absentCachePutKey(absentCache *absent, sds key);
int absentCachePutSubkey(absentCache *absent, sds key, sds subkey);
int absentCacheGetKey(absentCache *absent, sds key);
int absentCacheGetSubkey(absentCache *absent, sds key, sds subkey);
void absentCacheSetCapacity(absentCache *absent, size_t capacity);
#endif /* __CTRIP_STORAGE_ABSENT_CACHE_H__ */