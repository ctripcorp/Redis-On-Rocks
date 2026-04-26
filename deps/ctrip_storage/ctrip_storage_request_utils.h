
#ifndef CTRIP_STORAGE_REQUEST_UTILS_H
#define CTRIP_STORAGE_REQUEST_UTILS_H
#include "server.h"

/* RIO */
#define ROCKS_UNSET             -1
#define ROCKS_NOP               0
#define ROCKS_GET             	1
#define ROCKS_PUT            	  2
#define ROCKS_DEL              	3
#define ROCKS_ITERATE           4
#define ROCKS_TYPES             5

/* ROCKS_ITERATE FLAGS  */
#define ROCKS_ITERATE_NO_LIMIT 0
#define ROCKS_ITERATE_REVERSE (1<<0)
#define ROCKS_ITERATE_CONTINUOUSLY_SEEK (1<<1) /* return next seek start key */
#define ROCKS_ITERATE_LOW_BOUND_EXCLUDE (1<<2)
#define ROCKS_ITERATE_HIGH_BOUND_EXCLUDE (1<<3)
#define ROCKS_ITERATE_DISABLE_CACHE (1<<4)
#define ROCKS_ITERATE_PREFIX_MATCH (1<<5)

sds rocksEncodeMetaKey(redisDb *db, sds key);
int rocksDecodeMetaKey(const char *raw, size_t rawlen, int *dbid,
        const char **key, size_t *keylen);
int rocksDecodeMetaVal(const char *raw, size_t rawlen, int *pswap_type,
        long long *pexpire, uint64_t *pversion, const char **pextend,
        size_t *pextend_len);
sds rocksEncodeMetaVal(int swap_type, long long expire, uint64_t version, sds extend);
sds rocksEncodeDbRangeEndKey(int dbid);
#endif /* CTRIP_STORAGE_REQUEST_UTILS_H */