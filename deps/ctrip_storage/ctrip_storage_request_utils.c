#include "ctrip_storage_request_utils.h"
#include "ctrip_storage_utils.h"




const char *strObjectType(int type) {
    switch (type) {
    case OBJ_STRING:
    case SWAP_TYPE_BITMAP:
        return "string";
    case OBJ_HASH: return "hash";
    case OBJ_LIST: return "list";
    case OBJ_SET: return "set";
    case OBJ_ZSET: return "zset";
    case OBJ_STREAM: return "stream";
    default: return "unknown";
    }
}

static inline char abbrev2ObjectType(char abbrev) {
    char abbrevs[] = {'K','L','S','Z','H','M','X','B'};
    for (size_t i = 0; i < sizeof(abbrevs); i++) {
        if (abbrevs[i] == abbrev) return i;
    }
    return -1;
}




/* Note that metakey MUST be prefix of datakeys, rdb save key switch detection
 * relay on that assumption. */
sds encodeMetaKey(int dbid, const char* key, size_t keylen_) {
    keylen_t keylen = keylen_;
    size_t rawkeylen = sizeof(dbid)+sizeof(keylen)+keylen;
    sds rawkey = sdsnewlen(SDS_NOINIT,rawkeylen), ptr = rawkey;
    memcpy(ptr, &dbid, sizeof(dbid)), ptr += sizeof(dbid);
    memcpy(ptr, &keylen, sizeof(keylen_t)), ptr += sizeof(keylen_t);
    memcpy(ptr, key, keylen), ptr += keylen;
    return rawkey;
}

sds rocksEncodeMetaKey(redisDb *db, sds key) {
    return encodeMetaKey(db->id, key, key ? sdslen(key) : 0);
}

int rocksDecodeMetaKey(const char *raw, size_t rawlen, int *dbid,
        const char **key, size_t *keylen) {
    keylen_t keylen_;
    if (raw == NULL || rawlen < sizeof(int)+sizeof(keylen_t)) return -1;
    if (dbid) *dbid = *(int*)raw;
    raw += sizeof(int), rawlen -= sizeof(int);
    keylen_ = *(keylen_t*)raw;
    if (keylen) *keylen = keylen_;
    raw += sizeof(keylen_t), rawlen -= sizeof(keylen_t);
    if (key) *key = raw;
    if (rawlen < keylen_) return -1;
    return 0;
}

/* extend: pointer to rawkey, not allocated. */
int rocksDecodeMetaVal(const char *raw, size_t rawlen, int *pswap_type,
        long long *pexpire, uint64_t *pversion, const char **pextend,
        size_t *pextend_len) {
    const char *ptr = raw;
    size_t len = rawlen;
    long long expire;
    int swap_type;
    uint64_t encoded_version;

    if (rawlen < 1 + sizeof(expire) + sizeof(encoded_version)) return -1;

    if ((swap_type = abbrev2ObjectType(ptr[0])) < 0) return -1;
    ptr++, len--;
    if (pswap_type) *pswap_type = swap_type;

    expire = *(long long*)ptr;
    ptr += sizeof(long long), len -= sizeof(long long);
    if (pexpire) *pexpire = expire;

    encoded_version = *(uint64_t*)ptr;
    if (pversion) *pversion = rocksDecodeVersion(encoded_version);
    ptr += sizeof(encoded_version), len -= sizeof(encoded_version);

    if (pextend_len) *pextend_len = len;
    if (pextend) {
        *pextend = len > 0 ? ptr : NULL;
    }

    return 0;
}

sds rocksEncodeDbRangeStartKey(int dbid) {
    sds rawkey = sdsnewlen(SDS_NOINIT,sizeof(dbid));
    memcpy(rawkey, &dbid, sizeof(dbid));
    return rawkey;
}

sds rocksEncodeDbRangeEndKey(int dbid) {
    return rocksEncodeDbRangeStartKey(dbid+1);
}
