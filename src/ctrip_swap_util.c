#include "ctrip_swap.h"

/* Copyright (c) 2021, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "endianconv.h"

/* See keyIsExpired for more details */
size_t ctripDbSize(redisDb *db) {
    return dictSize(db->dict) + db->cold_keys;
}

/* See keyIsExpired for more details */
int timestampIsExpired(mstime_t when) {
    mstime_t now;

    if (when < 0) return 0;
    if (server.loading) return 0;
    if (server.lua_caller) {
        now = server.lua_time_snapshot;
    } else if (server.fixed_time_expire > 0) {
        now = server.mstime;
    } else {
        now = mstime();
    }
    return now > when;
}


sds objectDump(robj *o) {
    sds repr = sdsempty();

    repr = sdscatprintf(repr,"type:%s, ", getObjectTypeName(o));
    switch (o->encoding) {
    case OBJ_ENCODING_INT:
        repr = sdscatprintf(repr, "encoding:int, value:%ld", (long)o->ptr);
        break;
    case OBJ_ENCODING_EMBSTR:
        repr = sdscatprintf(repr, "encoding:emedstr, value:%.*s", (int)sdslen(o->ptr), (sds)o->ptr);
        break;
    case OBJ_ENCODING_RAW:
        repr = sdscatprintf(repr, "encoding:raw, value:%.*s", (int)sdslen(o->ptr), (sds)o->ptr);
        break;
    default:
        repr = sdscatprintf(repr, "encoding:%d, value:nan", o->encoding);
        break;
    }
    return repr;
}

/* For big Hash/Set/Zset object, object might changed by swap thread in
 * createOrMergeObject, so iterating those big objects in main thread without
 * requestGetIOAndLock is not safe. intead we just estimate those object size. */
size_t objectComputeSize(robj *o, size_t sample_size);
size_t objectEstimateSize(robj *o) {
    size_t asize = 0;
    if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT) {
        dict *d = o->ptr;
        asize = sizeof(*o)+sizeof(dict)+(sizeof(struct dictEntry*)*dictSlots(d));
        asize += DEFAULT_HASH_FIELD_SIZE*dictSize(d);
    } else {
        asize = objectComputeSize(o,5);
    }
    return asize;
}
size_t keyEstimateSize(redisDb *db, robj *key) {
    robj *val = lookupKey(db, key, LOOKUP_NOTOUCH);
    return val ? objectEstimateSize(val): 0;
}

/* Create an unshared object from src, note that o.refcount decreased. */
robj *unshareStringValue(robj *o) {
    serverAssert(o->type == OBJ_STRING);
    if (o->refcount != 1 || o->encoding != OBJ_ENCODING_RAW) {
        robj *decoded = getDecodedObject(o);
        decrRefCount(o);
        o = createRawStringObject(decoded->ptr, sdslen(decoded->ptr));
        decrRefCount(decoded);
        return o;
    } else {
        return o;
    }
}

const char *strObjectType(int type) {
    switch (type) {
    case OBJ_STRING: return "string";
    case OBJ_HASH: return "hash";
    case OBJ_LIST: return "list";
    case OBJ_SET: return "set";
    case OBJ_ZSET: return "zset";
    case OBJ_STREAM: return "stream";
    default: return "unknown";
    }
}

static inline char objectType2Abbrev(int object_type) {
    char abbrevs[] = {'K','L','S','Z','H','M','X'};
    if (object_type >= 0 && object_type < (int)sizeof(abbrevs)) {
        return abbrevs[object_type];
    } else {
        return '?';
    }
}

static inline char abbrev2ObjectType(char abbrev) {
    char abbrevs[] = {'K','L','S','Z','H','M','X'};
    for (size_t i = 0; i < sizeof(abbrevs); i++) {
        if (abbrevs[i] == abbrev) return i;
    }
    return -1;
}

sds rocksEncodeMetaVal(int object_type, long long expire, sds extend) {
    size_t len = 1 + sizeof(expire) + (extend ? sdslen(extend) : 0);
    sds raw = sdsnewlen(SDS_NOINIT,len), ptr = raw;
    ptr[0] = objectType2Abbrev(object_type), ptr++;
    memcpy(raw+1,&expire,sizeof(expire)), ptr+=sizeof(expire);
    if (extend) memcpy(ptr,extend,sdslen(extend));
    return raw;
}

/* extend: pointer to rawkey, not allocated. */
int rocksDecodeMetaVal(const char *raw, size_t rawlen, int *pobject_type,
        long long *pexpire, const char **pextend, size_t *pextend_len) {
    const char *ptr = raw;
    size_t len = rawlen;
    long long expire;
    int object_type;

    if (rawlen < 1 + sizeof(expire)) return -1;

    if ((object_type = abbrev2ObjectType(ptr[0])) < 0) return -1;
    ptr++, len--;
    if (pobject_type) *pobject_type = object_type;

    expire = *(long long*)ptr;
    ptr += sizeof(long long), len -= sizeof(long long);
    if (pexpire) *pexpire = expire;

    if (pextend_len) *pextend_len = len;
    if (pextend) {
        *pextend = len > 0 ? ptr : NULL;
    }

    return 0;
}
typedef unsigned int keylen_t;
sds rocksEncodeDataKey(redisDb *db, sds key, sds subkey) {
    int dbid = db->id;
    keylen_t keylen = key ? sdslen(key) : 0;
    keylen_t subkeylen = subkey ? sdslen(subkey) : 0;
    size_t rawkeylen = sizeof(dbid)+sizeof(keylen)+keylen+1+subkeylen;
    sds rawkey = sdsnewlen(SDS_NOINIT,rawkeylen), ptr = rawkey;
    memcpy(ptr, &dbid, sizeof(dbid)), ptr += sizeof(dbid);
    memcpy(ptr, &keylen, sizeof(keylen_t)), ptr += sizeof(keylen_t);
    memcpy(ptr, key, keylen), ptr += keylen;
    if (subkey) {
        memset(ptr,ROCKS_KEY_FLAG_SUBKEY,1), ptr += 1;
        memcpy(ptr, subkey, subkeylen), ptr += subkeylen;
    } else {
        memset(ptr,ROCKS_KEY_FLAG_NONE,1), ptr += 1;
    }
    return rawkey;
}

int rocksDecodeDataKey(const char *raw, size_t rawlen, int *dbid,
        const char **key, size_t *keylen,
        const char **subkey, size_t *subkeylen) {
    keylen_t keylen_;
    if (raw == NULL || rawlen < sizeof(int)+sizeof(keylen_t)+1) return -1;
    if (dbid) *dbid = *(int*)raw;
    raw += sizeof(int), rawlen -= sizeof(int);
    keylen_ = *(keylen_t*)raw;
    if (keylen) *keylen = keylen_;
    raw += sizeof(keylen_t), rawlen -= sizeof(keylen_t);
    if (key) *key = raw;
    if (rawlen < keylen_) return -1;
    raw += keylen_, rawlen -= keylen_;
    if (subkeylen) *subkeylen = rawlen - 1;
    if (subkey) {
        *subkey = raw[0] == ROCKS_KEY_FLAG_SUBKEY ? raw + 1 : NULL;
    }
    return 0;
}

sds rocksEncodeValRdb(robj *value) {
    rio sdsrdb;
    rioInitWithBuffer(&sdsrdb,sdsempty());
    rdbSaveObjectType(&sdsrdb,value) ;
    rdbSaveObject(&sdsrdb,value,NULL);
    return sdsrdb.io.buffer.ptr;
}

robj *rocksDecodeValRdb(sds raw) {
    robj *value;
    rio sdsrdb;
    int rdbtype;
    rioInitWithBuffer(&sdsrdb,raw);
    rdbtype = rdbLoadObjectType(&sdsrdb);
    value = rdbLoadObject(rdbtype,&sdsrdb,NULL,NULL);
    return value;
}

sds rocksEncodeObjectMetaLen(unsigned long len) {
    return sdsnewlen(&len,sizeof(len));
}

long rocksDecodeObjectMetaLen(const char *raw, size_t rawlen) {
    if (rawlen != sizeof(unsigned long)) return -1;
    return *(long*)raw;
}

sds rocksGenerateEndKey(sds start_key) {
    sds end_key = sdsdup(start_key);
    end_key[sdslen(end_key) - 1] = (uint8_t)ROCKS_KEY_FLAG_DELETE;
    return end_key;
}

sds encodeMetaScanKey(unsigned long cursor, int limit, sds seek) {
    size_t len = sizeof(cursor) + sizeof(limit) + (seek ? 0 : sdslen(seek));
    sds result = sdsnewlen(SDS_NOINIT,len);
    char *ptr = result;

    memcpy(ptr,&cursor,sizeof(cursor)), ptr+=sizeof(cursor);
    memcpy(ptr,&limit,sizeof(limit)), ptr+=sizeof(limit);
    if (seek) memcpy(ptr,seek,sdslen(seek));
    return result;
}

int decodeMetaScanKey(sds meta_scan_key, unsigned long *cursor, int *limit,
        const char **seek, size_t *seeklen) {
    size_t len = sizeof(unsigned long) + sizeof(int);
    const char *ptr = meta_scan_key;
    if (sdslen(meta_scan_key) < len) return -1;
    if (cursor) *cursor = *(unsigned long*)ptr;
    ptr += sizeof(unsigned long);
    if (limit) *limit = *(int*)ptr;
    ptr += sizeof(int);
    if (seek) *seek = ptr;
    if (seeklen) *seeklen = sdslen(meta_scan_key) - len;
    return 0;
}

int encodeFixed64(char* buf, uint64_t value) {
    if (BYTE_ORDER == BIG_ENDIAN) {
        memcpy(buf, &value, sizeof(value));
        return sizeof(value);
    } else {
        buf[0] = (uint8_t)((value >> 56) & 0xff);
        buf[1] = (uint8_t)((value >> 48) & 0xff);
        buf[2] = (uint8_t)((value >> 40) & 0xff);
        buf[3] = (uint8_t)((value >> 32) & 0xff);
        buf[4] = (uint8_t)((value >> 24) & 0xff);
        buf[5] = (uint8_t)((value >> 16) & 0xff);
        buf[6] = (uint8_t)((value >> 8) & 0xff);
        buf[7] = (uint8_t)(value & 0xff);
        return 8;
    }
}

int encodeDouble(char* buf, double value) {
    uint64_t u64;
    memcpy(&u64, &value, sizeof(value));
    uint64_t* ptr = &u64;
    if ((*ptr >> 63) == 1) {
        // signed bit would be zero
        *ptr ^= 0xffffffffffffffff;
    } else {
        // signed bit would be one
        *ptr |= 0x8000000000000000;
    }
    return encodeFixed64(buf, *ptr);
}

uint32_t decodeFixed32(const char *ptr) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    uint32_t value;
    memcpy(&value, ptr, sizeof(value));
    return value;
  } else {
    return (((uint32_t)((uint8_t)(ptr[3])))
        | ((uint32_t)((uint8_t)(ptr[2])) << 8)
        | ((uint32_t)((uint8_t)(ptr[1])) << 16)
        | ((uint32_t)((uint8_t)(ptr[0])) << 24));
  }
}


uint64_t decodeFixed64(const char *ptr) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    uint64_t value;
    memcpy(&value, ptr, sizeof(value));
    return value;
  } else {
    uint64_t hi = decodeFixed32(ptr);
    uint64_t lo = decodeFixed32(ptr+4);
    return (hi << 32) | lo;
  }
}

int decodeDouble(char* val, double* score) {
    uint64_t decoded = decodeFixed64(val);
    if ((decoded >> 63) == 0) {
        decoded ^= 0xffffffffffffffff;
    } else {
        decoded &= 0x7fffffffffffffff;
    }
    double value;
    memcpy(&value, &decoded, sizeof(value));
    *score = value;
    return sizeof(value);
}

sds encodeScoreHead(sds rawkey, int dbid, sds key, keylen_t keylen) {
    sds ptr = rawkey;
    memcpy(ptr, &dbid, sizeof(dbid)), ptr += sizeof(dbid);
    memcpy(ptr, &keylen, sizeof(keylen_t)), ptr += sizeof(keylen_t);
    memcpy(ptr, key, keylen), ptr += keylen;
    return ptr;
}

sds encodeScorePriex(redisDb* db, sds key) {
    int dbid = db->id;
    keylen_t keylen = key ? sdslen(key) : 0;
    keylen_t rawkeylen = sizeof(dbid)+sizeof(keylen)+keylen;
    sds rawkey = sdsnewlen(SDS_NOINIT,rawkeylen),
        ptr = encodeScoreHead(rawkey, dbid, key, keylen);
    return rawkey;
}

sds encodeScoreKey(redisDb* db ,sds key, sds subkey, double score) {
    int dbid = db->id;
    keylen_t keylen = key ? sdslen(key) : 0;
    keylen_t subkeylen = subkey ? sdslen(subkey): 0;
    keylen_t rawkeylen =  sizeof(dbid)+sizeof(keylen)+keylen+sizeOfDouble+subkeylen;
    sds rawkey = sdsnewlen(SDS_NOINIT,rawkeylen), 
        ptr = encodeScoreHead(rawkey, dbid, key, keylen);
    ptr += encodeDouble(ptr, score);
    if (subkey) {
        memcpy(ptr, subkey, subkeylen), ptr += subkeylen;
    }
    return rawkey;
}



int decodeScoreKey(char* raw, int rawlen, int* dbid, char** key, size_t* keylen, char** subkey, size_t* subkeylen, double* score) {
    keylen_t keylen_;
    if (raw == NULL || rawlen < sizeof(int)+sizeof(keylen_t)) return -1;
    if (dbid) *dbid = *(int*)raw;
    raw += sizeof(int), rawlen -= sizeof(int);
    keylen_ = *(keylen_t*)raw;
    if (keylen) *keylen = keylen_;
    raw += sizeof(keylen_t), rawlen -= sizeof(keylen_t);
    if (key) *key = raw;
    if (rawlen < keylen_) return -1;
    raw += keylen_, rawlen -= keylen_;
    int double_offset = decodeDouble(raw, score);
    raw += double_offset;
    rawlen -= double_offset;
    if (subkeylen) *subkeylen = rawlen;
    if (subkey) {
        *subkey = rawlen > 0 ? raw : NULL;
    }
    return 0;
}


sds encodeIntervalSds(int ex, MOVE IN sds data) {
    sds result;
    if (ex) {
        result = sdscatsds(sdsnewlen("(", 1), data);
    } else {
        result = sdscatsds(sdsnewlen("[", 1), data);
    }
    sdsfree(data);
    return result;
}

int decodeIntervalSds(sds data, int* ex, char** raw, size_t* rawlen) {
    if (sdslen(data) == 0) {
        return C_ERR;
    }
    switch (data[0])
    {
        case '(':
            *ex = 1;
            break;
        case '[':
            *ex = 0;
            break;
        default:
            return C_ERR;
            break;
    }
    *raw = data + 1;
    *rawlen = sdslen(data) - 1;
    return C_OK;
}

#ifdef REDIS_TEST

int swapUtilTest(int argc, char **argv, int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    int error = 0;
    redisDb* db = server.db + 0;

    TEST("util - encode & decode object meta len") {
        sds raw;
        raw = rocksEncodeObjectMetaLen(666);
        test_assert(666 == rocksDecodeObjectMetaLen(raw,sdslen(raw)));
        sdsfree(raw);
        raw = rocksEncodeObjectMetaLen(0);
        test_assert(0 == rocksDecodeObjectMetaLen(raw,sdslen(raw)));
        sdsfree(raw);
        raw = rocksEncodeObjectMetaLen(-1);
        test_assert(-1 == rocksDecodeObjectMetaLen(raw,sdslen(raw)));
        sdsfree(raw);
        raw = rocksEncodeObjectMetaLen(-666);
        test_assert(-666 == rocksDecodeObjectMetaLen(raw,sdslen(raw)));
        sdsfree(raw);
    }

    TEST("util - encode & decode key") {
        sds key = sdsnew("key1");
        sds f1 = sdsnew("f1");
        int dbId = 123456789;
        const char *keystr = NULL, *subkeystr = NULL;
        size_t klen = 123456789, slen = 123456789;
        sds empty = sdsempty();

        // util - encode & decode no subkey
        sds rocksKey = rocksEncodeDataKey(db,key,NULL);
        rocksDecodeDataKey(rocksKey,sdslen(rocksKey),&dbId,&keystr,&klen,&subkeystr,&slen);
        test_assert(dbId == db->id);
        test_assert(memcmp(key,keystr,klen) == 0);
        test_assert(subkeystr == NULL);
        sdsfree(rocksKey);
        // util - encode & decode with subkey
        rocksKey = rocksEncodeDataKey(db,key,f1);
        rocksDecodeDataKey(rocksKey,sdslen(rocksKey),&dbId,&keystr,&klen,&subkeystr,&slen);
        test_assert(dbId == db->id);
        test_assert(memcmp(key,keystr,klen) == 0);
        test_assert(memcmp(f1,subkeystr,slen) == 0);
        test_assert(sdslen(f1) == slen);
        sdsfree(rocksKey);
        // util - encode & decode with empty subkey
        rocksKey = rocksEncodeDataKey(db,key,empty);
        rocksDecodeDataKey(rocksKey,sdslen(rocksKey),&dbId,&keystr,&klen,&subkeystr,&slen);
        test_assert(dbId == db->id);
        test_assert(memcmp(key,keystr,klen) == 0);
        test_assert(memcmp("",subkeystr,slen) == 0);
        test_assert(slen == 0);
        sdsfree(rocksKey);
        // util - encode end key
        sds start_key = rocksEncodeDataKey(db,key,NULL);
        sds end_key = rocksGenerateEndKey(start_key);
        test_assert(sdscmp(start_key, end_key) < 0);
        rocksKey = rocksEncodeDataKey(db,key,empty);
        test_assert(sdscmp(rocksKey, start_key) > 0 && sdscmp(rocksKey, end_key) < 0);
        sdsfree(rocksKey);

        rocksKey = rocksEncodeDataKey(db,key,f1);
        test_assert(sdscmp(rocksKey, start_key) > 0 && sdscmp(rocksKey, end_key) < 0);
        sdsfree(rocksKey);

        sdsfree(empty);
    }

    return error;
}

#endif
