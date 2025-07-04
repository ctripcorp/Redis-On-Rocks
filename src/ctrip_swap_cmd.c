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

#include "ctrip_swap.h"
#include <math.h>
#include "slowlog.h"

struct redisCommand redisCommandTable[SWAP_CMD_COUNT] = {
    {"module",moduleCommand,-2,
     "admin no-script",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"get",getCommand,2,
     "read-only fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"getex",getexCommand,-2,
     "write fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"getdel",getdelCommand,2,
     "write fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    /* Note that we can't flag set as fast, since it may perform an
     * implicit DEL of a large key. */
    {"set",setCommand,-3,
     "write use-memory @string @swap_string @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_OVERWRITE,1,1,1,0,0,0},

    {"setnx",setnxCommand,3,
     "write use-memory fast @string @swap_string @swap_keyspace",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"setex",setexCommand,4,
     "write use-memory @string @swap_string @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_OVERWRITE,1,1,1,0,0,0},

    {"psetex",psetexCommand,4,
     "write use-memory @string @swap_string @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_OVERWRITE,1,1,1,0,0,0},

    {"append",appendCommand,3,
     "write use-memory fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"strlen",strlenCommand,2,
     "read-only fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"del",delCommand,-2,
     "write @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_DEL_MOCK_VALUE,1,-1,1,0,0,0},

    {"unlink",unlinkCommand,-2,
     "write fast @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_DEL_MOCK_VALUE,1,-1,1,0,0,0},

    {"exists",existsCommand,-2,
     "read-only fast @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,-1,1,0,0,0},

    {"setbit",setbitCommand,4,
     "write use-memory @bitmap @swap_bitmap",
     0,NULL,getKeyRequestsSetbit,SWAP_IN,0,1,1,1,0,0,0},

    {"getbit",getbitCommand,3,
     "read-only fast @bitmap @swap_bitmap",
     0,NULL,getKeyRequestsGetbit,SWAP_IN,0,1,1,1,0,0,0},

    {"bitfield",bitfieldCommand,-2,
     "write use-memory @bitmap @swap_bitmap",
     0,NULL,getKeyRequestsBitField,SWAP_IN,0,1,1,1,0,0,0},

    {"bitfield_ro",bitfieldroCommand,-2,
     "read-only fast @bitmap @swap_bitmap",
     0,NULL,getKeyRequestsBitField,SWAP_IN,0,1,1,1,0,0,0},

    {"setrange",setrangeCommand,4,
     "write use-memory @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"getrange",getrangeCommand,4,
     "read-only @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"substr",getrangeCommand,4,
     "read-only @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"incr",incrCommand,2,
     "write use-memory fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"decr",decrCommand,2,
     "write use-memory fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"mget",mgetCommand,-2,
     "read-only fast @string @swap_string @swap_keyspace",
     0,NULL,NULL,SWAP_IN,0,1,-1,1,0,0,0},

    {"rpush",rpushCommand,-3,
     "write use-memory fast @list @swap_list",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"lpush",lpushCommand,-3,
     "write use-memory fast @list @swap_list",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"rpushx",rpushxCommand,-3,
     "write use-memory fast @list @swap_list",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"lpushx",lpushxCommand,-3,
     "write use-memory fast @list @swap_list",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"linsert",linsertCommand,5,
     "write use-memory @list @swap_list",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"rpop",rpopCommand,-2,
     "write fast @list @swap_list",
     0,NULL,getKeyRequestsRpop,SWAP_IN,0,1,1,1,0,0,0},

    {"lpop",lpopCommand,-2,
     "write fast @list @swap_list",
     0,NULL,getKeyRequestsLpop,SWAP_IN,0,1,1,1,0,0,0},

    {"brpop",brpopCommand,-3,
     "write no-script @list @blocking @swap_list",
     0,NULL,getKeyRequestsBrpop,SWAP_IN,0,1,-2,1,0,0,0},

    {"brpoplpush",brpoplpushCommand,4,
     "write use-memory no-script @list @blocking @swap_list ",
     0,NULL,getKeyRequestsRpoplpush,SWAP_IN,0,1,2,1,0,0,0},

    {"blmove",blmoveCommand,6,
     "write use-memory no-script @list @blocking @swap_list",
     0,NULL,getKeyRequestsLmove,SWAP_IN,0,1,2,1,0,0,0},

    {"blpop",blpopCommand,-3,
     "write no-script @list @blocking @swap_list",
     0,NULL,getKeyRequestsBlpop,SWAP_IN,0,1,-2,1,0,0,0},

    {"llen",llenCommand,2,
     "read-only fast @list @swap_list",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"lindex",lindexCommand,3,
     "read-only @list @swap_list",
     0,NULL,getKeyRequestsLindex,SWAP_IN,0,1,1,1,0,0,0},

    {"lset",lsetCommand,4,
     "write use-memory @list @swap_list",
     0,NULL,getKeyRequestsLset,SWAP_IN,0,1,1,1,0,0,0},

    {"lrange",lrangeCommand,4,
     "read-only @list @swap_list",
     0,NULL,getKeyRequestsLrange,SWAP_IN,0,1,1,1,0,0,0},

    {"ltrim",ltrimCommand,4,
     "write @list @swap_list",
     0,NULL,getKeyRequestsLtrim,SWAP_IN,0,1,1,1,0,0,0},

    {"lpos",lposCommand,-3,
     "read-only @list @swap_list",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"lrem",lremCommand,4,
     "write @list @swap_list",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"rpoplpush",rpoplpushCommand,3,
     "write use-memory @list @swap_list",
     0,NULL,getKeyRequestsRpoplpush,SWAP_IN,0,1,2,1,0,0,0},

    {"lmove",lmoveCommand,5,
     "write use-memory @list @swap_list",
     0,NULL,getKeyRequestsLmove,SWAP_IN,0,1,2,1,0,0,0},

    {"sadd",saddCommand,-3,
     "write use-memory fast @set @swap_set",
     0,NULL,getKeyRequestsSadd,SWAP_IN,0,1,1,1,0,0,0},

    {"srem",sremCommand,-3,
     "write fast @set @swap_set",
     0,NULL,getKeyRequestsSrem,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    /* smove cmd intention flags set by getKeyRequestSmove */
    {"smove",smoveCommand,4,
     "write fast @set @swap_set",
     0,NULL,getKeyRequestSmove,SWAP_IN,0,1,2,1,0,0,0},

    {"sismember",sismemberCommand,3,
     "read-only fast @set @swap_set",
     0,NULL,getKeyRequestSmembers,SWAP_IN,0,1,1,1,0,0,0},

    {"smismember",smismemberCommand,-3,
     "read-only fast @set @swap_set",
     0,NULL,getKeyRequestSmembers,SWAP_IN,0,1,1,1,0,0,0},

    {"scard",swap_scardCommand,2,
     "read-only fast @set @swap_set",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"spop",spopCommand,-2,
     "write random fast @set @swap_set",
     0,NULL,NULL,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"srandmember",srandmemberCommand,-2,
     "read-only random @set @swap_set",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"sinter",sinterCommand,-2,
     "read-only to-sort @set @swap_set",
     0,NULL,NULL,SWAP_IN,0,1,-1,1,0,0,0},

    {"sinterstore",sinterstoreCommand,-3,
     "write use-memory @set @swap_set",
     0,NULL,getKeyRequestsSinterstore,SWAP_IN,0,1,-1,1,0,0,0},

    {"sunion",sunionCommand,-2,
     "read-only to-sort @set @swap_set",
     0,NULL,NULL,SWAP_IN,0,1,-1,1,0,0,0},

    {"sunionstore",sunionstoreCommand,-3,
     "write use-memory @set @swap_set",
     0,NULL,getKeyRequestsSunionstore,SWAP_IN,0,1,-1,1,0,0,0},

    {"sdiff",sdiffCommand,-2,
     "read-only to-sort @set @swap_set",
     0,NULL,NULL,SWAP_IN,0,1,-1,1,0,0,0},

    {"sdiffstore",sdiffstoreCommand,-3,
     "write use-memory @set @swap_set",
     0,NULL,getKeyRequestsSdiffstore,SWAP_IN,0,1,-1,1,0,0,0},

    {"smembers",sinterCommand,2,
     "read-only to-sort @set @swap_set",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"sscan",sscanCommand,-3,
     "read-only random @set @swap_set",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},
    /*  (zset type) write command flag should be SWAP_IN_DEL, Because the index (score_cf data) needs to be deleted */
    {"zadd",zaddCommand,-4,
     "write use-memory fast @sortedset @swap_zset",
     0,NULL,getKeyRequestsZAdd,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"zincrby",zincrbyCommand,4,
     "write use-memory fast @sortedset @swap_zset",
     0,NULL,getKeyRequestsZincrby,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"zrem",zremCommand,-3,
     "write fast @sortedset @swap_zset",
     0,NULL,getKeyRequestsZrem,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"zremrangebyscore",zremrangebyscoreCommand,4,
     "write @sortedset @swap_zset",
     0,NULL,getKeyRequestsZremRangeByScore,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"zremrangebyrank",zremrangebyrankCommand,4,
     "write @sortedset @swap_zset",
     0,NULL,NULL,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"zremrangebylex",zremrangebylexCommand,4,
     "write @sortedset @swap_zset",
     0,NULL,getKeyRequestsZremRangeByLex,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"zunionstore",zunionstoreCommand,-4,
     "write use-memory @sortedset @swap_zset @swap_set",
     0,zunionInterDiffStoreGetKeys,getKeyRequestsZunionstore,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"zinterstore",zinterstoreCommand,-4,
     "write use-memory @sortedset @swap_zset @swap_set",
     0,zunionInterDiffStoreGetKeys,getKeyRequestsZinterstore,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"zdiffstore",zdiffstoreCommand,-4,
     "write use-memory @sortedset @swap_zset @swap_set",
     0,zunionInterDiffStoreGetKeys,getKeyRequestsZdiffstore,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"zunion",zunionCommand,-3,
     "read-only @sortedset @swap_zset @swap_set",
     0,zunionInterDiffGetKeys,NULL,SWAP_IN,0,0,0,0,0,0,0},

    {"zinter",zinterCommand,-3,
     "read-only @sortedset @swap_zset @swap_set",
     0,zunionInterDiffGetKeys,NULL,SWAP_IN,0,0,0,0,0,0,0},

    {"zdiff",zdiffCommand,-3,
     "read-only @sortedset @swap_zset @swap_set",
     0,zunionInterDiffGetKeys,NULL,SWAP_IN,0,0,0,0,0,0,0},

    {"zrange",zrangeCommand,-4,
     "read-only @sortedset @swap_zset",
     0,NULL,getKeyRequestsZrange,SWAP_IN,0,1,1,1,0,0,0},

    {"zrangestore",zrangestoreCommand,-5,
     "write use-memory @sortedset @swap_zset",
     0,NULL,getKeyRequestsZrangestore,SWAP_IN,SWAP_IN_DEL,1,2,1,0,0,0},

    {"zrangebyscore",zrangebyscoreCommand,-4,
     "read-only @sortedset @swap_zset",
     0,NULL,getKeyRequestsZrangeByScore,SWAP_IN,0,1,1,1,0,0,0},

    {"zrevrangebyscore",zrevrangebyscoreCommand,-4,
     "read-only @sortedset @swap_zset",
     0,NULL,getKeyRequestsZrevrangeByScore,SWAP_IN,0,1,1,1,0,0,0},

    {"zrangebylex",zrangebylexCommand,-4,
     "read-only @sortedset @swap_zset",
     0,NULL,getKeyRequestsZrangeByLex,SWAP_IN,0,1,1,1,0,0,0},

    {"zrevrangebylex",zrevrangebylexCommand,-4,
     "read-only @sortedset @swap_zset",
     0,NULL,getKeyRequestsZrevrangeByLex,SWAP_IN,0,1,1,1,0,0,0},

    {"zcount",zcountCommand,4,
     "read-only fast @sortedset @swap_zset",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"zlexcount",zlexcountCommand,4,
     "read-only fast @sortedset @swap_zset",
     0,NULL,getKeyRequestsZlexCount,SWAP_IN,0,1,1,1,0,0,0},

    {"zrevrange",zrevrangeCommand,-4,
     "read-only @sortedset @swap_zset",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"zcard",zcardCommand,2,
     "read-only fast @sortedset @swap_zset",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"zscore",zscoreCommand,3,
     "read-only fast @sortedset @swap_zset",
     0,NULL,getKeyRequestsZScore,SWAP_IN,0,1,1,1,0,0,0},

    {"zmscore",zmscoreCommand,-3,
     "read-only fast @sortedset @swap_zset",
     0,NULL,getKeyRequestsZMScore,SWAP_IN,0,1,1,1,0,0,0},

    {"zrank",zrankCommand,3,
     "read-only fast @sortedset @swap_zset",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"zrevrank",zrevrankCommand,3,
     "read-only fast @sortedset @swap_zset",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"zscan",zscanCommand,-3,
     "read-only random @sortedset @swap_zset",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"zpopmin",zpopminCommand,-2,
     "write fast @sortedset @swap_zset",
     0,NULL,NULL,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"zpopmax",zpopmaxCommand,-2,
     "write fast @sortedset @swap_zset",
     0,NULL,NULL,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"bzpopmin",bzpopminCommand,-3,
     "write no-script fast @sortedset @blocking @swap_zset",
     0,NULL,getKeyRequestsZpopMin,SWAP_IN,SWAP_IN_DEL,1,-2,1,0,0,0},

    {"bzpopmax",bzpopmaxCommand,-3,
     "write no-script fast @sortedset @blocking @swap_zset",
     0,NULL,getKeyRequestsZpopMax,SWAP_IN,SWAP_IN_DEL,1,-2,1,0,0,0},

    {"zrandmember",zrandmemberCommand,-2,
     "read-only random @sortedset @swap_zset",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"hset",hsetCommand,-4,
     "write use-memory fast @hash @swap_hash",
     0,NULL,getKeyRequestsHset,SWAP_IN,0,1,1,1,0,0,0},

    {"hsetnx",hsetnxCommand,4,
     "write use-memory fast @hash @swap_hash",
     0,NULL,getKeyRequestsHsetnx,SWAP_IN,0,1,1,1,0,0,0},

    {"hget",hgetCommand,3,
     "read-only fast @hash @swap_hash",
     0,NULL,getKeyRequestsHget,SWAP_IN,0,1,1,1,0,0,0},

    {"hmset",hsetCommand,-4,
     "write use-memory fast @hash @swap_hash",
     0,NULL,getKeyRequestsHset,SWAP_IN,0,1,1,1,0,0,0},

    {"hmget",hmgetCommand,-3,
     "read-only fast @hash @swap_hash",
     0,NULL,getKeyRequestsHmget,SWAP_IN,0,1,1,1,0,0,0},

    {"hincrby",hincrbyCommand,4,
     "write use-memory fast @hash @swap_hash",
     0,NULL,getKeyRequestsHincrby,SWAP_IN,0,1,1,1,0,0,0},

    {"hincrbyfloat",hincrbyfloatCommand,4,
     "write use-memory fast @hash @swap_hash",
     0,NULL,getKeyRequestsHincrbyfloat,SWAP_IN,0,1,1,1,0,0,0},

    {"hdel",hdelCommand,-3,
     "write fast @hash @swap_hash",
     0,NULL,getKeyRequestsHdel,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"hlen",hlenCommand,2,
     "read-only fast @hash @swap_hash",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"hstrlen",hstrlenCommand,3,
     "read-only fast @hash @swap_hash",
     0,NULL,getKeyRequestsHstrlen,SWAP_IN,0,1,1,1,0,0,0},

    {"hkeys",hkeysCommand,2,
     "read-only to-sort @hash @swap_hash",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"hvals",hvalsCommand,2,
     "read-only to-sort @hash @swap_hash",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"hgetall",hgetallCommand,2,
     "read-only random @hash @swap_hash",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"hexists",hexistsCommand,3,
     "read-only fast @hash @swap_hash",
     0,NULL,getKeyRequestsHexists,SWAP_IN,0,1,1,1,0,0,0},

    {"hrandfield",hrandfieldCommand,-2,
     "read-only random @hash @swap_hash",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"hscan",hscanCommand,-3,
     "read-only random @hash @swap_hash",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"incrby",incrbyCommand,3,
     "write use-memory fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"decrby",decrbyCommand,3,
     "write use-memory fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"incrbyfloat",incrbyfloatCommand,3,
     "write use-memory fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"getset",getsetCommand,3,
     "write use-memory fast @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"mset",msetCommand,-3,
     "write use-memory @string @swap_string @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_OVERWRITE,1,-1,2,0,0,0},

    {"msetnx",msetnxCommand,-3,
     "write use-memory @string @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,-1,2,0,0,0},

    {"randomkey",randomkeyCommand,1,
     "read-only random @keyspace @swap_keyspace",
     0,NULL,getKeyRequestsMetaScan,SWAP_IN,SWAP_METASCAN_RANDOMKEY,0,0,0,0,0,0},

    {"select",selectCommand,2,
     "ok-loading fast ok-stale @keyspace",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"swapdb",swapdbCommand,3,
     "write fast @keyspace @dangerous @swap_keyspace",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"move",moveCommand,3,
     "write fast @keyspace",
     0,NULL,NULL,SWAP_NOP,0,1,1,1,0,0,0},

    {"copy",copyCommand,-3,
     "write use-memory @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,0,1,2,1,0,0,0},

    /* Like for SET, we can't mark rename as a fast command because
     * overwriting the target key may result in an implicit slow DEL. */
    {"rename",renameCommand,3,
     "write @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_DEL,1,2,1,0,0,0},

    {"renamenx",renamenxCommand,3,
     "write fast @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_DEL,1,2,1,0,0,0},

    {"expire",expireCommand,3,
     "write fast @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"expireat",expireatCommand,3,
     "write fast @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"pexpire",pexpireCommand,3,
     "write fast @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"pexpireat",pexpireatCommand,3,
     "write fast @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"keys",keysCommand,2,
     "read-only to-sort @keyspace @dangerous @swap_keyspace",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"scan",scanCommand,-2,
     "read-only random @keyspace @swap_keyspace",
     0,NULL,getKeyRequestsMetaScan,SWAP_IN,SWAP_METASCAN_SCAN,0,0,0,0,0,0},

    {"dbsize",dbsizeCommand,1,
     "read-only fast @keyspace",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"auth",authCommand,-2,
     "no-auth no-script ok-loading ok-stale fast @connection",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    /* We don't allow PING during loading since in Redis PING is used as
     * failure detection, and a loading server is considered to be
     * not available. */
    {"ping",pingCommand,-1,
     "ok-stale fast @connection",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"echo",echoCommand,2,
     "fast @connection",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"save",saveCommand,1,
     "admin no-script",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"bgsave",bgsaveCommand,-1,
     "admin no-script",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"refullsync",refullsyncCommand,1,
     "admin read-only",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"bgrewriteaof",bgrewriteaofCommand,1,
     "admin no-script",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"shutdown",shutdownCommand,-1,
     "admin no-script ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"lastsave",lastsaveCommand,1,
     "random fast ok-loading ok-stale @admin @dangerous",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"type",typeCommand,2,
     "read-only fast @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"multi",multiCommand,1,
     "no-script fast ok-loading ok-stale @transaction",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"exec",execCommand,1,
     "no-script no-slowlog ok-loading ok-stale @transaction",
     0,NULL,NULL,SWAP_IN,0,0,0,0,0,0,0},

    {"discard",discardCommand,1,
     "no-script fast ok-loading ok-stale @transaction",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"sync",syncCommand,1,
     "admin no-script",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"psync",syncCommand,-3,
     "admin no-script",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"xsync",syncCommand,-3,
     "admin no-script",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"replconf",replconfCommand,-1,
     "admin no-script ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"flushdb",flushdbCommand,-1,
     "write @keyspace @dangerous",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"flushall",flushallCommand,-1,
     "write @keyspace @dangerous",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"sort",sortCommand,-2,
     "write use-memory @list @set @sortedset @dangerous @swap_list @swap_set @swap_zset",
     0,sortGetKeys,getKeyRequestsSort,SWAP_IN,0,1,1,1,0,0,0},

    {"info",infoCommand,-1,
     "ok-loading ok-stale random @dangerous",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"monitor",monitorCommand,1,
     "admin no-script ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"ttl",ttlCommand,2,
     "read-only fast random @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"touch",touchCommand,-2,
     "read-only fast @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,-1,1,0,0,0},

    {"pttl",pttlCommand,2,
     "read-only fast random @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"persist",persistCommand,2,
     "write fast @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_META,1,1,1,0,0,0},

    {"slaveof",replicaofCommand,3,
     "admin no-script ok-stale",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"xslaveof",xslaveofCommand,3,
     "admin no-script ok-stale",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"replicaof",replicaofCommand,3,
     "admin no-script ok-stale",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"role",roleCommand,1,
     "ok-loading ok-stale no-script fast @dangerous",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"debug",debugCommand,-2,
     "admin no-script ok-loading ok-stale @swap_keyspace",
     0,debugGetKeys,getKeyRequestsDebug,SWAP_IN,0,0,0,0,0,0,0},

    {"config",configCommand,-2,
     "admin ok-loading ok-stale no-script",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"subscribe",subscribeCommand,-2,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"unsubscribe",unsubscribeCommand,-1,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"psubscribe",psubscribeCommand,-2,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"punsubscribe",punsubscribeCommand,-1,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"publish",publishCommand,3,
     "pub-sub ok-loading ok-stale fast may-replicate",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"pubsub",pubsubCommand,-2,
     "pub-sub ok-loading ok-stale random",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"watch",watchCommand,-2,
     "no-script fast ok-loading ok-stale @transaction @swap_keyspace",
     0,NULL,NULL,SWAP_NOP,0,1,-1,1,0,0,0},

    {"unwatch",unwatchCommand,1,
     "no-script fast ok-loading ok-stale @transaction",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"cluster",clusterCommand,-2,
     "admin ok-stale random",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"restore",restoreCommand,-4,
     "write use-memory @keyspace @dangerous @swap_keyspace",
     0,NULL,NULL,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"restore-asking",restoreCommand,-4,
     "write use-memory cluster-asking @keyspace @dangerous @swap_keyspace",
     0,NULL,NULL,SWAP_NOP,0,1,1,1,0,0,0},

    {"migrate",migrateCommand,-6,
     "write random @keyspace @dangerous @swap_keyspace",
     0,migrateGetKeys,NULL,SWAP_IN,SWAP_IN_DEL,3,3,1,0,0,0},

    {"asking",askingCommand,1,
     "fast @keyspace",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"readonly",readonlyCommand,1,
     "fast @keyspace",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"readwrite",readwriteCommand,1,
     "fast @keyspace",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"dump",dumpCommand,2,
     "read-only random @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"object",objectCommand,-2,
     "read-only random @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_IN,0,2,2,1,0,0,0},

    {"memory",memoryCommand,-2,
     "random read-only @swap_keyspace",
     0,memoryGetKeys,getKeyRequestsMemory,SWAP_IN,0,0,0,0,0,0,0},

    {"client",clientCommand,-2,
     "admin no-script random ok-loading ok-stale @connection",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"hello",helloCommand,-1,
     "no-auth no-script fast ok-loading ok-stale @connection",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    /* EVAL can modify the dataset, however it is not flagged as a write
     * command since we do the check while running commands from Lua.
     * 
     * EVAL and EVALSHA also feed monitors before the commands are executed,
     * as opposed to after.
      */
    {"eval",evalCommand,-3,
     "no-script no-monitor may-replicate @scripting @swap_keyspace",
     0,evalGetKeys,NULL,SWAP_IN,SWAP_IN_DEL,0,0,0,0,0,0},

    {"evalsha",evalShaCommand,-3,
     "no-script no-monitor may-replicate @scripting @swap_keyspace",
     0,evalGetKeys,NULL,SWAP_IN,SWAP_IN_DEL,0,0,0,0,0,0},

    {"slowlog",slowlogCommand,-2,
     "admin random ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"script",scriptCommand,-2,
     "no-script may-replicate @scripting",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"time",timeCommand,1,
     "random fast ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"bitop",bitopCommand,-4,
     "write use-memory @bitmap @swap_bitmap",
     0,NULL,getKeyRequestsBitop,SWAP_IN,0,2,-1,1,0,0,0},

    {"bitcount",bitcountCommand,-2,
     "read-only @bitmap @swap_bitmap",
     0,NULL,getKeyRequestsBitcount,SWAP_IN,0,1,1,1,0,0,0},

    {"bitpos",bitposCommand,-3,
     "read-only @bitmap @swap_bitmap",
     0,NULL,getKeyRequestsBitpos,SWAP_IN,0,1,1,1,0,0,0},

    {"wait",waitCommand,3,
     "no-script @keyspace",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"command",commandCommand,-1,
     "ok-loading ok-stale random @connection",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"geoadd",geoaddCommand,-5,
     "write use-memory @geo @swap_zset",
     0,NULL,getKeyRequestsGeoAdd,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    /* GEORADIUS has store options that may write. */
    {"georadius",georadiusCommand,-6,
     "write use-memory @geo @swap_zset",
     0,georadiusGetKeys,getKeyRequestsGeoRadius,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"georadius_ro",georadiusroCommand,-6,
     "read-only @geo @swap_zset",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"georadiusbymember",georadiusbymemberCommand,-5,
     "write use-memory @geo @swap_zset",
     0,georadiusGetKeys,getKeyRequestsGeoRadiusByMember,SWAP_IN,SWAP_IN_DEL,1,1,1,0,0,0},

    {"georadiusbymember_ro",georadiusbymemberroCommand,-5,
     "read-only @geo @swap_zset",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"geohash",geohashCommand,-2,
     "read-only @geo @swap_zset",
     0,NULL,getKeyRequestsGeoHash,SWAP_IN,0,1,1,1,0,0,0},

    {"geopos",geoposCommand,-2,
     "read-only @geo @swap_zset",
     0,NULL,getKeyRequestsGeoPos,SWAP_IN,0,1,1,1,0,0,0},

    {"geodist",geodistCommand,-4,
     "read-only @geo @swap_zset",
     0,NULL,getKeyRequestsGeoDist,SWAP_IN,0,1,1,1,0,0,0},

    {"geosearch",geosearchCommand,-7,
     "read-only @geo @swap_zset",
      0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"geosearchstore",geosearchstoreCommand,-8,
     "write use-memory @geo @swap_zset",
      0,NULL,getKeyRequestsGeoSearchStore,SWAP_IN,SWAP_IN_DEL,1,2,1,0,0,0},

    {"pfselftest",pfselftestCommand,1,
     "admin @hyperloglog @swap_string",
      0,NULL,NULL,SWAP_IN,0,0,0,0,0,0,0},

    {"pfadd",pfaddCommand,-2,
     "write use-memory fast @hyperloglog @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    /* Technically speaking PFCOUNT may change the key since it changes the
     * final bytes in the HyperLogLog representation. However in this case
     * we claim that the representation, even if accessible, is an internal
     * affair, and the command is semantically read only. */
    {"pfcount",pfcountCommand,-2,
     "read-only may-replicate @hyperloglog @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,-1,1,0,0,0},

    {"pfmerge",pfmergeCommand,-2,
     "write use-memory @hyperloglog @swap_string",
     0,NULL,NULL,SWAP_IN,0,1,-1,1,0,0,0},

    /* Unlike PFCOUNT that is considered as a read-only command (although
     * it changes a bit), PFDEBUG may change the entire key when converting
     * from sparse to dense representation */
    {"pfdebug",pfdebugCommand,-3,
     "admin write use-memory @hyperloglog @swap_string",
     0,NULL,NULL,SWAP_IN,0,2,2,1,0,0,0},

    {"xadd",xaddCommand,-5,
     "write use-memory fast random @stream",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"xrange",xrangeCommand,-4,
     "read-only @stream",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"xrevrange",xrevrangeCommand,-4,
     "read-only @stream",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"xlen",xlenCommand,2,
     "read-only fast @stream",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"xread",xreadCommand,-4,
     "read-only @stream @blocking",
     0,xreadGetKeys,NULL,SWAP_IN,0,0,0,0,0,0,0},

    {"xreadgroup",xreadCommand,-7,
     "write @stream @blocking",
     0,xreadGetKeys,NULL,SWAP_IN,0,0,0,0,0,0,0},

    {"xgroup",xgroupCommand,-2,
     "write use-memory @stream",
     0,NULL,NULL,SWAP_NOP,0,2,2,1,0,0,0},

    {"xsetid",xsetidCommand,3,
     "write use-memory fast @stream",
     0,NULL,NULL,SWAP_NOP,0,1,1,1,0,0,0},

    {"xack",xackCommand,-4,
     "write fast random @stream",
     0,NULL,NULL,SWAP_NOP,0,1,1,1,0,0,0},

    {"xpending",xpendingCommand,-3,
     "read-only random @stream",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"xclaim",xclaimCommand,-6,
     "write random fast @stream",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"xautoclaim",xautoclaimCommand,-6,
     "write random fast @stream",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"xinfo",xinfoCommand,-2,
     "read-only random @stream",
     0,NULL,NULL,SWAP_IN,0,2,2,1,0,0,0},

    {"xdel",xdelCommand,-3,
     "write fast @stream",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"xtrim",xtrimCommand,-4,
     "write random @stream",
     0,NULL,NULL,SWAP_IN,0,1,1,1,0,0,0},

    {"post",securityWarningCommand,-1,
     "ok-loading ok-stale read-only",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"host:",securityWarningCommand,-1,
     "ok-loading ok-stale read-only",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"latency",latencyCommand,-2,
     "admin no-script ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"lolwut",lolwutCommand,-1,
     "read-only fast",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"acl",aclCommand,-2,
     "admin no-script ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"stralgo",stralgoCommand,-2,
     "read-only @string @swap_string",
     0,lcsGetKeys,NULL,SWAP_IN,0,0,0,0,0,0,0},

    {"reset",resetCommand,1,
     "no-script ok-stale ok-loading fast @connection",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"failover",failoverCommand,-1,
     "admin no-script ok-stale",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    /* evict command used by shared fake to identify swap does
     * not need any swap before command proc, but triggers swap in proc. */
	{"swap.evict",swapEvictCommand,-2,
	 "read-only fast @keyspace @swap_keyspace",
     0,NULL,getKeyRequestsNone,SWAP_OUT,0,1,-1,1,0,0,0},

     {"swap.load",swapLoadCommand,-2,
      "read-only fast @swap_keyspace",
      0,NULL,getKeyRequestsNone,SWAP_IN,SWAP_IN_FORCE_HOT,1,-1,1,0,0,0},

	{"swap.expired",swapExpiredCommand,1,
	 "write fast @keyspace @swap_keyspace",
	 0,NULL,getKeyRequestsNone,SWAP_NOP,0,1,-1,1,0,0,0},

    {"swap.scanexpire",swapScanexpireCommand,1,
     "read-only no-script @keyspace @swap_keyspace",
     0,NULL,NULL,SWAP_NOP,0,1,-1,1,0,0,0},

	{"swap",swapCommand,-2,
	 "read-only fast",
	 0,NULL,NULL,SWAP_IN,0,0,0,0,0,0,0},

    {"swap.mutexop",swapMutexopCommand,1,
    "admin no-script",
    0,NULL,getKeyRequestsNone,SWAP_NOP,0,0,0,0,0,0,0},

	{"swap.debug",swapDebugCommand,-2,
	 "admin no-script ok-stale @swap_keyspace",
     0,NULL,getKeyRequestsGlobal,SWAP_NOP,0,0,0,0,0,0,0},

    {"gtid",gtidCommand, -3,
     "write use-memory no-script",
     0,NULL,getKeyRequestsGtid,SWAP_NOP/*not used*/,0,0,0,0,0,0,0},

    {"gtidx",gtidxCommand, -2,
     "admin no-script random ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"swap.slowlog",swapSlowlogCommand, -2,
     "admin random ok-loading ok-stale",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

    {"swap.info",swapInfoCommand, -2,
     "admin no-script ok-loading fast may-replicate",
     0,NULL,NULL,SWAP_NOP,0,0,0,0,0,0,0},

};

struct SwapDataTypeItem {
    const char *name;
    uint64_t flag;
} SwapCommandDataTypes[] = {
    {"swap_keyspace", CMD_SWAP_DATATYPE_KEYSPACE},
    {"swap_string", CMD_SWAP_DATATYPE_STRING},
    {"swap_hash", CMD_SWAP_DATATYPE_HASH},
    {"swap_set", CMD_SWAP_DATATYPE_SET},
    {"swap_zset", CMD_SWAP_DATATYPE_ZSET},
    {"swap_list", CMD_SWAP_DATATYPE_LIST},
    {"swap_bitmap", CMD_SWAP_DATATYPE_BITMAP},
    {NULL,0} /* Terminator. */
};
/* Given the category name the command returns the corresponding flag, or
 * zero if there is no match. */
uint64_t SwapCommandDataTypeFlagByName(const char *name) {
    for (int j = 0; SwapCommandDataTypes[j].flag != 0; j++) {
        if (!strcasecmp(name,SwapCommandDataTypes[j].name)) {
            return SwapCommandDataTypes[j].flag;
        }
    }
    return 0; /* No match. */
}
/* ----------------------------- swaps result ----------------------------- */
/* Prepare the getKeyRequestsResult struct to hold numswaps, either by using
 * the pre-allocated swaps or by allocating a new array on the heap.
 *
 * This function must be called at least once before starting to populate
 * the result, and can be called repeatedly to enlarge the result array.
 */
zrangespec* zrangespecdup(zrangespec* src) {
    zrangespec* dst = zmalloc(sizeof(zrangespec));
    dst->minex = src->minex;
    dst->min = src->min;
    dst->maxex = src->maxex;
    dst->max = src->max;
    return dst;
}

zlexrangespec* zlexrangespecdup(zlexrangespec* src) {
    zlexrangespec* dst = zmalloc(sizeof(zlexrangespec));
    dst->minex = src->minex;
    if (src->min != shared.minstring &&
        src->min != shared.maxstring) {
        dst->min = sdsdup(src->min);
    } else {
        dst->min = src->min;
    }

    dst->maxex = src->maxex;
    if (src->max != shared.minstring &&
        src->max != shared.maxstring) {
        dst->max = sdsdup(src->max);
    } else {
        dst->max = src->max;
    }
    return dst;
}

void copyKeyRequest(keyRequest *dst, keyRequest *src) {
    if (src->key) incrRefCount(src->key);
    dst->key = src->key;

    dst->level = src->level;
    dst->cmd_intention = src->cmd_intention;
    dst->cmd_intention_flags = src->cmd_intention_flags;
    dst->dbid = src->dbid;
    dst->type = src->type;
    dst->swap_cmd = src->swap_cmd;
    dst->deferred = src->deferred;
    dst->trace = src->trace;
    dst->cmd_flags = src->cmd_flags;

    switch (src->type) {
    case KEYREQUEST_TYPE_KEY:
        break;
    case KEYREQUEST_TYPE_SUBKEY:
        if (src->b.num_subkeys > 0)  {
            dst->b.subkeys = zmalloc(sizeof(robj*)*src->b.num_subkeys);
            for (int i = 0; i < src->b.num_subkeys; i++) {
                if (src->b.subkeys[i]) incrRefCount(src->b.subkeys[i]);
                dst->b.subkeys[i] = src->b.subkeys[i];
            }
        }
        dst->b.num_subkeys = src->b.num_subkeys;
        break;
    case KEYREQUEST_TYPE_RANGE:
        dst->l.num_ranges = src->l.num_ranges;
        dst->l.ranges = zmalloc(src->l.num_ranges*sizeof(struct range));
        memcpy(dst->l.ranges,src->l.ranges,src->l.num_ranges*sizeof(struct range));
        break;
    case KEYREQUEST_TYPE_SCORE:
        dst->zs.rangespec = zrangespecdup(src->zs.rangespec);
        dst->zs.reverse = src->zs.reverse;
        dst->zs.limit = src->zs.limit;
        break;
    case KEYREQUEST_TYPE_SAMPLE:
        dst->sp.count = src->sp.count;
        break;
    case KEYREQUEST_TYPE_BTIMAP_OFFSET:
        dst->bo.offset = src->bo.offset;
        break;
    case KEYREQUEST_TYPE_BTIMAP_RANGE:
        dst->br.start = src->br.start;
        dst->br.end = src->br.end;
        break;
    default:
        break;
    }

    dst->arg_rewrite[0] = src->arg_rewrite[0];
    dst->arg_rewrite[1] = src->arg_rewrite[1];
}

void moveKeyRequest(keyRequest *dst, keyRequest *src) {
    dst->key = src->key;
    src->key = NULL;
    dst->level = src->level;
    dst->cmd_intention = src->cmd_intention;
    dst->cmd_intention_flags = src->cmd_intention_flags;
    dst->dbid = src->dbid;
    dst->type = src->type;
    dst->swap_cmd = src->swap_cmd;
    dst->trace = src->trace;
    dst->deferred = src->deferred;
    dst->cmd_flags = src->cmd_flags;

    switch (src->type) {
    case KEYREQUEST_TYPE_KEY:
        break;
    case KEYREQUEST_TYPE_SUBKEY:
        dst->b.subkeys = src->b.subkeys;
        src->b.subkeys = NULL;
        dst->b.num_subkeys = src->b.num_subkeys;
        src->b.num_subkeys = 0;
        break;
    case KEYREQUEST_TYPE_RANGE:
        dst->l.num_ranges = src->l.num_ranges;
        dst->l.ranges = src->l.ranges;
        src->l.ranges = NULL;
        break;
    case KEYREQUEST_TYPE_SCORE:
        dst->zs.rangespec = src->zs.rangespec;
        src->zs.rangespec = NULL;
        dst->zs.reverse = src->zs.reverse;
        dst->zs.limit = src->zs.limit;
        break;
    case KEYREQUEST_TYPE_SAMPLE:
        dst->sp.count = src->sp.count;
        break;
    case KEYREQUEST_TYPE_BTIMAP_OFFSET:
        dst->bo.offset = src->bo.offset;
        break;
    case KEYREQUEST_TYPE_BTIMAP_RANGE:
        dst->br.start = src->br.start;
        dst->br.end = src->br.end;
        break;
    default:
        break;
    }

    dst->arg_rewrite[0] = src->arg_rewrite[0];
    dst->arg_rewrite[1] = src->arg_rewrite[1];
}

void keyRequestDeinit(keyRequest *key_request) {
    if (key_request == NULL) return;
    if (key_request->key) decrRefCount(key_request->key);
    key_request->key = NULL;

    switch (key_request->type) {
    case KEYREQUEST_TYPE_KEY:
        break;
    case KEYREQUEST_TYPE_SUBKEY:
        for (int i = 0; i < key_request->b.num_subkeys; i++) {
            if (key_request->b.subkeys[i])
                decrRefCount(key_request->b.subkeys[i]);
            key_request->b.subkeys[i] = NULL;
        }
        zfree(key_request->b.subkeys);
        key_request->b.subkeys = NULL;
        key_request->b.num_subkeys = 0;
        break;
    case KEYREQUEST_TYPE_RANGE:
        zfree(key_request->l.ranges);
        key_request->l.ranges = NULL;
        key_request->l.num_ranges = 0;
        break;
    case KEYREQUEST_TYPE_SCORE:
        if (key_request->zs.rangespec != NULL) {
            zfree(key_request->zs.rangespec);
            key_request->zs.rangespec = NULL;
        }
        break;
    case KEYREQUEST_TYPE_SAMPLE:
        key_request->sp.count = 0;
        break;
    case KEYREQUEST_TYPE_BTIMAP_OFFSET:
        key_request->bo.offset = 0;
        break;
    case KEYREQUEST_TYPE_BTIMAP_RANGE:
        key_request->br.start = 0;
        key_request->br.end = 0;
        break;
    default:
        break;
    }
}

void getKeyRequestsPrepareResult(getKeyRequestsResult *result, int num) {
	/* GETKEYS_RESULT_INIT initializes keys to NULL, point it to the
     * pre-allocated stack buffer here. */
	if (!result->key_requests) {
		serverAssert(!result->num);
		result->key_requests = result->buffer;
	}

	/* Resize if necessary */
	if (num > result->size) {
		if (result->key_requests != result->buffer) {
			/* We're not using a static buffer, just (re)alloc */
			result->key_requests = zrealloc(result->key_requests,
                    num*sizeof(keyRequest));
		} else {
			/* We are using a static buffer, copy its contents */
			result->key_requests = zmalloc(num*sizeof(keyRequest));
			if (result->num) {
				memcpy(result->key_requests,result->buffer,
                        result->num*sizeof(keyRequest));
            }
		}
		result->size = num;
	}
}

int expandKeyRequests(getKeyRequestsResult* result) {
    if (result->num == result->size) {
        int newsize = result->size +
            (result->size > 8192 ? 8192 : result->size);
        getKeyRequestsPrepareResult(result, newsize);
        return 1;
    }
    return 0;
}

keyRequest *getKeyRequestsAppendCommonResult(getKeyRequestsResult *result,
        int level, robj *key, int cmd_intention, int cmd_intention_flags, uint64_t cmd_flags,
        int dbid) {
    if (result->num == result->size) {
        int newsize = result->size +
            (result->size > 8192 ? 8192 : result->size);
        getKeyRequestsPrepareResult(result, newsize);
    }

    keyRequest *key_request = &result->key_requests[result->num++];
    key_request->level = level;
    key_request->key = key;
    key_request->cmd_intention = cmd_intention;
    key_request->cmd_intention_flags = cmd_intention_flags;
    key_request->cmd_flags = cmd_flags;
    key_request->dbid = dbid;
    key_request->trace = NULL;
    key_request->deferred = 0;
    argRewriteRequestInit(key_request->arg_rewrite + 0);
    argRewriteRequestInit(key_request->arg_rewrite + 1);
    return key_request;
}

void getKeyRequestsAppendScoreResult(getKeyRequestsResult *result, int level,
        robj *key, int reverse, zrangespec* rangespec, int limit, int cmd_intention,
        int cmd_intention_flags, uint64_t cmd_flags, int dbid) {
    expandKeyRequests(result);
    keyRequest *key_request = &result->key_requests[result->num++];
    key_request->level = level;
    key_request->key = key;
    key_request->type = KEYREQUEST_TYPE_SCORE;
    key_request->zs.reverse = reverse;
    key_request->zs.rangespec = rangespec;
    key_request->zs.limit = limit;
    key_request->cmd_intention = cmd_intention;
    key_request->cmd_intention_flags = cmd_intention_flags;
    key_request->cmd_flags = cmd_flags;
    key_request->dbid = dbid;
    key_request->trace = NULL;
    key_request->deferred = 0;
}

/* Note that key&subkeys ownership moved */
void getKeyRequestsAppendSubkeyResult(getKeyRequestsResult *result, int level,
        robj *key, int num_subkeys, robj **subkeys, int cmd_intention,
        int cmd_intention_flags, uint64_t cmd_flags, int dbid) {
    keyRequest *key_request = getKeyRequestsAppendCommonResult(result,level,
            key,cmd_intention,cmd_intention_flags,cmd_flags,dbid);

    key_request->type = KEYREQUEST_TYPE_SUBKEY;
    key_request->b.num_subkeys = num_subkeys;
    key_request->b.subkeys = subkeys;
    key_request->swap_cmd = NULL;
    key_request->trace = NULL;
    key_request->deferred = 0;
}

void getKeyRequestsAppendSampleResult(getKeyRequestsResult *result, int level,
        robj *key, int count, int cmd_intention,
        int cmd_intention_flags, uint64_t cmd_flags, int dbid) {
    keyRequest *key_request = getKeyRequestsAppendCommonResult(result,level,
            key,cmd_intention,cmd_intention_flags,cmd_flags,dbid);
    key_request->type = KEYREQUEST_TYPE_SAMPLE;
    key_request->sp.count = count;
    key_request->swap_cmd = NULL;
    key_request->trace = NULL;
    key_request->deferred = 0;
}

inline void getKeyRequestsAttachSwapTrace(getKeyRequestsResult * result, swapCmdTrace *swap_cmd,
                                   int from, int count) {
    if (server.swap_debug_trace_latency) {
        initSwapTraces(swap_cmd, count);
        for (int i = 0; i < count; i++) {
            result->key_requests[from + i].swap_cmd = swap_cmd;
            result->key_requests[from + i].trace = swap_cmd->swap_traces + i;
        }
    } else {
        swap_cmd->swap_cnt = count;
        for (int i = 0; i < count; i++) result->key_requests[from + i].swap_cmd = swap_cmd;
    }
}

void releaseKeyRequests(getKeyRequestsResult *result) {
    for (int i = 0; i < result->num; i++) {
        keyRequest *key_request = result->key_requests + i;
        keyRequestDeinit(key_request);
    }
}

void getKeyRequestsFreeResult(getKeyRequestsResult *result) {
    if (result && result->key_requests != result->buffer) {
        zfree(result->key_requests);
    }
}

/* NOTE that result.{key,subkeys} are ONLY REFS to client argv (since client
 * outlives getKeysResult if no swap action happend. key, subkey will be
  * copied (using incrRefCount) when async swap acutally proceed. */
static int _getSingleCmdKeyRequests(int dbid, struct redisCommand* cmd,
        robj** argv, int argc, getKeyRequestsResult *result) {
    if (cmd->getkeyrequests_proc == NULL) {
        int i, numkeys;
        getKeysResult keys = GETKEYS_RESULT_INIT;
        /* whole key swaping, swaps defined by command arity. */
        numkeys = getKeysFromCommand(cmd,argv,argc,&keys);
        getKeyRequestsPrepareResult(result,result->num+numkeys);
        for (i = 0; i < numkeys; i++) {
            robj *key = argv[keys.keys[i]];

            incrRefCount(key);
            getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,key,0,NULL,
                    cmd->intention,cmd->intention_flags,cmd->flags, dbid);
        }
        getKeysFreeResult(&keys);
        return 0;
    } else if (cmd->flags & CMD_MODULE) {
        /* TODO support module */
    } else {
        return cmd->getkeyrequests_proc(dbid,cmd,argv,argc,result);
    }
    return 0;
}

static void getSingleCmdKeyRequests(client *c, getKeyRequestsResult *result) {
    _getSingleCmdKeyRequests(c->db->id,c->cmd,c->argv,c->argc,result);
}

static inline int clientSwitchDb(client *c, int argidx) {
    long long dbid;
    if (getLongLongFromObject(c->argv[argidx],&dbid)) return C_ERR;
    if (dbid < 0 || dbid > server.dbnum)  return C_ERR;
    selectDb(c,dbid);
    return C_OK;
}

void freeClientSwapCmdTrace(client *c) {
    for (int i = 0; i < c->mstate.count; i++) {
        if (c->mstate.commands[i].swap_cmd) {
            swapCmdTraceFree(c->mstate.commands[i].swap_cmd);
            c->mstate.commands[i].swap_cmd = NULL;
        }
    }

    if (c->swap_cmd) {
        swapCmdTraceFree(c->swap_cmd);
        c->swap_cmd = NULL;
    }
}

void getKeyRequests(client *c, getKeyRequestsResult *result) {
    getKeyRequestsPrepareResult(result, MAX_KEYREQUESTS_BUFFER);
    swapCmdTrace *swap_cmd;

    if ((c->flags & CLIENT_MULTI) &&
            !(c->flags & (CLIENT_DIRTY_CAS | CLIENT_DIRTY_EXEC)) &&
            (c->cmd->proc == execCommand || isGtidExecCommand(c))) {
        /* if current is EXEC, we get swaps for all queue commands. */
        robj **orig_argv;
        int i, orig_argc;
        redisDb *orig_db;
        struct redisCommand *orig_cmd;
        int need_swap = 0;

        orig_argv = c->argv;
        orig_argc = c->argc;
        orig_cmd = c->cmd;
        orig_db = c->db;

        if (isGtidExecCommand(c)) {
            if (clientSwitchDb(c,2) != C_OK)
                return;
        }

        for (i = 0; i < c->mstate.count; i++) {
            int prev_keyrequest_num = result->num;

            c->argc = c->mstate.commands[i].argc;
            c->argv = c->mstate.commands[i].argv;
            c->cmd = c->mstate.commands[i].cmd;

            getSingleCmdKeyRequests(c, result);

            int requests_delta = result->num - prev_keyrequest_num;
            if (requests_delta) {
                need_swap = 1;
                swap_cmd = createSwapCmdTrace();
                getKeyRequestsAttachSwapTrace(result,swap_cmd,prev_keyrequest_num,requests_delta);
                c->mstate.commands[i].swap_cmd = swap_cmd;
            }
            for (int j = prev_keyrequest_num; j < result->num; j++) {
                result->key_requests[j].arg_rewrite[0].mstate_idx = i;
                result->key_requests[j].arg_rewrite[1].mstate_idx = i;
            }

            if (c->cmd->proc == selectCommand) {
                if (clientSwitchDb(c,1) != C_OK)
                    return;
            }
        }

        c->argv = orig_argv;
        c->argc = orig_argc;
        c->cmd = orig_cmd;
        c->db = orig_db;
        if (need_swap) c->swap_cmd = createSwapCmdTrace();
    } else {
        int prev_keyrequest_num = result->num;
        getSingleCmdKeyRequests(c, result);
        int requests_delta = result->num - prev_keyrequest_num;
        if (requests_delta) {
            swap_cmd = createSwapCmdTrace();
            getKeyRequestsAttachSwapTrace(result,swap_cmd,prev_keyrequest_num,requests_delta);
            c->swap_cmd = swap_cmd;
        }
    }
    result->swap_cmd = c->swap_cmd;
}

int getKeyRequestsNone(int dbid, struct redisCommand *cmd, robj **argv, int argc,
        getKeyRequestsResult *result) {
    UNUSED(dbid);
    UNUSED(cmd);
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(result);
    return 0;
}

/* Used by flushdb/flushall to get global scs(similar to table lock). */
int getKeyRequestsGlobal(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, getKeyRequestsResult *result) {
    UNUSED(cmd);
    UNUSED(argc);
    UNUSED(argv);
    getKeyRequestsPrepareResult(result,result->num+ 1);
    getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_SVR,NULL,0,NULL,
            cmd->intention,cmd->intention_flags,cmd->flags,dbid);
    return 0;
}

int getKeyRequestsMetaScan(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    char randbuf[16] = {0};
    robj *randkey;
    UNUSED(cmd);
    UNUSED(argc);
    UNUSED(argv);
    getRandomHexChars(randbuf,sizeof(randbuf));
    randkey = createStringObject(randbuf,sizeof(randbuf));
    getKeyRequestsPrepareResult(result,result->num+ 1);
    getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,randkey,0,NULL,
            cmd->intention,cmd->intention_flags,cmd->flags, dbid);
    return 0;
}

int getKeyRequestsOneDestKeyMultiSrcKeys(int dbid, struct redisCommand *cmd, robj **argv,
                                         int argc, struct getKeyRequestsResult *result, int dest_key_Index,
                                                 int first_src_key, int last_src_key) {
    UNUSED(cmd);
    if (last_src_key < 0) last_src_key += argc;
    getKeyRequestsPrepareResult(result, result->num + 1 + last_src_key - first_src_key + 1);

    if (dest_key_Index > 0 && dest_key_Index < argc) {
        incrRefCount(argv[dest_key_Index]);
        /**
         *  @example
         *      zunionstore dest(keyspace) 2 src1(list) src2(list)
        */
        getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,argv[dest_key_Index], 0, NULL,
                               SWAP_IN, SWAP_IN_DEL,cmd->flags | CMD_SWAP_DATATYPE_KEYSPACE, dbid);
    }

    for(int i = first_src_key; i <= last_src_key; i++) {
        incrRefCount(argv[i]);
        getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,argv[i], 0, NULL,
                                   SWAP_IN,0, cmd->flags, dbid);
    }

    return 0;
}

int getKeyRequestsSort(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    int i, j;

    UNUSED(cmd);

    struct {
        char *name;
        int skip;
    } skiplist[] = {
            {"limit", 2},
            {"get", 1},
            {"by", 1},
            {NULL, 0} /* End of elements. */
    };
    int storekeyIndex = -1;
    for (i = 2; i < argc; i++) {
        if (!strcasecmp(argv[i]->ptr,"store") && i+1 < argc) {
            /* we don't break after store key found to be sure
             * to process the *last* "STORE" option if multiple
             * ones are provided. This is same behavior as SORT. */
            storekeyIndex = i+1;
        }
        for (j = 0; skiplist[j].name != NULL; j++) {
            if (!strcasecmp(argv[i]->ptr,skiplist[j].name)) {
                i += skiplist[j].skip;
                break;
            }
        }
    }

    return getKeyRequestsOneDestKeyMultiSrcKeys(dbid, cmd, argv, argc, result, storekeyIndex, 1, 1);
}

int getKeyRequestsZunionInterDiffGeneric(int dbid, struct redisCommand *cmd, robj **argv, int argc,
        struct getKeyRequestsResult *result, int op) {
    UNUSED(op);
    long long setnum;
    if (getLongLongFromObject(argv[2], &setnum) != C_OK) {
        return C_ERR;
    }
    if (setnum < 1 || setnum + 3 > argc) {
        return C_ERR;
    }

    return getKeyRequestsOneDestKeyMultiSrcKeys(dbid, cmd, argv, argc, result, 1, 3, 2 + setnum);
}

int getKeyRequestsZunionstore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZunionInterDiffGeneric(dbid, cmd, argv, argc, result, SET_OP_UNION);
}

int getKeyRequestsZinterstore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZunionInterDiffGeneric(dbid, cmd, argv, argc, result, SET_OP_INTER);
}
int getKeyRequestsZdiffstore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZunionInterDiffGeneric(dbid, cmd, argv, argc, result, SET_OP_DIFF);
}

#define GETKEYS_RESULT_SUBKEYS_INIT_LEN 8
#define GETKEYS_RESULT_SUBKEYS_LINER_LEN 1024

int getKeyRequestsSingleKeyWithSubkeys(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result,
        int key_index, int first_subkey, int last_subkey, int subkey_step) {
    int i, num = 0, capacity = GETKEYS_RESULT_SUBKEYS_INIT_LEN;
    robj *key, **subkeys = NULL;
    UNUSED(cmd);

    subkeys = zmalloc(capacity*sizeof(robj*));
    getKeyRequestsPrepareResult(result,result->num+1);

    key = argv[key_index];
    incrRefCount(key);

    if (last_subkey < 0) last_subkey += argc;
    for (i = first_subkey; i <= last_subkey; i += subkey_step) {
        robj *subkey = argv[i];
        if (num >= capacity) {
            if (capacity < GETKEYS_RESULT_SUBKEYS_LINER_LEN)
                capacity *= 2;
            else
                capacity += GETKEYS_RESULT_SUBKEYS_LINER_LEN;

            subkeys = zrealloc(subkeys, capacity*sizeof(robj*));
        }
        incrRefCount(subkey);
        subkeys[num++] = subkey;
    }
    getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,key,num,subkeys,
            cmd->intention,cmd->intention_flags,cmd->flags, dbid);

    return 0;
}

int getKeyRequestsHset(int dbid,struct redisCommand *cmd, robj **argv, int argc,
        struct getKeyRequestsResult *result) {
    return getKeyRequestsSingleKeyWithSubkeys(dbid,cmd,argv,argc,result,1,2,-1,2);
}

int getKeyRequestsHmget(int dbid, struct redisCommand *cmd, robj **argv, int argc,
        struct getKeyRequestsResult *result) {
    return getKeyRequestsSingleKeyWithSubkeys(dbid,cmd,argv,argc,result,1,2,-1,1);
}

int getKeyRequestSmembers(int dbid, struct redisCommand *cmd, robj **argv, int argc,
                          struct getKeyRequestsResult *result) {
    return getKeyRequestsSingleKeyWithSubkeys(dbid,cmd,argv,argc,result,1,2,-1,1);
}

int getKeyRequestSmove(int dbid, struct redisCommand *cmd, robj **argv, int argc,
                       struct getKeyRequestsResult *result) {
    robj** subkeys;

    UNUSED(argc), UNUSED(cmd);

    getKeyRequestsPrepareResult(result, result->num + 2);

    incrRefCount(argv[1]);
    incrRefCount(argv[3]);
    subkeys = zmalloc(sizeof(robj*));
    subkeys[0] = argv[3];
    getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,argv[1], 1, subkeys,
                               SWAP_IN, SWAP_IN_DEL, cmd->flags, dbid);

    incrRefCount(argv[2]);
    incrRefCount(argv[3]);
    subkeys = zmalloc(sizeof(robj*));
    subkeys[0] = argv[3];
    getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,argv[2], 1, subkeys,
                               SWAP_IN, 0, cmd->flags, dbid);

    return 0;
}

int getKeyRequestsSinterstore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsOneDestKeyMultiSrcKeys(dbid, cmd, argv, argc, result, 1, 2, -1);
}

/* Key */
void getKeyRequestsSingleKey(getKeyRequestsResult *result,
        robj *key/*ref*/, int cmd_intention, int cmd_intention_flags, uint64_t cmd_flags, int dbid) {
    keyRequest *key_request;
    incrRefCount(key);
    key_request = getKeyRequestsAppendCommonResult(result,
            REQUEST_LEVEL_KEY,key,cmd_intention,cmd_intention_flags,cmd_flags, dbid);
    key_request->type = KEYREQUEST_TYPE_KEY;
}

/* Segment */
void getKeyRequestsAppendRangeResult(getKeyRequestsResult *result, int level,
        robj *key, int arg_rewrite0, int arg_rewrite1, int num_ranges,
        range *ranges, int cmd_intention, int cmd_intention_flags, uint64_t cmd_flags, int dbid) {
    keyRequest *key_request = getKeyRequestsAppendCommonResult(result,level,
            key,cmd_intention,cmd_intention_flags,cmd_flags, dbid);

    key_request->type = KEYREQUEST_TYPE_RANGE;
    key_request->l.num_ranges = num_ranges;
    key_request->l.ranges = ranges;
    key_request->arg_rewrite[0].arg_idx = arg_rewrite0;
    key_request->arg_rewrite[1].arg_idx = arg_rewrite1;
}

/* There are no command with more than 2 ranges request. */
#define GETKEYS_RESULT_SEGMENTS_MAX_LEN 2
int _getKeyRequestsSingleKeyWithRangesGeneric(int dbid, int intention, int intention_flags, uint64_t cmd_flags,
            robj *key, struct getKeyRequestsResult *result, int arg_rewrite0,
            int arg_rewrite1, int num_ranges, va_list ap) {

    int i, capacity = GETKEYS_RESULT_SEGMENTS_MAX_LEN;
    range *ranges = NULL;

    serverAssert(capacity >= num_ranges);
    ranges = zmalloc(capacity*sizeof(range));
    getKeyRequestsPrepareResult(result,result->num+1);

    incrRefCount(key);

    for (i = 0; i < num_ranges; i++) {
        long start = va_arg(ap,long);
        long end = va_arg(ap,long);
        int reverse = va_arg(ap,int);
        ranges[i].start = start;
        ranges[i].end = end;
        ranges[i].reverse = reverse;
    }

    getKeyRequestsAppendRangeResult(result, REQUEST_LEVEL_KEY,key,
            arg_rewrite0,arg_rewrite1,num_ranges,ranges,intention,
            intention_flags, cmd_flags, dbid);

    return 0;
}

int getKeyRequestsSwapBlockedLmove(int dbid, int intention, int intention_flags, uint64_t cmd_flags,
            robj *key, struct getKeyRequestsResult *result, int arg_rewrite0,
            int arg_rewrite1, int num_ranges, ...) {
    va_list ap;
    va_start(ap, num_ranges);
    int code = _getKeyRequestsSingleKeyWithRangesGeneric(dbid, intention, intention_flags, cmd_flags,
                                                         key, result, arg_rewrite0, arg_rewrite1, num_ranges, ap);
    va_end(ap);
    return code;
}

int getKeyRequestsSingleKeyWithRanges(int dbid, struct redisCommand *cmd, robj **argv,
         int argc, struct getKeyRequestsResult *result, int key_index,
         int arg_rewrite0, int arg_rewrite1, int num_ranges, ...) {
    UNUSED(argc);
    va_list ap;
    va_start(ap, num_ranges);
    int code = _getKeyRequestsSingleKeyWithRangesGeneric(dbid, cmd->intention, cmd->intention_flags, cmd->flags,
        argv[key_index], result, arg_rewrite0, arg_rewrite1, num_ranges, ap);
    va_end(ap);
    return code;
}

int getKeyRequestsSingleKeyWithBitmapOffset(int dbid, struct redisCommand *cmd, robj **argv,
         int argc, struct getKeyRequestsResult *result, int key_index,
         int arg_idx_rewrite0, long offset) {

    UNUSED(argc);
    getKeyRequestsPrepareResult(result,result->num+1);

    robj *key = argv[key_index];
    incrRefCount(key);

    keyRequest *key_request = getKeyRequestsAppendCommonResult(result,REQUEST_LEVEL_KEY,
            key,cmd->intention,cmd->intention_flags,cmd->flags, dbid);

    key_request->type = KEYREQUEST_TYPE_BTIMAP_OFFSET;
    key_request->bo.offset = offset;
    key_request->arg_rewrite[0].arg_idx = arg_idx_rewrite0;

    return 0;
}

int getKeyRequestsSingleKeyWithBitmapRange(int dbid, struct redisCommand *cmd, robj **argv,
         int argc, struct getKeyRequestsResult *result, int key_index,
        long long start, long long end) {

    UNUSED(argc);
    getKeyRequestsPrepareResult(result,result->num+1);

    robj *key = argv[key_index];
    incrRefCount(key);

    keyRequest *key_request = getKeyRequestsAppendCommonResult(result,REQUEST_LEVEL_KEY,
            key,cmd->intention,cmd->intention_flags,cmd->flags, dbid);

    key_request->type = KEYREQUEST_TYPE_BTIMAP_RANGE;
    key_request->br.start = start;
    key_request->br.end = end;

    return 0;
}

int getKeyRequestsLpop(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    long long count = 1, value;

    if (argc >= 3) {
        if (getLongLongFromObject(argv[2],&value) == C_OK)
            count = value;
    }

    getKeyRequestsSingleKeyWithRanges(dbid,cmd,argv,argc,
            result,1,-1,-1,1/*num_ranges*/,0L,(long)count,(int)0);
    return 0;

}

int getKeyRequestsBlpop(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    for (int i = 1; i < argc-1; i++) {
        getKeyRequestsSingleKeyWithRanges(dbid,cmd,argv,argc,
                result,i,-1,-1,1/*num_ranges*/,0L,0L,(int)0);
    }
    return 0;
}

int getKeyRequestsRpop(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    long long count = 1, value;

    if (argc >= 3) {
        if (getLongLongFromObject(argv[2],&value) == C_OK)
            count = value;
    }

    getKeyRequestsSingleKeyWithRanges(dbid,cmd,argv,argc,
            result,1,-1,-1,1/*num_ranges*/,(long)-count,-1L,(int)0);
    return 0;
}

int getKeyRequestsBrpop(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    for (int i = 1; i < argc-1; i++) {
        getKeyRequestsSingleKeyWithRanges(dbid,cmd,argv,argc,
                result,i,-1,-1,1/*num_ranges*/,-1L,-1L,(int)0);
    }
    return 0;
}

int getKeyRequestsRpoplpush(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    getKeyRequestsSingleKeyWithRanges(dbid,cmd,argv,argc,
            result,1,-1,-1,1/*num_ranges*/,-1L,-1L,(int)0); /* source */
    getKeyRequestsSingleKey(result,argv[2],SWAP_IN,SWAP_IN_META,cmd->flags | CMD_SWAP_DATATYPE_KEYSPACE,dbid);
    return 0;
}

int getKeyRequestsLmove(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    long start, end;
    if ((argc != 5/*lmove*/ && argc != 6/*blmove*/) ||
        (strcasecmp(argv[3]->ptr,"left") && strcasecmp(argv[3]->ptr,"right")) ||
        (strcasecmp(argv[4]->ptr,"left") && strcasecmp(argv[4]->ptr,"right"))) {
        return -1;
    }

    if (!strcasecmp(argv[3]->ptr,"left")) {
        start = 0, end = 0;
    } else {
        start = -1, end = -1;
    }
    /* source */
    getKeyRequestsSingleKeyWithRanges(dbid,cmd,argv,argc,
            result,1,-1,-1,1/*num_ranges*/,start,end,(int)0);
    /* destination */
    getKeyRequestsSingleKey(result,argv[2],SWAP_IN,SWAP_IN_META,cmd->flags,dbid);
    return 0;
}

int getKeyRequestsLindex(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    long long index;
    if (getLongLongFromObject(argv[2],&index) != C_OK) return -1;
    getKeyRequestsSingleKeyWithRanges(dbid,cmd,argv,argc,
            result,1,2,-1,1/*num_ranges*/,(long)index,(long)index,(int)0);
    return 0;
}

int getKeyRequestsLrange(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    long long start, end;
    if (getLongLongFromObject(argv[2],&start) != C_OK) return -1;
    if (getLongLongFromObject(argv[3],&end) != C_OK) return -1;
    getKeyRequestsSingleKeyWithRanges(dbid,cmd,argv,argc,
            result,1,2,3,1/*num_ranges*/,(long)start,(long)end,(int)0);
    return 0;
}

int getKeyRequestsLtrim(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    long long start, stop;
    if (getLongLongFromObject(argv[2],&start) != C_OK) return -1;
    if (getLongLongFromObject(argv[3],&stop) != C_OK) return -1;
    getKeyRequestsSingleKeyWithRanges(dbid,cmd,argv,argc,
            result,1,2,3,1/*num_ranges*/,(long)start,(long)stop,(int)1/*reverse*/);
    return 0;
}
/** zset **/
int getKeyRequestsZAdd(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    int first_score = 2;
    while(first_score < argc) {
        char *opt = argv[first_score]->ptr;
        if (
            strcasecmp(opt,"nx") != 0 &&
            strcasecmp(opt,"xx") != 0 &&
            strcasecmp(opt,"ch") != 0 &&
            strcasecmp(opt,"incr") != 0 &&
            strcasecmp(opt,"gt") != 0 &&
            strcasecmp(opt,"lt") != 0
        ) {
            break;
        }
        first_score++;
    }
    return getKeyRequestsSingleKeyWithSubkeys(dbid, cmd, argv, argc, result, 1, first_score + 1, -1, 2);
}

int getKeyRequestsZScore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsSingleKeyWithSubkeys(dbid, cmd,argv,argc,result,1,2,-1,1);
}

int getKeyRequestsZincrby(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsSingleKeyWithSubkeys(dbid, cmd, argv, argc, result, 1, 3, -1, 2);
}

int getKeyRequestsZMScore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsSingleKeyWithSubkeys(dbid, cmd, argv, argc, result, 1, 2, -1, 1);
}

#define ZMIN -1
#define ZMAX 1
int getKeyRequestsZpopGeneric(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result, int flags) {
    UNUSED(cmd), UNUSED(flags);
    getKeyRequestsPrepareResult(result,result->num+ argc - 2);
    for(int i = 1; i < argc - 1; i++) {
        incrRefCount(argv[i]);
        getKeyRequestsAppendSubkeyResult(result, REQUEST_LEVEL_KEY, argv[i], 0, NULL, cmd->intention,
            cmd->intention_flags, cmd->flags, dbid);
    }
    return C_OK;
}

int getKeyRequestsZpopMin(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZpopGeneric(dbid, cmd, argv, argc, result, ZMIN);
}

int getKeyRequestsZpopMax(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZpopGeneric(dbid, cmd, argv, argc, result, ZMAX);
}

int getKeyRequestsZrangestore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsOneDestKeyMultiSrcKeys(dbid, cmd, argv, argc, result, 1, 2, 2);
}


typedef enum {
    ZRANGE_DIRECTION_AUTO = 0,
    ZRANGE_DIRECTION_FORWARD,
    ZRANGE_DIRECTION_REVERSE
} zrange_direction;
typedef enum {
    ZRANGE_AUTO = 0,
    ZRANGE_RANK,
    ZRANGE_SCORE,
    ZRANGE_LEX,
} zrange_type;

int zslParseRange(robj *min, robj *max, zrangespec *spec);
int getKeyRequestsZrangeGeneric(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result, zrange_type rangetype, zrange_direction direction) {
    if (argc < 4) return C_ERR;
    robj *minobj ,*maxobj;
    int argc_start = 1;
    long long opt_offset = 0, opt_limit = 0;
    /* Step 1: Skip the <src> <min> <max> args and parse remaining optional arguments. */
    for (int j=argc_start + 3; j < argc; j++) {
        int leftargs = argc-j-1;
        if (!strcasecmp(argv[j]->ptr,"withscores")) {
            /* opt_withscores = 1; */
        } else if (!strcasecmp(argv[j]->ptr,"limit") && leftargs >= 2) {

            if (getLongLongFromObject(argv[j+1], &opt_offset) != C_OK
            || getLongLongFromObject(argv[j+2], &opt_limit) != C_OK) {
                return C_ERR;
            }
            j += 2;
        } else if (direction == ZRANGE_DIRECTION_AUTO &&
                   !strcasecmp(argv[j]->ptr,"rev"))
        {
            direction = ZRANGE_DIRECTION_REVERSE;
        } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(argv[j]->ptr,"bylex"))
        {
            rangetype = ZRANGE_LEX;
        } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(argv[j]->ptr,"byscore"))
        {
            rangetype = ZRANGE_SCORE;
        } else {

            return C_ERR;
        }
    }
    if (direction == ZRANGE_DIRECTION_REVERSE) {
        minobj = argv[3];
        maxobj = argv[2];
    } else {
        minobj = argv[2];
        maxobj = argv[3];
    }
    robj* key = argv[1];
    incrRefCount(key);

    getKeyRequestsPrepareResult(result,result->num+ 1);

    switch (rangetype)
    {
    case ZRANGE_SCORE:
        {
            zrangespec* spec = zmalloc(sizeof(zrangespec));
            /* code */
            if (zslParseRange(minobj, maxobj, spec) != C_OK) {
                decrRefCount(key);
                zfree(spec);
                return C_ERR;
            }
            getKeyRequestsAppendScoreResult(result, REQUEST_LEVEL_KEY, key, direction == ZRANGE_DIRECTION_REVERSE, spec, opt_offset + opt_limit,cmd->intention, cmd->intention_flags, cmd->flags, dbid);
        }
        break;
    default:
        getKeyRequestsAppendSubkeyResult(result, REQUEST_LEVEL_KEY, key, 0, NULL, cmd->intention, cmd->intention_flags, cmd->flags, dbid);
        break;
    }

    return C_OK;
}

int getKeyRequestsZrange(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZrangeGeneric(dbid, cmd, argv, argc, result, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
}

int getKeyRequestsZrangeByScore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZrangeGeneric(dbid, cmd, argv, argc, result, ZRANGE_SCORE, ZRANGE_DIRECTION_FORWARD);
}

int getKeyRequestsZrevrangeByScore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZrangeGeneric(dbid, cmd, argv, argc, result, ZRANGE_SCORE, ZRANGE_DIRECTION_REVERSE);
}


int getKeyRequestsZrevrangeByLex(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZrangeGeneric(dbid, cmd, argv, argc, result, ZRANGE_LEX, ZRANGE_DIRECTION_REVERSE);
}

int getKeyRequestsZrangeByLex(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZrangeGeneric(dbid, cmd, argv, argc, result, ZRANGE_LEX, ZRANGE_DIRECTION_FORWARD);
}

int getKeyRequestsZlexCount(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZrangeGeneric(dbid, cmd, argv, argc, result, ZRANGE_LEX, ZRANGE_DIRECTION_FORWARD);
}

int getKeyRequestsZremRangeByLex(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsZrangeGeneric(dbid, cmd, argv, argc, result, ZRANGE_LEX, ZRANGE_DIRECTION_FORWARD);
}
/** geo **/
int getKeyRequestsGeoAdd(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    int first_score = 2;
    while(first_score < argc) {
        char *opt = argv[first_score]->ptr;
        if (
            strcasecmp(opt,"nx") != 0 &&
            strcasecmp(opt,"xx") != 0 &&
            strcasecmp(opt,"ch") != 0
        ) {
            break;
        }
        first_score++;
    }
    return getKeyRequestsSingleKeyWithSubkeys(dbid, cmd, argv, argc, result, 1, first_score + 2, -1, 3);
}

int getKeyRequestsGeoDist(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsSingleKeyWithSubkeys(dbid, cmd, argv, argc, result, 1, 2, -2, 1);
}

int getKeyRequestsGeoHash(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsSingleKeyWithSubkeys(dbid, cmd, argv, argc, result, 1, 2, -1, 1);
}

int getKeyRequestsGeoRadius(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    int storekeyIndex = -1;
    for(int i =0; i < argc; i++) {
        if (!strcasecmp(argv[i]->ptr, "store") && (i+1) < argc) {
            storekeyIndex = i+1;
            i++;
        } else if(!strcasecmp(argv[i]->ptr, "storedist") && (i+1) < argc) {
            storekeyIndex = i+1;
            i++;
        }
    }
    return getKeyRequestsOneDestKeyMultiSrcKeys(dbid, cmd, argv, argc, result, storekeyIndex, 1, 1);
}

int getKeyRequestsGeoSearchStore(int dbid, struct redisCommand *cmd, robj **argv, int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsOneDestKeyMultiSrcKeys(dbid, cmd, argv, argc, result, 1, 2, 2);
}

static inline void getKeyRequestsGtidArgRewriteAdjust(
        struct getKeyRequestsResult *result, int orig_krs_num, int start_index) {
    for (int i = orig_krs_num; i < result->num; i++) {
        keyRequest *kr = result->key_requests+i;
        if (kr->arg_rewrite[0].arg_idx > 0) kr->arg_rewrite[0].arg_idx += start_index;
        if (kr->arg_rewrite[1].arg_idx > 0) kr->arg_rewrite[1].arg_idx += start_index;
    }
}

int getKeyRequestsGtid(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    int start_index, exec_dbid, orig_num;
    struct redisCommand* exec_cmd;
    long long value;

    UNUSED(dbid), UNUSED(cmd);

    if (getLongLongFromObject(argv[2],&value)) return C_ERR;
    if (value < 0 || value > server.dbnum)  return C_ERR;
    exec_dbid = (int)value;

    if (strncmp(argv[3]->ptr, "/*", 2))
        start_index = 3;
    else
        start_index = 4;

    orig_num = result->num;

    exec_cmd = lookupCommandByCString(argv[start_index]->ptr);
    if (_getSingleCmdKeyRequests(exec_dbid,exec_cmd,argv+start_index,
            argc-start_index,result)) return C_ERR;

    getKeyRequestsGtidArgRewriteAdjust(result,orig_num,start_index);
    return C_OK;
}

int getKeyRequestsDebug(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    robj *key;
    if (!strcasecmp(argv[1]->ptr,"reload") ||
            !strcasecmp(argv[1]->ptr,"loadaof") ||
            !strcasecmp(argv[1]->ptr,"digest") ||
            !strcasecmp(argv[1]->ptr,"change-repl-id")) {
        return getKeyRequestsGlobal(dbid,cmd,argv,argc,result);
    } else if (argc == 3 && (!strcasecmp(argv[1]->ptr,"object") ||
                !strcasecmp(argv[1]->ptr, "ziplist") ||
                !strcasecmp(argv[1]->ptr, "sdslen"))) {
        key = argv[2];
        incrRefCount(key);
        getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,key,0,NULL,
                cmd->intention,cmd->intention_flags,cmd->flags,dbid);
        return 0;
    } else if (argc >= 3 && (!strcasecmp(argv[1]->ptr,"mallctl") ||
                !strcasecmp(argv[1]->ptr,"mallctl-str"))) {
        key = argv[2];
        incrRefCount(key);
        getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,key,0,NULL,
                cmd->intention,cmd->intention_flags,cmd->flags,dbid);
        return 0;
    } else if (argc >= 3 && !strcasecmp(argv[1]->ptr,"digest-value")) {
        for (int i = 2; i < argc; i++) {
            key = argv[i];
            incrRefCount(key);
            getKeyRequestsAppendSubkeyResult(result,REQUEST_LEVEL_KEY,key,0,NULL,
                    cmd->intention,cmd->intention_flags,cmd->flags, dbid);
        }
        return 0;
    } else {
        return getKeyRequestsNone(dbid,cmd,argv,argc,result);
    }
}

int getKeyRequestsSetbit(int dbid, struct redisCommand *cmd, robj **argv,
                         int argc, struct getKeyRequestsResult *result) {
    long long offset;
    if (getLongLongFromObject(argv[2],&offset) != C_OK) return -1;
    getKeyRequestsSingleKeyWithBitmapOffset(dbid,cmd,argv,argc,
                                      result,1,2,
                                      offset);
    return 0;
}

int getKeyRequestsGetbit(int dbid, struct redisCommand *cmd, robj **argv,
                         int argc, struct getKeyRequestsResult *result) {
    long long offset;
    if (getLongLongFromObject(argv[2],&offset) != C_OK) return -1;
    getKeyRequestsSingleKeyWithBitmapOffset(dbid,cmd,argv,argc,
            result,1,2,offset);
    return 0;
}

int getKeyRequestsBitcount(int dbid, struct redisCommand *cmd, robj **argv,
                         int argc, struct getKeyRequestsResult *result) {
    long long start, end;

    if (argc < 4) {
        /* BITCOUNT key [start end], both start and end may not exist. */
        getKeyRequestsSingleKey(result,argv[1],SWAP_IN,0,cmd->flags,dbid);
    } else {
        if (getLongLongFromObject(argv[2],&start) != C_OK) return -1;
        if (getLongLongFromObject(argv[3],&end) != C_OK) return -1;
        getKeyRequestsSingleKeyWithBitmapRange(dbid,cmd,argv,argc,
                result,1,start,end);
    }
    return 0;
}

int getKeyRequestsBitpos(int dbid, struct redisCommand *cmd, robj **argv,
                         int argc, struct getKeyRequestsResult *result) {
    long long start, end;
    /* BITPOS key bit [start [end] ], start or end may not exist.  */
    if (argc <= 3) {
        getKeyRequestsSingleKey(result,argv[1],SWAP_IN,0,cmd->flags,dbid);
    } else if (argc == 4) {
        if (getLongLongFromObject(argv[3],&start) != C_OK) return -1;

        /* max size of bitmap is 512MB, last possible bit (equal to 2^32 - 1, UINT_MAX),
         * start and end specify a byte index, UINT_MAX could cover the range. */
        getKeyRequestsSingleKeyWithBitmapRange(dbid,cmd,argv,argc,
                result,1,start,UINT_MAX);
    } else {
        if (getLongLongFromObject(argv[3],&start) != C_OK) return -1;
        if (getLongLongFromObject(argv[4],&end) != C_OK) return -1;
        getKeyRequestsSingleKeyWithBitmapRange(dbid,cmd,argv,argc,
                result,1,start,end);
    }
    return 0;
}

int getKeyRequestsBitop(int dbid, struct redisCommand *cmd, robj **argv,
                        int argc, struct getKeyRequestsResult *result) {
    return getKeyRequestsOneDestKeyMultiSrcKeys(dbid, cmd, argv, argc, result, 2, 3, -1);
}

int getKeyRequestsBitField(int dbid, struct redisCommand *cmd, robj **argv,
                         int argc, struct getKeyRequestsResult *result) {

    UNUSED(argc);
    getKeyRequestsSingleKey(result,argv[1],cmd->intention,cmd->intention_flags,cmd->flags,dbid);
    return 0;
}


#define GET_KEYREQUESTS_MEMORY_MUL 4

int getKeyRequestsMemory(int dbid, struct redisCommand *cmd, robj **argv,
        int argc, struct getKeyRequestsResult *result) {
    if (!strcasecmp(argv[1]->ptr,"usage")) {
        robj *key;
        int count = 5; /* OBJ_COMPUTE_SIZE_DEF_SAMPLES */
        long long value;

        if (argc >= 5 && !strcasecmp(argv[3]->ptr,"samples") &&
                getLongLongFromObject(argv[4],&value) == C_OK) {
            count = value;
        }

        if (argc > 2) {
            key = argv[2];
            incrRefCount(key);
            getKeyRequestsAppendSampleResult(result,REQUEST_LEVEL_KEY,key,
                    count*GET_KEYREQUESTS_MEMORY_MUL,
                    cmd->intention,cmd->intention_flags,cmd->flags,dbid);
        }
        return 0;
    } else {
        return getKeyRequestsNone(dbid,cmd,argv,argc,result);
    }
}

/* The swap.info command, propagate system info to slave.
 * SWAP.INFO <subcommand> [<arg> [value] [opt] ...]
 *
 * subcommand supported:
 * SWAP.INFO SST-AGE-LIMIT <sst age limit> */
void swapInfoCommand(client *c) {
    if (c->argc < 2) {
        addReply(c,shared.ok);
        return;
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
            "SST-AGE-LIMIT <sst age limit>",
            "    Set sst age limit to launch ttl compact for aged sst files.",
            NULL};
        addReplyHelp(c, help);
        return;
    } else {
        sds *swap_info_argv = (sds*)zmalloc(sizeof(sds)*c->argc);
        for (int i = 0; i < c->argc; i++) {
            swap_info_argv[i] = c->argv[i]->ptr;
        }
        swapApplySwapInfo(c->argc, swap_info_argv);
        zfree(swap_info_argv);
    }

    addReply(c,shared.ok);
}

#ifdef REDIS_TEST

void rewriteResetClientCommandCString(client *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = zmalloc(sizeof(robj*)*argc);
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        char *a = va_arg(ap, char*);
        argv[j] = createStringObject(a, strlen(a));
    }
    replaceClientCommandVector(c, argc, argv);
    va_end(ap);
}

void initServerConfig(void);
int swapCmdTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    int error = 0;
    client *c;

    TEST("cmd: init") {
        initServerConfig();
        ACLInit();
        server.hz = 10;
        c = createClient(NULL);
        initTestRedisDb();
        selectDb(c,0);
    }

    TEST("cmd: no key") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        rewriteResetClientCommandCString(c,1,"PING");
        getKeyRequests(c,&result);
        test_assert(result.num == 0);
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
    }

    TEST("cmd: single key") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        rewriteResetClientCommandCString(c,2,"GET","KEY");
        getKeyRequests(c,&result);
        test_assert(result.num == 1);
        test_assert(!strcmp(result.key_requests[0].key->ptr, "KEY"));
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
    }

    TEST("cmd: multiple keys") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        rewriteResetClientCommandCString(c,3,"MGET","KEY1","KEY2");
        getKeyRequests(c,&result);
        test_assert(result.num == 2);
        test_assert(!strcmp(result.key_requests[0].key->ptr, "KEY1"));
        test_assert(!strcmp(result.key_requests[1].key->ptr, "KEY2"));
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
    }

    TEST("cmd: multi/exec") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        c->flags |= CLIENT_MULTI;
        rewriteResetClientCommandCString(c,1,"PING");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,3,"MGET","KEY1","KEY2");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,3,"SET","KEY3","VAL3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,1,"EXEC");
        getKeyRequests(c,&result);
        test_assert(result.num == 3);
        test_assert(!strcmp(result.key_requests[0].key->ptr, "KEY1"));
        test_assert(result.key_requests[0].b.subkeys == NULL);
        test_assert(!strcmp(result.key_requests[1].key->ptr, "KEY2"));
        test_assert(result.key_requests[1].b.subkeys == NULL);
        test_assert(!strcmp(result.key_requests[2].key->ptr, "KEY3"));
        test_assert(result.key_requests[2].b.subkeys == NULL);
        test_assert(result.key_requests[2].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[2].cmd_intention_flags == SWAP_IN_OVERWRITE);
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
        discardTransaction(c);
    }

    TEST("cmd: hash subkeys") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        rewriteResetClientCommandCString(c,5,"HMGET","KEY","F1","F2","F3");
        getKeyRequests(c,&result);
        test_assert(result.num == 1);
        test_assert(!strcmp(result.key_requests[0].key->ptr, "KEY"));
        test_assert(result.key_requests[0].b.num_subkeys == 3);
        test_assert(!strcmp(result.key_requests[0].b.subkeys[0]->ptr, "F1"));
        test_assert(!strcmp(result.key_requests[0].b.subkeys[1]->ptr, "F2"));
        test_assert(!strcmp(result.key_requests[0].b.subkeys[2]->ptr, "F3"));
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
    }

    TEST("cmd: multi/exec hash subkeys") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        c->flags |= CLIENT_MULTI;
        rewriteResetClientCommandCString(c,1,"PING");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,3,"MGET","KEY1","KEY2");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,5,"HMGET","HASH","F1","F2","F3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,1,"EXEC");
        getKeyRequests(c,&result);
        test_assert(result.num == 3);
        test_assert(!strcmp(result.key_requests[0].key->ptr, "KEY1"));
        test_assert(result.key_requests[0].b.subkeys == NULL);
        test_assert(!strcmp(result.key_requests[1].key->ptr, "KEY2"));
        test_assert(result.key_requests[1].b.subkeys == NULL);
        test_assert(!strcmp(result.key_requests[2].key->ptr, "HASH"));
        test_assert(result.key_requests[2].b.num_subkeys == 3);
        test_assert(!strcmp(result.key_requests[2].b.subkeys[0]->ptr, "F1"));
        test_assert(!strcmp(result.key_requests[2].b.subkeys[1]->ptr, "F2"));
        test_assert(!strcmp(result.key_requests[2].b.subkeys[2]->ptr, "F3"));
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
        discardTransaction(c);
    }

    TEST("cmd: dispatch swap sequentially for reentrant-key request") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        rewriteResetClientCommandCString(c,4,"MGET", "K1", "K2", "K1");
        getKeyRequests(c,&result);
        test_assert(result.num == 3);
        test_assert(!strcmp(result.key_requests[0].key->ptr, "K1"));
        test_assert(result.key_requests[0].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[0].cmd_intention_flags == 0);
        test_assert(!strcmp(result.key_requests[1].key->ptr, "K2"));
        test_assert(!strcmp(result.key_requests[2].key->ptr, "K1"));
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
    }

    TEST("cmd: dispatch swap sequentially for reentrant-key request (multi)") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        c->flags |= CLIENT_MULTI;
        rewriteResetClientCommandCString(c,3,"HMGET","HASH", "F1");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,2,"DEL","HASH");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,1,"EXEC");
        getKeyRequests(c,&result);
        test_assert(result.num == 2);
        test_assert(!strcmp(result.key_requests[0].key->ptr, "HASH"));
        test_assert(result.key_requests[0].b.num_subkeys == 1);
        test_assert(result.key_requests[0].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[0].cmd_intention_flags == 0);
        test_assert(!strcmp(result.key_requests[1].key->ptr, "HASH"));
        test_assert(result.key_requests[1].b.subkeys == NULL);
        test_assert(result.key_requests[1].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[1].cmd_intention_flags == SWAP_IN_DEL_MOCK_VALUE);
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
        discardTransaction(c);
    }

    TEST("cmd: dispatch swap sequentially with db/svr request") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        c->flags |= CLIENT_MULTI;
        rewriteResetClientCommandCString(c,1,"PING");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,1,"FLUSHDB");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,1,"EXEC");
        getKeyRequests(c,&result);
        test_assert(result.num == 1);
        test_assert(result.key_requests[0].key == NULL);
        test_assert(result.key_requests[0].cmd_intention == SWAP_NOP);
        test_assert(result.key_requests[0].cmd_intention_flags == 0);
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
        discardTransaction(c);
    }

    TEST("cmd: dbid, cmd_intention, cmd_intention_flags set properly") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        selectDb(c,1);
        c->flags |= CLIENT_MULTI;
        rewriteResetClientCommandCString(c,1,"PING");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,3,"MGET","KEY1","KEY2");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,5,"HDEL","HASH","F1","F2","F3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,1,"FLUSHDB");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,1,"EXEC");
        getKeyRequests(c,&result);
        test_assert(result.num == 4);
        test_assert(!strcmp(result.key_requests[0].key->ptr, "KEY1"));
        test_assert(result.key_requests[0].b.subkeys == NULL);
        test_assert(result.key_requests[0].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[0].cmd_intention_flags == 0);
        test_assert(result.key_requests[0].dbid == 1);
        test_assert(!strcmp(result.key_requests[1].key->ptr, "KEY2"));
        test_assert(result.key_requests[1].b.subkeys == NULL);
        test_assert(result.key_requests[1].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[1].cmd_intention_flags == 0);
        test_assert(result.key_requests[1].dbid == 1);
        test_assert(!strcmp(result.key_requests[2].key->ptr, "HASH"));
        test_assert(!strcmp(result.key_requests[2].b.subkeys[0]->ptr, "F1"));
        test_assert(!strcmp(result.key_requests[2].b.subkeys[1]->ptr, "F2"));
        test_assert(!strcmp(result.key_requests[2].b.subkeys[2]->ptr, "F3"));
        test_assert(result.key_requests[2].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[2].cmd_intention_flags == SWAP_IN_DEL);
        test_assert(result.key_requests[2].dbid == 1);
        test_assert(result.key_requests[3].key == NULL);
        test_assert(result.key_requests[3].cmd_intention == SWAP_NOP);
        test_assert(result.key_requests[3].cmd_intention_flags == 0);
        test_assert(result.key_requests[3].dbid == 1);
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
        discardTransaction(c);
    }

    TEST("cmd: encode/decode Scorekey") {

    }

    TEST("cmd: gtid") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        selectDb(c,1);
        c->flags |= CLIENT_MULTI;
        rewriteResetClientCommandCString(c,4,"GTID","A:1","1","PING");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,3,"MGET","KEY1","KEY2");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,3,"LINDEX","LIST","3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,6,"GTID","A:2","2","MGET","KEY1","KEY2");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,8,"GTID","A:3","3","HDEL","HASH","F1","F2","F3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,6,"GTID","A:4","4","LINDEX","LIST","3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,4,"GTID","A:5","5","FLUSHDB");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,3,"LINDEX","LIST","3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,4,"GTID","A:10","10","EXEC");

        getKeyRequests(c,&result);

        test_assert(result.num == 9);
        test_assert(c->db->id == 1);

        test_assert(!strcmp(result.key_requests[0].key->ptr, "KEY1"));
        test_assert(result.key_requests[0].b.subkeys == NULL);
        test_assert(result.key_requests[0].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[0].cmd_intention_flags == 0);
        test_assert(result.key_requests[0].dbid == 10);
        test_assert(!strcmp(result.key_requests[1].key->ptr, "KEY2"));
        test_assert(result.key_requests[1].b.subkeys == NULL);
        test_assert(result.key_requests[1].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[1].cmd_intention_flags == 0);
        test_assert(result.key_requests[1].dbid == 10);

        test_assert(!strcmp(result.key_requests[2].key->ptr, "LIST"));
        test_assert(result.key_requests[2].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[2].cmd_intention_flags == 0);
        test_assert(result.key_requests[2].dbid == 10);
        test_assert(result.key_requests[2].l.num_ranges == 1);
        test_assert(result.key_requests[2].l.ranges[0].start == 3 && result.key_requests[2].l.ranges[0].end == 3);
        test_assert(result.key_requests[2].arg_rewrite[0].mstate_idx == 2);
        test_assert(result.key_requests[2].arg_rewrite[0].arg_idx == 2);

        test_assert(!strcmp(result.key_requests[3].key->ptr, "KEY1"));
        test_assert(result.key_requests[3].b.subkeys == NULL);
        test_assert(result.key_requests[3].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[3].cmd_intention_flags == 0);
        test_assert(result.key_requests[3].dbid == 2);
        test_assert(!strcmp(result.key_requests[4].key->ptr, "KEY2"));
        test_assert(result.key_requests[4].b.subkeys == NULL);
        test_assert(result.key_requests[4].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[4].cmd_intention_flags == 0);
        test_assert(result.key_requests[4].dbid == 2);

        test_assert(!strcmp(result.key_requests[5].key->ptr, "HASH"));
        test_assert(!strcmp(result.key_requests[5].b.subkeys[0]->ptr, "F1"));
        test_assert(!strcmp(result.key_requests[5].b.subkeys[1]->ptr, "F2"));
        test_assert(!strcmp(result.key_requests[5].b.subkeys[2]->ptr, "F3"));
        test_assert(result.key_requests[5].dbid == 3);

        test_assert(!strcmp(result.key_requests[6].key->ptr, "LIST"));
        test_assert(result.key_requests[6].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[6].cmd_intention_flags == 0);
        test_assert(result.key_requests[6].dbid == 4);
        test_assert(result.key_requests[6].l.num_ranges == 1);
        test_assert(result.key_requests[6].l.ranges[0].start == 3 && result.key_requests[2].l.ranges[0].end == 3);
        test_assert(result.key_requests[6].arg_rewrite[0].mstate_idx == 5);
        test_assert(result.key_requests[6].arg_rewrite[0].arg_idx == 5);

        test_assert(result.key_requests[7].dbid == 5);
        test_assert(result.key_requests[7].level == REQUEST_LEVEL_SVR);
        test_assert(result.key_requests[7].key == NULL);

        test_assert(!strcmp(result.key_requests[8].key->ptr, "LIST"));
        test_assert(result.key_requests[8].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[8].cmd_intention_flags == 0);
        test_assert(result.key_requests[8].dbid == 10);
        test_assert(result.key_requests[8].l.num_ranges == 1);
        test_assert(result.key_requests[8].l.ranges[0].start == 3 && result.key_requests[2].l.ranges[0].end == 3);
        test_assert(result.key_requests[8].arg_rewrite[0].mstate_idx == 7);
        test_assert(result.key_requests[8].arg_rewrite[0].arg_idx == 2);

        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
        discardTransaction(c);
    }

    TEST("cmd: select in multi/exec") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        selectDb(c,1);

        c->flags |= CLIENT_MULTI;
        rewriteResetClientCommandCString(c,3,"MGET","KEY1","KEY2");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,6,"GTID","A:2","1","MGET","KEY1","KEY2");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,2,"SELECT","3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,6,"GTID","A:1","2","LINDEX","LIST","3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,5,"HDEL","HASH","F1","F2","F3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,2,"SELECT","4");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,3,"LINDEX","LIST","3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,6,"GTID","A:2","5","MGET","KEY1","KEY2");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,3,"LINDEX","LIST","3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,4,"GTID","A:10","10","EXEC");

        getKeyRequests(c,&result);

        test_assert(result.num == 10);
        test_assert(c->db->id == 1);

        test_assert(!strcmp(result.key_requests[0].key->ptr, "KEY1"));
        test_assert(result.key_requests[0].dbid == 10);
        test_assert(!strcmp(result.key_requests[1].key->ptr, "KEY2"));
        test_assert(result.key_requests[1].dbid == 10);

        test_assert(!strcmp(result.key_requests[2].key->ptr, "KEY1"));
        test_assert(result.key_requests[2].dbid == 1);
        test_assert(!strcmp(result.key_requests[3].key->ptr, "KEY2"));
        test_assert(result.key_requests[3].dbid == 1);

        test_assert(!strcmp(result.key_requests[4].key->ptr, "LIST"));
        test_assert(result.key_requests[4].arg_rewrite[0].mstate_idx == 3);
        test_assert(result.key_requests[4].arg_rewrite[0].arg_idx == 5);
        test_assert(result.key_requests[4].dbid == 2);

        test_assert(!strcmp(result.key_requests[5].key->ptr, "HASH"));
        test_assert(result.key_requests[5].dbid == 3);

        test_assert(!strcmp(result.key_requests[6].key->ptr, "LIST"));
        test_assert(result.key_requests[6].arg_rewrite[0].mstate_idx == 6);
        test_assert(result.key_requests[6].arg_rewrite[0].arg_idx == 2);
        test_assert(result.key_requests[6].dbid == 4);

        test_assert(!strcmp(result.key_requests[7].key->ptr, "KEY1"));
        test_assert(result.key_requests[7].dbid == 5);
        test_assert(!strcmp(result.key_requests[8].key->ptr, "KEY2"));
        test_assert(result.key_requests[8].dbid == 5);

        test_assert(!strcmp(result.key_requests[9].key->ptr, "LIST"));
        test_assert(result.key_requests[9].dbid == 4);
        test_assert(result.key_requests[9].arg_rewrite[0].mstate_idx == 8);
        test_assert(result.key_requests[9].arg_rewrite[0].arg_idx == 2);

        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
        discardTransaction(c);
    }

    TEST("cmd: debug") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        selectDb(c,0);

        c->flags |= CLIENT_MULTI;
        rewriteResetClientCommandCString(c,3,"DEBUG","OBJECT","KEY1");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,2,"DEBUG","RELOAD");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,2,"DEBUG","DIGEST");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,4,"DEBUG","DIGEST-VALUE","KEY2","KEY3");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,3,"DEBUG","SLEEP","5");
        queueMultiCommand(c);
        rewriteResetClientCommandCString(c,1,"EXEC");

        getKeyRequests(c,&result);

        test_assert(result.num == 5);
        test_assert(!strcmp(result.key_requests[0].key->ptr,"KEY1"));
        test_assert(result.key_requests[1].level == REQUEST_LEVEL_SVR && result.key_requests[1].key == NULL);
        test_assert(result.key_requests[2].level == REQUEST_LEVEL_SVR && result.key_requests[2].key == NULL);
        test_assert(!strcmp(result.key_requests[3].key->ptr,"KEY2"));
        test_assert(result.key_requests[3].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[3].cmd_intention_flags == 0);
        test_assert(!strcmp(result.key_requests[4].key->ptr,"KEY3"));
        test_assert(result.key_requests[4].cmd_intention == SWAP_IN);
        test_assert(result.key_requests[4].cmd_intention_flags == 0);

        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
        discardTransaction(c);
    }

    TEST("cmd: memory") {
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        rewriteResetClientCommandCString(c,5,"MEMORY","USAGE","KEY","SAMPLES","10");
        getKeyRequests(c,&result);
        test_assert(result.num == 1);
        test_assert(result.key_requests[0].type == KEYREQUEST_TYPE_SAMPLE);
        test_assert(!strcmp(result.key_requests[0].key->ptr, "KEY"));
        test_assert(result.key_requests[0].sp.count == 40);

        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
    }

    TEST("cmd: ltrim") {
        range *r;
        getKeyRequestsResult result = GET_KEYREQUESTS_RESULT_INIT;
        rewriteResetClientCommandCString(c,4,"LTRIM","KEY","1","3");
        getKeyRequests(c,&result);
        test_assert(result.num == 1);
        test_assert(result.key_requests[0].type == KEYREQUEST_TYPE_RANGE);
        test_assert(!strcmp(result.key_requests[0].key->ptr, "KEY"));
        test_assert(result.key_requests[0].l.num_ranges == 1);
        r = result.key_requests[0].l.ranges;
        test_assert(r->start == 1 && r->end == 3 && r->reverse == 1);
        releaseKeyRequests(&result);
        getKeyRequestsFreeResult(&result);
    }

    return error;
}

#endif

