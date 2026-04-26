#include "ctrip_storage.h"
#include "ctrip_storage_evict.h"
int getKeyRequestsNone(int dbid, struct redisCommand *cmd, robj **argv, int argc,
        getKeyRequestsResult *result) {
    UNUSED(dbid);
    UNUSED(cmd);
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(result);
    return 0;
}


void ctripStorageCommand(client* c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"EVICT <key> [<key> ...]",
"    Evict key(s) asap.",
"WAIT",
"    Wait for async swap operations to complete.",
"OBJECT <key>",
"    Show info about `key` and assosiated value.",
"ENCODE-META-KEY <key>",
"    Encode meta key.",
"DECODE-META-KEY <rawkey>",
"    Decode meta key.",
"ENCODE-DATA-KEY <key> <version> <subkey>",
"    Encode data key.",
"DECODE-DATA-KEY <rawkey>",
"    Decode data key.",
"RIO-GET meta|data <rawkey> <rawkey> ...",
"    Get raw value from rocksdb.",
"RIO-SCAN meta|data <prefix>",
"    Scan rocksdb with prefix.",
"RIO-ERROR <count> [ACTION name]",
"    Make next count rio return error.",
"RESET-STATS",
"    Reset swap stats.",
"COMPACT",
"   COMPACT rocksdb",
"FLUSH [<cfname,cfname...>]",
"   Flush rocksdb",
"ROCKSDB-PROPERTY-INT <rocksdb-prop-name> [<cfname,cfname...>]",
"    Get rocksdb property value (int type)",
"ROCKSDB-PROPERTY-VALUE <rocksdb-prop-name> [<cfname,cfname...>]",
"    Get rocksdb property value (string type)",
"SCAN-SESSION [<cursor>]",
"    List assigned scan sesions",
NULL
        };
        addReplyHelp(c, help);
    } else if (c->argc > 2 && !strcasecmp(c->argv[1]->ptr,"evict")) {
        ctripStorageEvictCommand(c);
    } else if (c->argc >= 2 && !strcasecmp(c->argv[1]->ptr,"wait")) {
        ctripStorageWaitCommand(c);
    } else {
        addReplyError(c,"Unknown subcommand, try `HELP STORAGE`.");
    }
}