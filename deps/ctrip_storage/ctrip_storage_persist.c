#include "ctrip_storage_persist.h"
#include "ctrip_storage_request.h"
static void swapPersistStatInit(swapPersistStat *stat) {
    stat->add_succ = 0;
    stat->add_ignored = 0;
    stat->started = 0;
    stat->rewind_dirty = 0;
    stat->rewind_newer = 0;
    stat->ended = 0;
    stat->dont_keep = 0;
    stat->keep_data = 0;
}

void persistingKeyEntryFree(dict *privdata, void *val) {
    UNUSED(privdata);
    if (val != NULL) zfree(val);
}

dictType persistingKeysDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    persistingKeyEntryFree,    /* val destructor */
    NULL                       /* allow to expand */
};

persistingKeys *persistingKeysNew() {
    persistingKeys *keys = zmalloc(sizeof(persistingKeys));
    keys->todo = listCreate();
    keys->doing = listCreate();
    keys->map = dictCreate(&persistingKeysDictType);
    return keys;
}

swapPersistCtx *swapPersistCtxNew() {
    int dbnum = server.dbnum;
    swapPersistCtx *ctx = zmalloc(sizeof(swapPersistCtx));
    ctx->keys = zmalloc(dbnum*sizeof(persistingKeys*));
    for (int i = 0; i < dbnum; i++) {
        ctx->keys[i] = persistingKeysNew();
    }
    ctx->version = SWAP_PERSIST_VERSION_INITIAL;
    ctx->inprogress_count = 0;
    ctx->inprogress_limit = 0;
    ctx->keep = 1;
    swapPersistStatInit(&ctx->stat);
    return ctx;
}