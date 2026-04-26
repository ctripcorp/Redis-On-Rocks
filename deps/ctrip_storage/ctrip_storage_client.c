#include "ctrip_storage_client.h"


/* arg rewrite */
void argRewritesReset(argRewrites *arg_rewrites) {
    memset(arg_rewrites,0,sizeof(argRewrites));
}
argRewrites *argRewritesCreate() {
    argRewrites *arg_rewrites = zmalloc(sizeof(argRewrites));
    argRewritesReset(arg_rewrites);
    return arg_rewrites;
}

void initDeferredCommand(client* c) {
    serverAssert(c->deferred_cmd == NULL);
    c->deferred_cmd = zmalloc(sizeof(deferredCommand));
    c->deferred_cmd->flags = 0;
    c->deferred_cmd->keyrequests_count = 0;
    c->deferred_cmd->swap_cmd = NULL;
    c->deferred_cmd->swap_result = 0;
    c->deferred_cmd->swap_cmd_reploff = -1;
    c->deferred_cmd->swap_repl_client = NULL;
    c->deferred_cmd->swap_lock_mode = SWAP_LOCK_UNIQUE;
    c->deferred_cmd->CLIENT_DEFERED_CLOSING = 0;
    c->deferred_cmd->CLIENT_REPL_SWAPPING = 0;
    c->deferred_cmd->swap_locks = listCreate();
    c->deferred_cmd->swap_metas = NULL;
    c->deferred_cmd->swap_errcode = 0;
    c->deferred_cmd->swap_arg_rewrites = argRewritesCreate();
    c->deferred_cmd->rate_limit_event_id = -1;
    c->deferred_cmd->swap_duration = 0;
}

void resetDeferredCommand(client *c) {
    if (c->deferred_cmd->swap_cmd) {
        swapCmdTraceFree(c->deferred_cmd->swap_cmd);
        c->deferred_cmd->swap_cmd = NULL;
    }
}