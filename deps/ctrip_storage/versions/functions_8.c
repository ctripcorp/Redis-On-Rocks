#include "functions.h"

int adaptationClientMstateGetArgc(client *c, int idx)
{
    return c->mstate.commands[idx]->argc;
}

struct redisCommand* adaptationClientMstateGetCmd(client *c, int idx)
{
    return c->mstate.commands[idx]->cmd;
}

robj** adaptationClientMstateGetArgv(client *c, int idx)
{
    return c->mstate.commands[idx]->argv;
}

int adaptationIsGtidExecCommand(client *c) {
    return sdscmp(c->cmd->fullname, "gtid") == 0;
}

robj* adaptationRdbLoadObject(int rdbtype, rio *rdb, int *error, int rdbver, void* pd) {
    UNUSED(rdbver);
    UNUSED(pd);
    return rdbLoadObject(rdbtype, rdb, NULL, 0, error);
}