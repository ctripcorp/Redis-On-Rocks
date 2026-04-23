#include "functions.h"

int clientMstateGetArgc(client *c, int idx)
{
    return c->mstate.commands[idx]->argc;
}

struct redisCommand* clientMstateGetCmd(client *c, int idx)
{
    return c->mstate.commands[idx]->cmd;
}

robj** clientMstateGetArgv(client *c, int idx)
{
    return c->mstate.commands[idx]->argv;
}

int isGtidExecCommand(client *c) {
    return sdscmp(c->cmd->fullname, "gtid") == 0;
}