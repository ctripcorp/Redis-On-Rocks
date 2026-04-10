
#ifndef __CTRIP_REDIS_VERSION_FUNCTIONS_H__
#define __CTRIP_REDIS_VERSION_FUNCTIONS_H__
#include "server.h"
/********* 适配不同版本的 Redis 内部函数接口 **********/
/*client->mstate 适配*/
int clientMstateGetArgc(client *c, int idx);
robj** clientMstateGetArgv(client *c, int idx);
struct redisCommand* clientMstateGetCmd(client *c, int idx); 

int isGtidExecCommand(client *c);

#endif /* __CTRIP_REDIS_VERSION_FUNCTIONS_H__ */