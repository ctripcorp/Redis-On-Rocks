
#ifndef __CTRIP_REDIS_VERSION_FUNCTIONS_H__
#define __CTRIP_REDIS_VERSION_FUNCTIONS_H__
#include "server.h"
/********* 适配不同版本的 Redis 内部函数接口 **********/
/*client->mstate 适配*/
int adaptationClientMstateGetArgc(client *c, int idx);
robj** adaptationClientMstateGetArgv(client *c, int idx);
struct redisCommand* adaptationClientMstateGetCmd(client *c, int idx); 

int adaptationIsGtidExecCommand(client *c);

robj* adaptationRdbLoadObject(int rdbtype, rio *rdb, int *error, int rdbver, void* pd);
void zslFreeNode(zskiplist *zsl, zskiplistNode *node);
const void *zslGetNodeElementForDict(const void *node);
void zslUnlinkNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update);

#endif /* __CTRIP_REDIS_VERSION_FUNCTIONS_H__ */