



#ifndef __CTRIP_STORAGE_H__
#define __CTRIP_STORAGE_H__
#include "ctrip_storage_request.h"
#include "ctrip_storage_client.h"
#include "ctrip_storage_error.h"
#include "ctrip_storage_lock.h"
#include "ctrip_storage_expire.h"

void rejectCommand(client *c, robj *reply); //in server.c
int dictResizeAllowed(size_t moreMem, double usedRatio); //in server.c
void dictObjectDestructor(dict *d, void *val); //in server.c

void initStorage();
void initStorageDB(redisDb *db);


/*框架 */
typedef enum {
    STORAGE_ACTION_SKIP,      /* C_OK 跳过后续处理，直接返回 */
    STORAGE_ACTION_ERR,     /* C_ERR 出错，返回错误 */
    STORAGE_ACTION_CONTINUE      /* C_OK 继续正常流程 */
} STORAGE_ACTION_TYPE;
int storageCheckProcessCommand(client *c);
int serverStorageBeforeProcessCommand(client *c);
int serverStorageAfterProcessCommand(client *c);

int serverStorageBeforeSleep();
int serverStorageAfterSleep();



void resetStorage();

int isStorageSPIEnabled();
void processReadyDeferredCommands();
void serverStoragePut();
void serverStorageGet();


/*** 引擎 ***/
/* 内存引擎*/
void* initMemoryStorageEngine();
/* RocksDB引擎*/
void* initRocksDBStorageEngine();


/*宏*/
static inline void clientSwapError(client *c, int swap_errcode) {
  if (c && swap_errcode) {
    if (swap_errcode != SWAP_ERR_DATA_WRONG_TYPE_ERROR &&
        swap_errcode != SWAP_ERR_METASCAN_UNSUPPORTED_IN_MULTI) {
        atomicIncr(server.storage.swap_error_count,1);
    }
    c->deferred_cmd->swap_errcode = swap_errcode;
  }
}

#endif /* __CTRIP_STORAGE_H__ */
