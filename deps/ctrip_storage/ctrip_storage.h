



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
void processReadyDeferredCommands();



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

// int isStorageSPIEnabled();
void storageSPICronLoop();

// int isClientStopNeeded(client *c);
void storageBeforeSleep();
void serverStoragePut();
void serverStorageGet();




/*** 引擎 ***/
/* 内存引擎*/
void* createMemoryStorageEngine();
/* RocksDB引擎*/
void* createRocksDBStorageEngine();
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

/* StorageEvictCtx */

// void StorageEvictCtxStart(size_t mem_used, size_t mem_tofree);
// int StorageEvictShouldStop();
// void StorageEvictSelectedKey(redisDb *db, robj *keyobj, long long *key_mem_freed);
// void StorageEvictCtxEnd();


// int storageActiveExpireTryExpire(redisDb *db, robj *keyobj);


// int storageSlaveExpireCheckColdKey(redisDb *db, int dbid, sds keyname, uint64_t *new_dbids);

#define SWAP_PERSIST_STATE_TODO  0
#define SWAP_PERSIST_STATE_DOING 1
size_t kvobjComputeSize(robj *key, kvobj *o, size_t sample_size, int dbid);

#endif /* __CTRIP_STORAGE_H__ */
