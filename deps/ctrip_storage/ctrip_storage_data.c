#include "ctrip_storage_data.h"
#include "ctrip_storage_filter.h"
void swapDataTurnWarmOrHot(swapData *data) {
    if (data->expire != -1) {
        setExpire(NULL,data->db,data->key,data->expire);
    }
    data->db->storage.cold_keys--;
    coldFilterDeleteKey(data->db->storage.cold_filter,data->key->ptr);
}

void swapDataTurnCold(swapData *data) {
    coldFilterAddKey(data->db->storage.cold_filter,data->key->ptr);
    data->db->storage.cold_keys++;
}

void swapDataTurnDeleted(swapData *data, int del_skip) {
    if (swapDataIsCold(data)) {
        data->db->storage.cold_keys--;
        coldFilterDeleteKey(data->db->storage.cold_filter,data->key->ptr);
    } else {
        /* rocks-meta already deleted, only need to delete object_meta
         * from keyspace. */
        if (!del_skip && data->expire != -1) {
            removeExpire(data->db,data->key);
        }
    }
}

/* Main-thread: swap out data out of keyspace. */
inline int swapDataSwapOut(swapData *d, void *datactx, int keep_data, int *totally_out) {
    if (d->type->swapOut)
        return d->type->swapOut(d, datactx, keep_data, totally_out);
    else
        return 0;
}


/* Main-thread: swap del data out of keyspace. */
inline int swapDataSwapDel(swapData *d, void *datactx, int async) {
    if (d->type->swapDel)
        return d->type->swapDel(d, datactx, async);
    else
        return 0;
}

void swapDataMergeAbsentSubkey(swapData *data) {
    swapDataAbsentSubkey *absent = data->absent;
    if (absent == NULL) return;
    for (size_t i = 0; i < absent->count; i++) {
        sds key = data->key->ptr;
        sds subkey = absent->subkeys[i];
        coldFilterSubkeyNotFound(data->db->storage.cold_filter,key,subkey);
    }
}


/* Main-thread: swap in created or merged result into keyspace. */
inline int swapDataSwapIn(swapData *d, void **result, void *datactx) {
    if (d->type->swapIn)
        return d->type->swapIn(d,result,datactx);
    else
        return 0;
}