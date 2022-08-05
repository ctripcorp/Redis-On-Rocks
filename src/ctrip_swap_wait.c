/* Copyright (c) 2021, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "ctrip_swap.h"

int64_t swapTxidInit() {
    return server.swap_txid = 0;
}

int64_t swapTxidNext() {
    return server.swap_txid++;
}

static inline void requestListenersLink(requestListeners *listeners,
        requestListener *listener) {
    while (listeners) {
        listeners->nlistener++;
        if (listeners->cur_txid != listener->txid) {
            listeners->cur_txid = listener->txid;
            listeners->cur_ntxlistener = 0;
        }
        listeners->cur_ntxlistener++;
        listeners = listeners->parent;
    }
}

static inline void requestListenersUnlink(requestListeners *listeners) {
    while (listeners) {
        listeners->nlistener--;
        listeners = listeners->parent;
    }
}

static void requestListenersPush(requestListeners *listeners,
        requestListener *listener) {
    serverAssert(listeners);
    listAddNodeTail(listeners->listeners, listener);
    requestListenersLink(listeners, listener);
}

requestListener *requestListenersPop(requestListeners *listeners) {
    serverAssert(listeners);
    if (!listLength(listeners->listeners)) return NULL;
    listNode *ln = listFirst(listeners->listeners);
    requestListener *listener = listNodeValue(ln);
    listDelNode(listeners->listeners, ln);
    requestListenersUnlink(listeners);
    return listener;
}

requestListener *requestListenersPeek(requestListeners *listeners) {
    serverAssert(listeners);
    if (!listLength(listeners->listeners)) return NULL;
    listNode *ln = listFirst(listeners->listeners);
    requestListener *listener = listNodeValue(ln);
    return listener;
}

static requestListener *requestListenersFirst(requestListeners *listeners) {
    serverAssert(listeners);
    listNode *ln = listFirst(listeners->listeners);
    return ln ? listNodeValue(ln) : NULL;
}

requestListener *requestListenerCreate(requestListeners *listeners,
        int64_t txid) {
    requestListener *listener = zcalloc(sizeof(requestListener));
    UNUSED(listeners);
    listener->txid = txid;
    listener->entries = listener->buf;
    listener->capacity = sizeof(listener->buf)/sizeof(requestListenerEntry);
    listener->count = 0;
    listener->proceeded = 0;
    listener->notified = 0;
    requestListenersPush(listeners, listener);
    serverAssert(listener->txid == listeners->cur_txid);
    listener->ntxlistener = listeners->cur_ntxlistener;
    return listener;
}

/* Normally pd is swapCtx, which should not be freed untill binded listener
 * released. so we pass pdfree to listener to free it. */
void requestListenerPushEntry(requestListener *listener,
        redisDb *db, robj *key, requestProceed cb, client *c, void *pd,
        freefunc pdfree, void *msgs) {
    requestListenerEntry *entry;
    UNUSED(msgs);

    if (listener->count == listener->capacity) {
        size_t orig_capacity = listener->capacity;
        listener->capacity *= 2;
        if (listener->buf == listener->entries) {
            listener->entries = zcalloc(
                    listener->capacity * sizeof(requestListenerEntry));
            memcpy(listener->entries, listener->buf,
                    sizeof(requestListenerEntry) * orig_capacity);
        } else {
            listener->entries = zrealloc(listener->entries,
                    listener->capacity * sizeof(requestListenerEntry));
        }
        serverAssert(listener->capacity > listener->count);
    }

    entry = listener->entries + listener->count;

    entry->db = db;
    if (key) incrRefCount(key);
    entry->key = key;
    entry->proceed = cb;
    entry->c = c;
    entry->pd = pd;
    entry->pdfree = pdfree;
#ifdef SWAP_DEBUG
    entry->msgs = msgs;

    listener->count++;
#endif
}

void requestListenerRelease(requestListener *listener) {
    int i;
    requestListenerEntry *entry;
    if (!listener) return;
    for (i = 0; i < listener->count; i++) {
       entry = listener->entries + i; 
       if (entry->key) decrRefCount(entry->key);
       if (entry->pdfree) entry->pdfree(entry->pd);
    }
    if (listener->buf != listener->entries) zfree(listener->entries);
    zfree(listener);
}

char *requestListenerDump(requestListener *listener) {
    static char repr[256];
    char *ptr = repr, *end = repr + sizeof(repr) - 1;
    
    ptr += snprintf(ptr,end-ptr,
            "txid=%ld,count=%d,proceeded=%d,notified=%d,ntxlistener=%d,entries=[",
            listener->txid,listener->count,listener->proceeded,listener->notified,
            listener->ntxlistener);

    for (int i = 0; i < listener->count && ptr < end; i++) {
        requestListenerEntry *entry = listener->entries+i;
        const char *intention = entry->c->cmd ? swapIntentionName(entry->c->cmd->intention) : "<nil>";
        char *cmd = (entry->c && entry->c->cmd) ? entry->c->cmd->name : "<nil>";
        char *key = entry->key ? entry->key->ptr : "<nil>";
        ptr += snprintf(ptr,end-ptr,"(%s:%s:%s),",intention,cmd,key);
    }

    if (ptr < end) snprintf(ptr, end-ptr, "]");
    return repr;
}

dictType requestListenersDictType = {
    dictSdsHash,                    /* hash function */
    NULL,                           /* key dup */
    NULL,                           /* val dup */
    dictSdsKeyCompare,              /* key compare */
    dictSdsDestructor,              /* key destructor */
    NULL,                           /* val destructor */
    NULL                            /* allow to expand */
};

static requestListeners *requestListenersCreate(int level, redisDb *db,
        robj *key, requestListeners *parent) {
    requestListeners *listeners;

    listeners = zmalloc(sizeof(requestListeners));
    listeners->listeners = listCreate();
    listeners->nlistener = 0;
    listeners->parent = parent;
    listeners->level = level;
    listeners->cur_txid = -1;
    listeners->cur_ntxlistener = 0;

    switch (level) {
    case REQUEST_LEVEL_SVR:
        listeners->svr.dbnum = server.dbnum;
        listeners->svr.dbs = zmalloc(server.dbnum*sizeof(requestListeners));
        break;
    case REQUEST_LEVEL_DB:
        serverAssert(db);
        listeners->db.db = db;
        listeners->db.keys = dictCreate(&requestListenersDictType, NULL);
        break;
    case REQUEST_LEVEL_KEY:
        serverAssert(key);
        serverAssert(parent->level == REQUEST_LEVEL_DB);
        incrRefCount(key);
        listeners->key.key = key;
        dictAdd(parent->db.keys,sdsdup(key->ptr),listeners);
        break;
    default:
        break;
    }

    return listeners;
}

void requestListenersRelease(requestListeners *listeners) {
    if (!listeners) return;
    serverAssert(!listLength(listeners->listeners));
    listRelease(listeners->listeners);
    listeners->listeners = NULL;

    switch (listeners->level) {
    case REQUEST_LEVEL_SVR:
        zfree(listeners->svr.dbs);
        break;
    case REQUEST_LEVEL_DB:
        dictRelease(listeners->db.keys);
        break;
    case REQUEST_LEVEL_KEY:
        serverAssert(listeners->parent->level == REQUEST_LEVEL_DB);
        dictDelete(listeners->parent->db.keys,listeners->key.key->ptr);
        decrRefCount(listeners->key.key);
        break;
    default:
        break;
    }
    zfree(listeners);
}

sds requestListenersDump(requestListeners *listeners) {
    listIter li;
    listNode *ln;
    sds result = sdsempty();
    char *key;

    switch (listeners->level) {
    case REQUEST_LEVEL_SVR:
        key = "<svr>";
        break;
    case REQUEST_LEVEL_DB:
        key = "<db>";
        break;
    case REQUEST_LEVEL_KEY:
        key = listeners->key.key->ptr;
        break;
    default:
        key = "?";
        break;
    }
    result = sdscatprintf(result,"(level=%s,len=%ld,key=%s):",
            requestLevelName(listeners->level),
            listLength(listeners->listeners), key);

    result = sdscat(result, "[");
    listRewind(listeners->listeners,&li);
    while ((ln = listNext(&li))) {
        requestListener *listener = listNodeValue(ln);
        if (ln != listFirst(listeners->listeners)) result = sdscat(result,",");
        result = sdscat(result,requestListenerDump(listener));
    }
    result = sdscat(result, "]");
    return result;
}


requestListeners *serverRequestListenersCreate() {
    int i;
    requestListeners *s = requestListenersCreate(
            REQUEST_LEVEL_SVR,NULL,NULL,NULL);

    for (i = 0; i < server.dbnum; i++) {
        redisDb *db = server.db + i;
        s->svr.dbs[i] = requestListenersCreate(
                REQUEST_LEVEL_DB,db,NULL,s);
    }
    return s;
}

void serverRequestListenersRelease(requestListeners *s) {
    int i;
    for (i = 0; i < s->svr.dbnum; i++) {
        requestListenersRelease(s->svr.dbs[i]);
    }
    requestListenersRelease(s);
    zfree(s);
}

static requestListeners *requestBindListeners(redisDb *db, robj *key,
        int create) {
    requestListeners *svr_listeners, *db_listeners, *key_listeners;

    svr_listeners = server.request_listeners;
    if (db == NULL || listLength(svr_listeners->listeners)) {
        return svr_listeners;
    }

    db_listeners = svr_listeners->svr.dbs[db->id];
    if (key == NULL || listLength(db_listeners->listeners)) {
        return db_listeners;
    }

    key_listeners = dictFetchValue(db_listeners->db.keys,key->ptr);
    if (key_listeners == NULL) {
        if (create) {
            key_listeners = requestListenersCreate(
                    REQUEST_LEVEL_KEY,db,key,db_listeners);
        }
    }

    return key_listeners;
}

static inline int proceed(requestListeners *listeners,
        requestListener *listener) {
    int i, retval, result;

    DEBUG_MSGS_APPEND(listener->msgs,"wait-proceed","listener=%s",
            requestListenerDump(listener));
    for (i = listener->proceeded; i < listener->count; i++) {
        requestListenerEntry *entry = listener->entries+i;
        retval = entry->proceed(listeners,entry->db,
                entry->key,entry->c,entry->pd);
        if (retval < 0) result = retval;
        listener->proceeded++;
    }
    serverAssert(listener->proceeded == listener->count);

    return result;
}

static inline requestListener *requestListenersLast(requestListeners *listeners) {
    listNode *ln = listLast(listeners->listeners);
    return ln ? listNodeValue(ln) : NULL;
}

/* There are listener in listeners blocking txid from proceeding. */
static int listenerWaitWouldBlock(int64_t txid, requestListeners *listeners) {
    int64_t ntxlistener;
    ntxlistener = listeners->cur_txid == txid ? listeners->cur_ntxlistener : 0;
    return listeners->nlistener > ntxlistener;
}

int requestWaitWouldBlock(int64_t txid, redisDb *db, robj *key) {
    requestListeners *listeners = requestBindListeners(db,key,0);
    if (listeners == NULL) return 0;
    return listenerWaitWouldBlock(txid, listeners);
}

requestListener *requestBindListener(int64_t txid,
        requestListeners *listeners) {
    requestListener *last = requestListenersLast(listeners);
    if (last == NULL || last->txid != txid) {
        last = requestListenerCreate(listeners,txid);
    }
    return last;
}

/* Note: to support reentrant wait, requestWait for one txid MUST next to
 * each other (submit by main thread in one batch). */
int requestWait(int64_t txid, redisDb *db, robj *key, requestProceed cb,
        client *c, void *pd, freefunc pdfree, void *msgs) {
    int blocking;
    requestListeners *listeners;
    requestListener *listener;

    listeners = requestBindListeners(db,key,1);
    blocking = listenerWaitWouldBlock(txid,listeners);

    listener = requestBindListener(txid,listeners);
    requestListenerPushEntry(listener,db,key,cb,c,pd,pdfree,msgs);

#ifdef SWAP_DEBUG
    sds dump = requestListenersDump(listeners);
    DEBUG_MSGS_APPEND(msgs,"wait-bind","listener = %s", dump);
    sdsfree(dump);
#endif

    /* Proceed right away if request key is not blocking, otherwise
     * proceed is defered. */
    if (!blocking) proceed(listeners,listener);

    return 0;
}

int proceedChain(requestListeners *listeners, requestListener *listener) {
    int nchilds;
    requestListeners *parent;
    requestListener *first;
    int64_t txid = listener->txid;

    while (1) {
        if (listener != NULL) proceed(listeners,listener);

        parent = listeners->parent;
        if (parent == NULL) break;

        first = requestListenersFirst(parent);
        if (first) {
            nchilds = parent->nlistener-(int)listLength(parent->listeners);
        }

        /* Proceed upwards if:
         * - parent is empty.
         * - all childs and parent are in the same tx. */ 
        if (first == NULL || (first->txid == txid &&
                    first->ntxlistener > nchilds)) {
            listeners = parent;
            listener = first;
        } else {
            break;
        }
    }

    return 0;
}

int requestNotify(void *listeners_) {
    requestListeners *listeners = listeners_, *parent;
    requestListener *current, *next;

    current = requestListenersPeek(listeners);

#ifdef SWAP_DEBUG
    sds dump = requestListenersDump(listeners);
    DEBUG_MSGS_APPEND(current->msgs,"wait-unbind","listener=%s", dump);
    sdsfree(dump);
#endif

    serverAssert(current->count > current->notified);
    current->notified++;
    if (current->notified < current->count) {
        /* wait untill all notified for reentrant listener. */
        return 0;
    } else {
        requestListenersPop(listeners);
        requestListenerRelease(current);
    }

    while (listeners) {
        if (listLength(listeners->listeners)) {
            next = requestListenersPeek(listeners);
            proceedChain(listeners,next);
            break;
        }

        parent = listeners->parent;
        if (listeners->level == REQUEST_LEVEL_KEY) {
            /* Only key level listeners releases, DB or server level
             * key released only when server exit. */
            requestListenersRelease(listeners);
        }

        if (parent == NULL) {
            listeners = NULL;
            break;
        }

        /* Go upwards if all sibling listeners notified. */
        if (parent->nlistener > (int)listLength(parent->listeners)) {
            listeners = NULL; 
            break;
        }

        listeners = parent;
    }

    return 0;
}

#ifdef REDIS_TEST

static int blocked;

int proceedNotifyLater(void *listeners, redisDb *db, robj *key, client *c, void *pd_) {
    UNUSED(db), UNUSED(key), UNUSED(c);
    void **pd = pd_;
    *pd = listeners;
    blocked--;
    return 0;
}

int swapWaitTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    int error = 0;
    redisDb *db, *db2;
    robj *key1, *key2, *key3;
    void *handle1, *handle2, *handle3, *handledb, *handledb2, *handlesvr;
    int64_t txid = 0;

    TEST("wait: init") {
        int i;
        server.hz = 10;
        server.dbnum = 4;
        server.db = zmalloc(sizeof(redisDb)*server.dbnum);
        for (i = 0; i < server.dbnum; i++) server.db[i].id = i;
        db = server.db, db2 = server.db+1;
        server.request_listeners = serverRequestListenersCreate();
        key1 = createStringObject("key-1",5);
        key2 = createStringObject("key-2",5);
        key3 = createStringObject("key-3",5);

        test_assert(server.request_listeners);
        test_assert(!blocked);
    }

   TEST("wait: parallel key") {
       handle1 = NULL, handle2 = NULL, handle3 = NULL, handlesvr = NULL;
       requestWait(txid++,db,key1,proceedNotifyLater,NULL,&handle1,NULL,NULL), blocked++;
       requestWait(txid++,db,key2,proceedNotifyLater,NULL,&handle2,NULL,NULL), blocked++;
       requestWait(txid++,db,key3,proceedNotifyLater,NULL,&handle3,NULL,NULL), blocked++;
       test_assert(!blocked);
       test_assert(requestWaitWouldBlock(txid++,db,key1));
       test_assert(requestWaitWouldBlock(txid++,db,key2));
       test_assert(requestWaitWouldBlock(txid++,db,key3));
       test_assert(requestWaitWouldBlock(txid++,db,NULL));
       requestNotify(handle1);
       test_assert(!requestWaitWouldBlock(txid++,db,key1));
       requestNotify(handle2);
       test_assert(!requestWaitWouldBlock(txid++,db,key2));
       requestNotify(handle3);
       test_assert(!requestWaitWouldBlock(txid++,db,key3));
       test_assert(!requestWaitWouldBlock(txid++,NULL,NULL));
   } 

   TEST("wait: pipelined key") {
       int i;
       for (i = 0; i < 3; i++) {
           blocked++;
           requestWait(txid++,db,key1,proceedNotifyLater,NULL,&handle1,NULL,NULL);
       }
       test_assert(requestWaitWouldBlock(txid++,db,key1));
       /* first one proceeded, others blocked */
       test_assert(blocked == 2);
       for (i = 0; i < 2; i++) {
           requestNotify(handle1);
           test_assert(requestWaitWouldBlock(txid++,db,key1));
       }
       test_assert(blocked == 0);
       requestNotify(handle1);
       test_assert(!requestWaitWouldBlock(txid++,db,key1));
   }

   TEST("wait: parallel db") {
       requestWait(txid++,db,NULL,proceedNotifyLater,NULL,&handledb,NULL,NULL), blocked++;
       requestWait(txid++,db2,NULL,proceedNotifyLater,NULL,&handledb2,NULL,NULL), blocked++;
       test_assert(!blocked);
       test_assert(requestWaitWouldBlock(txid++,db,NULL));
       test_assert(requestWaitWouldBlock(txid++,db2,NULL));
       requestNotify(handledb);
       requestNotify(handledb2);
       test_assert(!requestWaitWouldBlock(txid++,db,NULL));
       test_assert(!requestWaitWouldBlock(txid++,db2,NULL));
   }

    TEST("wait: mixed parallel-key/db/parallel-key") {
        handle1 = NULL, handle2 = NULL, handle3 = NULL, handledb = NULL;
        requestWait(txid++,db,key1,proceedNotifyLater,NULL,&handle1,NULL,NULL),blocked++;
        requestWait(txid++,db,key2,proceedNotifyLater,NULL,&handle2,NULL,NULL),blocked++;
        requestWait(txid++,db,NULL,proceedNotifyLater,NULL,&handledb,NULL,NULL),blocked++;
        requestWait(txid++,db,key3,proceedNotifyLater,NULL,&handle3,NULL,NULL),blocked++;
        /* key1/key2 proceeded, db/key3 blocked */
        test_assert(requestWaitWouldBlock(txid++,db,NULL));
        test_assert(blocked == 2);
        /* key1/key2 notify */
        requestNotify(handle1);
        test_assert(requestWaitWouldBlock(txid++,db,NULL));
        requestNotify(handle2);
        test_assert(requestWaitWouldBlock(txid++,db,NULL));
        /* db proceeded, key3 still blocked. */
        test_assert(blocked == 1);
        test_assert(handle3 == NULL);
        /* db notified, key3 proceeds but still blocked */
        requestNotify(handledb);
        test_assert(!blocked);
        test_assert(requestWaitWouldBlock(txid++,db,NULL));
        /* db3 proceed, noting would block */
        requestNotify(handle3);
        test_assert(!requestWaitWouldBlock(txid++,db,NULL));
    }

    TEST("wait: mixed parallel-key/server/parallel-key") {
        handle1 = NULL, handle2 = NULL, handle3 = NULL, handlesvr = NULL;
        requestWait(txid++,db,key1,proceedNotifyLater,NULL,&handle1,NULL,NULL),blocked++;
        requestWait(txid++,db,key2,proceedNotifyLater,NULL,&handle2,NULL,NULL),blocked++;
        requestWait(txid++,NULL,NULL,proceedNotifyLater,NULL,&handlesvr,NULL,NULL),blocked++;
        requestWait(txid++,db,key3,proceedNotifyLater,NULL,&handle3,NULL,NULL),blocked++;
        /* key1/key2 proceeded, svr/key3 blocked */
        test_assert(requestWaitWouldBlock(txid++,NULL,NULL));
        test_assert(requestWaitWouldBlock(txid++,db,NULL));
        test_assert(blocked == 2);
        /* key1/key2 notify */
        requestNotify(handle1);
        test_assert(requestWaitWouldBlock(txid++,NULL,NULL));
        requestNotify(handle2);
        test_assert(requestWaitWouldBlock(txid++,NULL,NULL));
        /* svr proceeded, key3 still blocked. */
        test_assert(blocked == 1);
        test_assert(handle3 == NULL);
        /* svr notified, db3 proceeds but still would block */
        requestNotify(handlesvr);
        test_assert(!blocked);
        test_assert(requestWaitWouldBlock(txid++,NULL,NULL));
        /* db3 proceed, noting would block */
        requestNotify(handle3);
        test_assert(!requestWaitWouldBlock(txid++,NULL,NULL));
    }

    return error;
}

#endif
