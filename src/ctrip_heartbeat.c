/* Copyright (c) 2025, ctrip.com * All rights reserved.
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
#include "ctrip.h"

 /* record the clients with heartbeat actions. */
rax *HeartbeatTable = NULL;

/* This is the structure that control heartbeat actions. */
typedef struct heartbeatState {
    long long send_period[NUM_HEARTBEAT_ACTIONS];
    long long last_sent_ts[NUM_HEARTBEAT_ACTIONS];
} heartbeatState;

typedef void (*heartbeatHook)(client*, heartbeatState*);

void heartbeatSystime(client *c, heartbeatState *hbs) {
    if (hbs->send_period[HEARTBEAT_SYSTIME_IDX] <= 0 ||
        server.mstime - hbs->last_sent_ts[HEARTBEAT_SYSTIME_IDX] < hbs->send_period[HEARTBEAT_SYSTIME_IDX] * 1000) {
        return;
    }

    serverAssert(c->flags & CLIENT_HEARTBEAT_SYSTIME);
    /* only support resp3 */
    if (c->resp > 2) {
        addReplyPushLen(c,2);
        addReplyBulkCBuffer(c,"systime",7);   
        addReplyLongLong(c,server.mstime);
        hbs->last_sent_ts[HEARTBEAT_SYSTIME_IDX] = server.mstime;
    }
}

void heartbeatMkps(client *c, heartbeatState *hbs) {
    if (hbs->send_period[HEARTBEAT_MKPS_IDX] <= 0 ||
        server.mstime - hbs->last_sent_ts[HEARTBEAT_MKPS_IDX] < hbs->send_period[HEARTBEAT_MKPS_IDX] * 1000) {
        return;
    }

    serverAssert(c->flags & CLIENT_HEARTBEAT_MKPS);
    /* only support resp3 */
    if (c->resp > 2) {
        addReplyPushLen(c,2);
        addReplyBulkCBuffer(c,"mkps",4);   
        addReplyLongLong(c,getInstantaneousMetric(STATS_METRIC_MODIFIED_KEYS));
        hbs->last_sent_ts[HEARTBEAT_MKPS_IDX] = server.mstime;
    }
}

heartbeatHook heartbeatActions[NUM_HEARTBEAT_ACTIONS] = {
    heartbeatSystime,  /*  HEARTBEAT_SYSTIME_IDX */
    heartbeatMkps     /* HEARTBEAT_MKPS_IDX */
};

void ctripHeartbeat(void) {
    if (HeartbeatTable == NULL) return;

    raxIterator ri;
    raxStart(&ri,HeartbeatTable);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        heartbeatState *hbs = ri.data;
        client *c;
        memcpy(&c,ri.key,sizeof(c));

        for (int i = 0; i < NUM_HEARTBEAT_ACTIONS; i++) {
            heartbeatActions[i](c,hbs);
        }
    }
    raxStop(&ri);
}

void ctripDisableHeartbeat(client *c) {

    if (!(c->flags & (CLIENT_HEARTBEAT_SYSTIME | CLIENT_HEARTBEAT_MKPS))) {
        return;
    }

    heartbeatState *hbs = raxFind(HeartbeatTable,(unsigned char*)&c,sizeof(c));
    serverAssert(hbs != raxNotFound);
    raxRemove(HeartbeatTable,(unsigned char*)&c,sizeof(c),NULL);
    zfree(hbs);

    if (raxSize(HeartbeatTable) == 0) {
        raxFree(HeartbeatTable);
        HeartbeatTable = NULL;
    }

    /* Clear flags and adjust the count. */
    server.heartbeat_clients--;
    c->flags &= ~(CLIENT_HEARTBEAT_SYSTIME|CLIENT_HEARTBEAT_MKPS);
}

void ctripEnableHeartbeat(client *c, uint64_t options, long long heartbeat_period[]) {

    if (!(c->flags & (CLIENT_HEARTBEAT_SYSTIME|CLIENT_HEARTBEAT_MKPS))) server.heartbeat_clients++;

    serverAssert(options & (CLIENT_HEARTBEAT_SYSTIME | CLIENT_HEARTBEAT_MKPS));
    c->flags |= (options & (CLIENT_HEARTBEAT_SYSTIME | CLIENT_HEARTBEAT_MKPS));

    if (HeartbeatTable == NULL) {
        HeartbeatTable = raxNew();
    }
    heartbeatState *hbs = raxFind(HeartbeatTable,(unsigned char*)c,sizeof(c));
    if (hbs == raxNotFound) {
        hbs = zcalloc(sizeof(heartbeatState));
        raxInsert(HeartbeatTable,(unsigned char*)&c,sizeof(c),hbs,NULL);
    }
    for (int i = 0; i < NUM_HEARTBEAT_ACTIONS; i++) {
        hbs->send_period[i] = heartbeat_period[i];
        hbs->last_sent_ts[i] = server.mstime;
    }
}
