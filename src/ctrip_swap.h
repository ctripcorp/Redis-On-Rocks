#ifndef  __CTRIP_SWAP_H__
#define  __CTRIP_SWAP_H__

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
#include "server.h"

typedef rocksdb_iterator_t rocksIter;
rocksIter *rocksCreateIter(struct rocks *rocks, redisDb *db);
int rocksIterNext(rocksIter *it, const char **rawkey, size_t *klen, const char **rawval, size_t *vlen);
void rocksReleaseIter(rocksIter *it);

sds rocksEncodeKey(int type, sds key);
int rocksDecodeKey(const char *rawkey, size_t rawlen, const char **key, size_t *klen);

int rdbSaveRocks(rio *rdb, redisDb *db, int rdbflags);

/* parallel swap */
typedef int (*parallelSwapFinishedCb)(sds rawkey, sds rawval, void *pd);

typedef struct {
    int inprogress;         /* swap entry in progress? */
    int pipe_read_fd;       /* read end to wait rio swap finish. */
    int pipe_write_fd;      /* write end to notify swap finish by rio. */
    struct RIO *r;          /* swap attached RIO handle. */
    parallelSwapFinishedCb cb; /* swap finished callback. */
    void *pd;               /* swap finished private data. */
} swapEntry;

typedef struct parallelSwap {
    list *entries;
    int parallel;
} parallelSwap;

parallelSwap *parallelSwapNew(int parallel);
void parallelSwapFree(parallelSwap *ps);
int parallelSwapSubmit(parallelSwap *ps, int action, sds rawkey, sds rawval, parallelSwapFinishedCb cb, void *pd);
int parallelSwapDrain(parallelSwap *ps);
int parallelSwapGet(sds rawkey, parallelSwapFinishedCb cb, void *pd);
int parallelSwapPut(sds rawkey, sds rawval, parallelSwapFinishedCb cb, void *pd);
int parallelSwapDel(sds rawkey, parallelSwapFinishedCb cb, void *pd);

#endif
