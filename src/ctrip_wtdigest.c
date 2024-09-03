/* Copyright (c) 2023, ctrip.com * All rights reserved.
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
#include "ctrip_wtdigest.h"
#include "ctrip_wtdigest_malloc.h"
#include "../deps/tdigest/tdigest.h"

#define DEFAULT_COMPRESSION 100
#define DEFAULT_WINDOW_SECONDS 3600

struct wtdigest_t {
    uint8_t num_buckets;
    td_histogram_t **buckets;
    unsigned long long last_reset_time;
    uint8_t cur_read_index;
    unsigned long long window_seconds;
    int isRunning;
};

wtdigest* wtdigestCreate(uint8_t num_buckets)
{
    serverAssert(num_buckets != 0);
    wtdigest *wt = wtdigest_malloc(sizeof(wtdigest));
    wt->num_buckets = num_buckets;

    wt->buckets = wtdigest_malloc(num_buckets * sizeof(td_histogram_t *));
    for (uint8_t i = 0; i < num_buckets; i++) {
        wt->buckets[i] = td_new(DEFAULT_COMPRESSION);
        // serverAssert(wt->buckets[i] != NULL);
    }

    wt->last_reset_time = 0;
    wt->cur_read_index = 0;
    wt->window_seconds = DEFAULT_WINDOW_SECONDS;
    wt->isRunning = 1;

    return wt;
}

void wtdigestDestroy(wtdigest* wt)
{
    for (uint8_t i = 0; i < wt->num_buckets; i++) {
        // serverAssert(wt->buckets[i] != NULL);
        td_free(wt->buckets[i]);
    }
    wtdigest_free(wt->buckets);
    wtdigest_free(wt);
}

void wtdigestSetWindow(wtdigest* wt, unsigned long long window_seconds)
{
    wt->window_seconds = window_seconds;
}

unsigned long long wtdigestGetWindow(wtdigest* wt)
{
    return wt->window_seconds;
}

int wtdigestIsRunnning(wtdigest* wt)
{
    return wt->isRunning == 1;
}

void wtdigestStart(wtdigest* wt)
{
    wt->isRunning = 1;
}

/*
 * will reset all buckets, and change status to not-running
 */
void wtdigestStop(wtdigest* wt)
{
    for (uint8_t i = 0; i < wt->num_buckets; i++) {
        td_reset(wt->buckets[i]);
    }
    wt->isRunning = 0;
    wt->last_reset_time = 0;
    wt->cur_read_index = 0;
}

void resetBucketsIfNeed(wtdigest* wt)
{
    // unsigned long long reset_period = wt->window_seconds / wt->num_buckets;

    // time_t now_time;
    // time(&now_time);

    // unsigned long long time_passed = now_time - wt->last_reset_time;

    // if (time_passed < reset_period) {
    //     return;
    // }

    // unsigned long long num_buckets_passed = time_passed / reset_period;
    // num_buckets_passed = MIN(wt->num_buckets, num_buckets_passed);

    // for (unsigned long long i = 0; i < num_buckets_passed; i++) {
    //     uint8_t reset_index = (wt->cur_read_index + i) % wt->num_buckets;
    //     td_reset(wt->buckets[reset_index]);
    // }
    // wt->cur_read_index = (wt->cur_read_index + num_buckets_passed) % wt->num_buckets;
    // wt->last_reset_time = now_time;
    return;
}

/**
 * Adds a value to a wtdigest.
 * @param val The value to add.
 * @param weight The weight of this value, sugggested to set to 1 normally.
 * time complexity : nlog(n)
 */
void wtdigestAdd(wtdigest* wt, double val, unsigned long long weight)
{
    if (!wt->isRunning) {
        return;
    }

    resetBucketsIfNeed(wt);

    for (uint8_t i = 0; i < wt->num_buckets; i++) {
        int res = td_add(wt->buckets[i], val, weight);
        if (res != 0) {
            // serverLog(LL_DEBUG, "error happened when wtdigest add val, ret: %d, bucket index: %u.", res, i);
        }
    }

    return 0;
}

/**
 * Returns an estimate of the cutoff such that a specified fraction of the value
 * added to this wtdigest would be less than or equal to the cutoff.
 *
 * @param q The desired fraction.
 * @param res_status 
 * @return The value x such that cdf(x) == q,（cumulative distribution function，CDF).
 * time complexity : nlog(n)
 */
double wtdigestQuantile(wtdigest* wt, double q, int *res_status)
{
    if (!wt->isRunning) {
        *res_status = ERR_STATUS_WTD;
        return 0;
    }
    resetBucketsIfNeed(wt);
    *res_status = OK_STATUS_WTD;
    return td_quantile(wt->buckets[wt->cur_read_index], q);
}

