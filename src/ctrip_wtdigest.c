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

#include "ctrip_wtdigest.h"
#include "ctrip_wtdigest_malloc.h"
#include "../deps/tdigest/tdigest.h"

struct wtdigest_t {
    uint8_t num_td;
    td_histogram_t **td;
    long long last_refresh_ts;
    uint8_t cur_td_index;
    long long window_seconds;
    int isRunning;
};

wtdigest* wtdigestCreate(uint8_t buckets_num)
{
    return NULL;
}

void wtdigestDestroy(wtdigest* wt)
{

}

void wtdigestSetWindow(wtdigest* wt, long long window_seconds)
{

}

long long wtdigestGetWindow(wtdigest* wt)
{
    return 0;
}

int wtdigestIsRunnning(wtdigest* wt)
{
    return 0;
}

void wtdigestStart(wtdigest* wt)
{

}

/*
 * will reset all buckets, and change status to not-running
 */
void wtdigestStop(wtdigest* wt)
{

}

/**
 * Adds a value to a wtdigest.
 * @param val The value to add.
 * @param weight The weight of this value, sugggested to set to 1 normally.
 * time complexity : nlog(n)
 */
void wtdigestAdd(wtdigest* wt, double val, long long weight)
{

}

/**
 * Returns an estimate of the cutoff such that a specified fraction of the value
 * added to this wtdigest would be less than or equal to the cutoff.
 *
 * @param q The desired fraction.
 * @return The value x such that cdf(x) == q,（cumulative distribution function，CDF).
 * time complexity : nlog(n)
 */
long long wtdigestQuantile(wtdigest* wt, double q)
{
    return 0;
}