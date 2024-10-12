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
    unsigned long long begin_time;
};

wtdigest* wtdigestCreate(uint8_t num_buckets)
{
    serverAssert(num_buckets != 0);
    wtdigest *wt = wtdigest_malloc(sizeof(wtdigest));
    wt->num_buckets = num_buckets;

    wt->buckets = wtdigest_malloc(num_buckets * sizeof(td_histogram_t *));
    for (uint8_t i = 0; i < num_buckets; i++) {
        wt->buckets[i] = td_new(DEFAULT_COMPRESSION);
        serverAssert(wt->buckets[i] != NULL);
    }

    wt->last_reset_time = time(NULL);
    wt->cur_read_index = 0;
    wt->window_seconds = DEFAULT_WINDOW_SECONDS;
    wt->begin_time = time(NULL);

    return wt;
}

void wtdigestDestroy(wtdigest* wt)
{
    if (wt == NULL) {
        return;
    }
    for (uint8_t i = 0; i < wt->num_buckets; i++) {
        serverAssert(wt->buckets[i] != NULL);
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

unsigned long long wtdigestGetRunnningTime(wtdigest* wt)
{
    return time(NULL) - wt->begin_time;
}

void wtdigestReset(wtdigest* wt)
{
    for (unsigned long long i = 0; i < wt->num_buckets; i++) {
        td_reset(wt->buckets[i]);
    }

    wt->last_reset_time = time(NULL);
    wt->cur_read_index = 0;
    wt->begin_time = time(NULL);
}

void resetBucketsIfNeed(wtdigest* wt)
{
    unsigned long long reset_period = wt->window_seconds / wt->num_buckets;

    time_t now_time = time(NULL);

    unsigned long long time_passed = now_time - wt->last_reset_time;

    if (time_passed < reset_period) {
        return;
    }

    unsigned long long num_buckets_passed = time_passed / reset_period;
    num_buckets_passed = MIN(wt->num_buckets, num_buckets_passed);

    for (unsigned long long i = 0; i < num_buckets_passed; i++) {
        uint8_t reset_index = (wt->cur_read_index + i) % wt->num_buckets;
        td_reset(wt->buckets[reset_index]);
    }
    wt->cur_read_index = (wt->cur_read_index + num_buckets_passed) % wt->num_buckets;
    wt->last_reset_time = now_time;
    return;
}

int wtdigestAdd(wtdigest* wt, double val, unsigned long long weight)
{
    resetBucketsIfNeed(wt);

    for (uint8_t i = 0; i < wt->num_buckets; i++) {
        int res = td_add(wt->buckets[i], val, weight);
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

double wtdigestQuantile(wtdigest* wt, double q)
{
    resetBucketsIfNeed(wt);
    return td_quantile(wt->buckets[wt->cur_read_index], q);
}

long long wtdigestSize(wtdigest* wt)
{
    return td_size(wt->buckets[wt->cur_read_index]);
}

#ifdef REDIS_TEST
int wtdigestTest(int argc, char *argv[], int accurate) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);
    int error = 0;

        TEST("wtdigest: create destroy") {
            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS);
            wtdigestDestroy(wt);
        }

        TEST("wtdigest: set & get window & get runnning time") {

            unsigned long long start_time = time(NULL);
            sleep(1);
            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS);
            test_assert(wt->begin_time >= start_time + 1);

            serverAssert(wtdigestGetWindow(wt) == DEFAULT_WINDOW_SECONDS);
            wtdigestSetWindow(wt, 7200);
            serverAssert(wtdigestGetWindow(wt) == 7200);

            sleep(1);
            unsigned long long running_time = wtdigestGetRunnningTime(wt);
            serverAssert(running_time >= 1);

            unsigned long long end_time = time(NULL);
            serverAssert(end_time - start_time >= running_time);
            serverAssert(end_time - start_time >= 2);
            wtdigestDestroy(wt);
        }

        TEST("wtdigest: basic add Quantile") {

            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS);

            for (int i = 0; i < 30; i++) {
                wtdigestAdd(wt, 200, 1);
            }

            for (int i = 0; i < 50; i++) {
                wtdigestAdd(wt, 100, 1);
            }

            for (int i = 0; i < 20; i++) {
                wtdigestAdd(wt, 300, 1);
            }

            wtdigestAdd(wt, 400, 1);
            wtdigestAdd(wt, 4, 1);

            int q;

            q = (int)wtdigestQuantile(wt, 0.001);
            serverAssert(4 == q);

            q = (int)wtdigestQuantile(wt, 0.5);
            serverAssert(150 == q);

            q = (int)wtdigestQuantile(wt, 0.80);
            serverAssert(268 == q);

            q = (int)wtdigestQuantile(wt, 0.99);
            serverAssert(300 == q);

            q = (int)wtdigestQuantile(wt, 0.999);
            serverAssert(400 == q);

            wtdigestDestroy(wt);
        }

        TEST("wtdigest: bucket rotate reading") {

            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS);

            /* reset period = 2 */
            wtdigestSetWindow(wt, 2 * WTD_DEFAULT_NUM_BUCKETS);

            /* rotate for two circle */
            for (int i = 0; i < WTD_DEFAULT_NUM_BUCKETS * 2; i++) {
                wtdigestAdd(wt, 10, 1);
                serverAssert((i % WTD_DEFAULT_NUM_BUCKETS) == wt->cur_read_index);

                long long size = td_size(wt->buckets[wt->cur_read_index]);
                serverAssert(size <= 1 * WTD_DEFAULT_NUM_BUCKETS);

                uint8_t last_bucket_idx = (wt->cur_read_index + WTD_DEFAULT_NUM_BUCKETS - 1) % WTD_DEFAULT_NUM_BUCKETS;
                long long last_bucket_size = td_size(wt->buckets[last_bucket_idx]);
                serverAssert(last_bucket_size == 1);

                time_t now_time;
                time(&now_time);
                serverAssert(now_time - wt->last_reset_time < 2);
                sleep(2);
            }

            wtdigestDestroy(wt);
        }

        TEST("wtdigest: reset bucket") {

            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS);

            /* reset period = 1 */
            wtdigestSetWindow(wt, WTD_DEFAULT_NUM_BUCKETS);
            wtdigestAdd(wt, 10, 1);

            uint8_t index1 = wt->cur_read_index;
            sleep(3);

            (void)wtdigestQuantile(wt, 0.5);

            /* reset and pass 3 buckets */
            uint8_t index2 = wt->cur_read_index;
            serverAssert(index2 - index1 == 3);

            sleep(4);

            (void)wtdigestQuantile(wt, 0.5);

            uint8_t index3 = wt->cur_read_index;
            serverAssert(index3 == 1);

            wtdigestDestroy(wt);
        }

        TEST("wtdigest: rotate add Quantile") {
            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS);

            /* reset period = 1 */
            wtdigestSetWindow(wt, WTD_DEFAULT_NUM_BUCKETS);

            /* rotate  */
            for (int j = 0; j < WTD_DEFAULT_NUM_BUCKETS * 100000; j++) {
                
                /* add different value at each reset period */
                if (j % WTD_DEFAULT_NUM_BUCKETS <= 2) {
                    for (int i = 0; i < 10; i++) {
                        wtdigestAdd(wt, 100, 1);
                    }
                }

                if (j % WTD_DEFAULT_NUM_BUCKETS == 3 || j % WTD_DEFAULT_NUM_BUCKETS == 4) {
                    for (int i = 0; i < 10; i++) {
                        wtdigestAdd(wt, 200, 1);
                    }
                }

                if (j % WTD_DEFAULT_NUM_BUCKETS == 5) {
                    for (int i = 0; i < 10; i++) {
                        wtdigestAdd(wt, 300, 1);
                    }
                }

                int q = (int)wtdigestQuantile(wt, 0.5);

                serverAssert(100 <= q);
                serverAssert(200 >= q);

            }
            wtdigestDestroy(wt);
        }

        TEST("wtdigest: reset then add") {
        
            wtdigest *wt = wtdigestCreate(WTD_DEFAULT_NUM_BUCKETS);
        
            /* reset period = 1 */
            wtdigestSetWindow(wt, WTD_DEFAULT_NUM_BUCKETS);
            sleep(2);
            wtdigestAdd(wt, 100, 1);
            serverAssert(wt->cur_read_index == 2);

            wtdigestReset(wt);

            for (unsigned long long i = 0; i < wt->num_buckets; i++) {
                serverAssert(0 == td_size(wt->buckets[i]));
            }
            serverAssert(wt->cur_read_index == 0);

            /* restart to work */
            sleep(2);       
            wtdigestAdd(wt, 100, 1);
            serverAssert(wt->cur_read_index == 2);

            serverAssert(1 == td_size(wt->buckets[wt->cur_read_index]));

            wtdigestDestroy(wt);
        }

        return error;
}
#endif 