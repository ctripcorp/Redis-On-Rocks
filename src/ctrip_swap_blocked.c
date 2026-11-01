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
#include "ctrip_swap.h"

void incrSwapUnBlockCtxVersion(void) {
    if (server.swap_dependency_block_ctx) {
        server.swap_dependency_block_ctx->version++;
        if (server.swap_dependency_block_ctx->swapping_count > 0)
            server.swap_dependency_block_ctx->swap_retry_count++;
    }
}

swapUnblockCtx *createSwapUnblockCtx(void) {
    swapUnblockCtx *swap_dependency_block_ctx = zmalloc(sizeof(swapUnblockCtx));
    swap_dependency_block_ctx->version = 0;
    swap_dependency_block_ctx->swap_total_count = 0;
    swap_dependency_block_ctx->swapping_count = 0;
    swap_dependency_block_ctx->swap_retry_count = 0;
    swap_dependency_block_ctx->swap_err_count = 0;
    return swap_dependency_block_ctx;
}

void releaseSwapUnblockCtx(swapUnblockCtx *swap_dependency_block_ctx) {
    if (!swap_dependency_block_ctx) return;
    zfree(swap_dependency_block_ctx);
}
