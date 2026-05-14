# Regression test for ASAN false-positive "direct leak" of listMeta.
#
# Root cause: objectMeta stored a listMeta* in a 60-bit bitfield (ptr:60).
# The raw memory word is (heap_addr << 4) | swap_type_nibble, which ASAN's
# LeakSanitizer does not recognize as a valid heap pointer. When the server
# exits with an unprocessed batch in server.swap_CQ->complete_queue, ASAN
# (on x86-64 with GCC) reports listMeta as a "direct leak" even though it IS
# reachable via: server.swap_CQ->complete_queue->batch->req->data->new_meta->ptr
#
# Fix: change objectMeta.ptr from a 60-bit bitfield to a plain void*, so ASAN
# can follow the pointer and classify listMeta as "still reachable" (not leaked).
#
# Reproduction mechanism:
#   asyncCompleteQueueDrain has a 2000ms time limit. With rio_delay=3s and
#   shutdown triggered immediately after the eviction (<< 1ms gap), the drain
#   times out while the background thread is still sleeping. swapThreadsDeinit
#   then joins the background thread; the thread finishes the write, appends
#   the batch to complete_queue, and exits. Nobody processes complete_queue
#   after this, so evictClientKeyRequestFinished (and listMetaFree) are never
#   called. At clean server exit (~3s total) ASAN's LSan runs:
#     - old code (ptr:60): (lm_addr<<4)|swap_type not a valid pointer -> DIRECT LEAK
#     - new code (void*ptr): pointer visible -> still reachable -> no leak
#
# NOTE: Full ASAN detection requires an x86-64 build where jemalloc+ASAN
# properly intercepts all allocations. On aarch64, jemalloc allocations may
# not be tracked by LSan, making the leak undetectable locally but visible
# in CI (x86-64, GCC).

start_server {tags {"swap" "regression"}} {
    test {no ASAN leak when async complete_queue has unprocessed batch at clean shutdown} {
        # Disable eviction initially so setup commands don't trigger eviction.
        r config set swap-debug-evict-keys 0

        # Set RIO delay: 3s > asyncCompleteQueueDrain timeout (2s).
        # When the background thread picks up the eviction, it will sleep 3s.
        r config set swap-debug-rio-delay-micro 3000000

        # Create hot list keys. listSwapAna (SWAP_OUT, pure-hot key path) will
        # allocate a listMeta and store it in objectMeta.ptr for each key.
        for {set i 0} {$i < 5} {incr i} {
            r rpush "list:$i" a b c d e
        }

        # Re-enable eviction, then trigger it immediately.
        r config set swap-debug-evict-keys 5

        # PING triggers commandProcessed -> swapDebugEvictKeys -> deferred SWAP_OUT
        # requests for all hot list keys. Background thread starts its 3s write.
        r ping

        # Immediately initiate graceful shutdown (<1ms after eviction start).
        # asyncCompleteQueueDrain(2000ms) times out (thread still in 3s write).
        # swapThreadsDeinit joins the thread; thread finishes write, appends
        # the batch to complete_queue, and exits. Server exits cleanly (~3s).
        # ASAN's LeakSanitizer runs at exit (on x86-64 with GCC ASAN+jemalloc):
        #   - old code: objectMeta.ptr:60 = (lm_addr<<4)|swap_type -> unrecognized
        #               heap pointer -> listMeta reported as DIRECT LEAK -> FAIL
        #   - new code: objectMeta.ptr (void*) = lm_addr -> traceable -> PASS
        catch {r shutdown nosave}
    }
}
