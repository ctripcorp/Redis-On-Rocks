#include "buffered_allocator.h"

#include <stdlib.h>
#include <stddef.h>
#include "assert.h"
#include <zmalloc.h>
#define BUFFERED_ALLOCATOR_BUFFERED (1ULL<<0)
static inline void bufferedAllocatorSetBuffered(bufferedAllocatorPtr *ptr, int buffered) {
    if (buffered) {
        ptr->flags |= BUFFERED_ALLOCATOR_BUFFERED;
    } else {
        ptr->flags &= ~BUFFERED_ALLOCATOR_BUFFERED;
    }
}

static inline int bufferedAllocatorGetBuffered(bufferedAllocatorPtr *ptr) {
    return ptr->flags & BUFFERED_ALLOCATOR_BUFFERED;
}

static inline void bufferedAllocatorNewAux(bufferedAllocator *ba, bufferedAllocatorPtr *ptr) {
    if (ba->newauxcb) ba->newauxcb(ptr->content);
}

static inline void bufferedAllocatorFreeAux(bufferedAllocator *ba, bufferedAllocatorPtr *ptr) {
    if (ba->freeauxcb) ba->freeauxcb(ptr->content);
}

static inline bufferedAllocatorPtr *bufferedAllocatorPtrFromContent(void *content) {
    return (bufferedAllocatorPtr*)((char*)content-offsetof(bufferedAllocatorPtr,content));
}

static inline void bufferedAllocatorPushPtr(bufferedAllocator *ba, bufferedAllocatorPtr *ptr) {
    assert(ba->occupied > 0);
    ba->occupied--;
    ba->stack[ba->occupied] = ptr;
}

static inline bufferedAllocatorPtr *bufferedAllocatorPopPtr(bufferedAllocator *ba) {
    assert(ba->occupied < ba->capacity);
    bufferedAllocatorPtr *ptr = ba->stack[ba->occupied];
    ba->occupied++;
    return ptr;
}

static inline int bufferedAllocatorEmpty(bufferedAllocator *ba) {
    assert(ba->occupied <= ba->capacity);
    return ba->occupied == ba->capacity;
}

static inline int bufferedAllocatorFull(bufferedAllocator *ba) {
    assert(ba->occupied <= ba->capacity);
    return ba->occupied == 0;
}

bufferedAllocator *bufferedAllocatorCreate(size_t capacity, size_t size, newauxfn newauxcb, freeauxfn freeauxcb) {
    bufferedAllocator *ba = zcalloc(sizeof(bufferedAllocator));
    size_t elesize = sizeof(bufferedAllocatorPtr)+size;

    ba->occupied = capacity;
    ba->capacity = capacity;
    ba->unbuffered = 0;
    ba->size = size;
    ba->newauxcb = newauxcb;
    ba->freeauxcb = freeauxcb;

    ba->stack = zcalloc(sizeof(void*)*capacity);
    ba->buffered = zcalloc(elesize*capacity);

    for (size_t i = 0; i < capacity; i++) {
        bufferedAllocatorPtr *ptr = (void*)((char*)ba->buffered + elesize*i);
        bufferedAllocatorSetBuffered(ptr,1);
        bufferedAllocatorNewAux(ba,ptr);
        bufferedAllocatorPushPtr(ba,ptr);
    }

    return ba;
}

void bufferedAllocatorDestroy(bufferedAllocator *ba) {
    assert(ba->unbuffered == 0);
    assert(bufferedAllocatorFull(ba));
    for (size_t i = 0; i < ba->capacity; i++) {
        bufferedAllocatorPtr *ptr = bufferedAllocatorPopPtr(ba);
        bufferedAllocatorFreeAux(ba,ptr);
    }
    zfree(ba->buffered), ba->buffered = NULL;
    zfree(ba->stack), ba->stack = NULL;
    zfree(ba);
}

void *bufferedAllocatorAlloc(bufferedAllocator *ba) {
    bufferedAllocatorPtr *ptr;

    if (bufferedAllocatorEmpty(ba)) {
        ptr = zcalloc(sizeof(bufferedAllocatorPtr)+ba->size);
        bufferedAllocatorNewAux(ba,ptr);
        bufferedAllocatorSetBuffered(ptr,0);
        ba->unbuffered++;
    } else {
        ptr = bufferedAllocatorPopPtr(ba);
        bufferedAllocatorSetBuffered(ptr,1);
    }
    return ptr->content;
}

void bufferedAllocatorFree(bufferedAllocator *ba, void *content) {
    if (content == NULL) return;
    bufferedAllocatorPtr *ptr = bufferedAllocatorPtrFromContent(content);
    if (bufferedAllocatorGetBuffered(ptr)) {
        bufferedAllocatorPushPtr(ba,ptr);
    } else {
        bufferedAllocatorFreeAux(ba,ptr);
        zfree(ptr);
        ba->unbuffered--;
    }
}