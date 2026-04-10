
#ifndef __BUFFERED_ALLOCATOR_H__
#define __BUFFERED_ALLOCATOR_H__
#include <stdlib.h>
typedef struct bufferedAllocatorPtr {
    long flags;
    char content[];
} bufferedAllocatorPtr;
typedef void (*newauxfn)(void*);
typedef void (*freeauxfn)(void*);
typedef struct bufferedAllocator {
    bufferedAllocatorPtr *buffered; /* array of buffered(pre-allocated) ptr */
    bufferedAllocatorPtr **stack; /* stack of pointer to buffered ptr */
    size_t capacity;
    size_t occupied;
    size_t size; /* size of buffered element */
    size_t unbuffered; /* # of unbuffered ptr */
    newauxfn newauxcb; /* callback to create child ptr member */
    freeauxfn freeauxcb; /* callback to free child ptr member */
} bufferedAllocator;

bufferedAllocator *bufferedAllocatorCreate(size_t capacity, size_t size, newauxfn newauxcb, freeauxfn freeauxcb);
void bufferedAllocatorDestroy(bufferedAllocator *ba);
void *bufferedAllocatorAlloc(struct bufferedAllocator *ba);
void bufferedAllocatorFree(struct bufferedAllocator *ba, void *content);
#endif