#ifndef HDR_MALLOC_H__
#define HDR_MALLOC_H__

#include <stdlib.h>

void *zmalloc(size_t size);
void *zcalloc(size_t size);
void *zrealloc(void *ptr, size_t size);
void zfree(void *ptr);

#define hdr_malloc zmalloc
#define hdr_calloc(num, size) zcalloc((size_t)(num) * (size_t)(size))
#define hdr_realloc zrealloc
#define hdr_free zfree
#endif
