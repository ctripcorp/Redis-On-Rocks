#ifndef __CTRIP_STORAGE_ADLIST_H__
#define __CTRIP_STORAGE_ADLIST_H__

#include "adlist.h"

/* extent list api so that list node re-allocate can be avoided. */

void listUnlink(list *list, listNode *node);
void listLinkHead(list *list, listNode *node);
void listLinkTail(list *list, listNode *node);

#endif /* __CTRIP_STORAGE_ADLIST_H__ */