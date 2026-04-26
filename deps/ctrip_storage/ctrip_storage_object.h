

#ifndef __CTRIP_STORAGE_OBJECT_H__
#define __CTRIP_STORAGE_OBJECT_H__


/* object*/ 
typedef struct StorageObjectNamespace {
    unsigned dirty_meta:1;     /* set to 1 if rocksdb and redis meta differs */ 
    unsigned dirty_data:1;     /* set to 1 if rocksdb and redis data differs */ 
    unsigned persistent:1; 
    unsigned persist_keep:1;   /* set to 1 if persist key should keep value in memory */ 
} StorageObjectNamespace;
#define initObjectStorage(o) do {\
    (o)->storage.dirty_meta = 1; \
    (o)->storage.dirty_data = 1; \
    (o)->storage.persistent = 0; \
    (o)->storage.persist_keep = 0; \
} while(0)

#define objectIsMetaDirty(o) ((o)->storage.dirty_meta)
#define objectIsDataDirty(o) ((o)->storage.dirty_data)

#define objectIsDirty(o) (objectIsMetaDirty(o) || objectIsDataDirty(o))

#define clearObjectMetaDirty(o) do { \
    if (o) o->storage.dirty_meta = 0; \
} while(0)

#define clearObjectDataDirty(o) do { \
    if (o) o->storage.dirty_data = 0; \
} while(0)

#define clearObjectDirty(o) do { \
  clearObjectMetaDirty(o); \
  clearObjectDataDirty(o); \
} while(0)

#define clearObjectPersistKeep(o) do { \
    if (o) o->storage.persist_keep = 0; \
} while(0)

#define overwriteObjectPersistent(o,pk) do { \
    if (o) o->storage.persistent = pk; \
} while(0)

#define setObjectPersistent(o) do { \
    if (o) o->storage.persistent = 1; \
} while(0)

#endif