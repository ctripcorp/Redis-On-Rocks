#include "ctrip_storage_metric.h"
#include "server.h"

void metricDebugInfo(int type, long val) {
    atomicIncr(server.storage.swap_debug_info[type].count, 1);
    atomicIncr(server.storage.swap_debug_info[type].value, val);
}