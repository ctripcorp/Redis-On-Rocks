/*
 * ctrip.h
 *
 *  Created on: Sep 21, 2017
 *      Author: mengwenchao
 */

#ifndef SRC_CTRIP_H_
#define SRC_CTRIP_H_

#define XREDIS_VERSION "2.2.0"
#define CONFIG_DEFAULT_SLAVE_REPLICATE_ALL 0

void xslaveofCommand(client *c);
void refullsyncCommand(client *c);

#ifdef REDIS_TEST
#define TEST(name) printf("test — %s\n", name);
#define TEST_DESC(name, ...) printf("test — " name "\n", __VA_ARGS__);
#define test_assert(e) do {							\
	if (!(e)) {				\
		printf(						\
		    "%s:%d: Failed assertion: \"%s\"\n",	\
		    __FILE__, __LINE__, #e);				\
		error++;						\
	}								\
} while (0)
#endif


/* client heartbeat */
typedef enum {
    HEARTBEAT_SYSTIME_IDX = 0, /* CLIENT_HEARTBEAT_SYSTIME */
    HEARTBEAT_MKPS_IDX,        /* CLIENT_HEARTBEAT_MKPS */
    NUM_HEARTBEAT_ACTIONS
} heartbeatActionsTypes;

void ctripHeartbeat(void);
void ctripDisableHeartbeat(client *c);
void ctripEnableHeartbeat(client *c, uint64_t options, long long heartbeat_period[]);

void tryRegisterClientsWriteEvent(void);

/* Reusable arrays for dirty subkeys commands */
void dirtyArraysTryAlloc(size_t n);
size_t dirtyArraysCapacity(void);
sds *dirtyArraysSubkeys(void);
size_t *dirtyArraysSublens(void);
void dirtyArraysFree(void);

#endif /* SRC_CTRIP_H_ */
