/*
 * ctrip.h
 *
 *  Created on: Sep 21, 2017
 *      Author: mengwenchao
 */

#ifndef SRC_CTRIP_H_
#define SRC_CTRIP_H_

#define XREDIS_VERSION "2.0.1"
#define CONFIG_DEFAULT_SLAVE_REPLICATE_ALL 0

void xslaveofCommand(client *c);
void refullsyncCommand(client *c);
void debugSwapOutCommand(client *c);

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

#endif /* SRC_CTRIP_H_ */
