#ifndef CHANNEL_H
#define CHANNEL_H

#include "common.h"

typedef struct channel {
    sds name;
    sds topicName;
    RB_ENTRY(NSQMessage) inFlightPqueues;
    ngx_queue_t memoryQueue;
    diskqueue *backendQueue;
    dict *inFlightMessages;
    void *ctx;
} channel;

channel *newChannel(sds topicName, sds channelName, void *ctx);
void closeChannel(channel *ch);

#endif //