#ifndef CHANNEL_H
#define CHANNEL_H

#include "common.h"

typedef struct channel {
    sds name;
    sds topicName;
    RB_ENTRY(NSQMessage) inFlightPqueues;
    uint64_t    memDepth;
    ngx_queue_t memoryQueue;
    diskqueue *backendQueue;
    dict *inFlightMessages;
    void *ctx;
} channel;

channel *newChannel(sds topicName, sds channelName, void *opt, void *ctx);
void putToChanMessage(channel *ch, NSQMessage *msg);
void closeChannel(channel *ch);

#endif //