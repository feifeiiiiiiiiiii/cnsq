#ifndef TOPIC_H
#define TOPIC_H

#include "../util/sds.h"
#include "../util/queue.h"
#include "../diskqueue/diskqueue.h"
#include "../util/dict.h"

typedef struct topic {
    sds name;
    uint64_t memDepth;
    diskqueue *dq;

    ngx_queue_t memoryQueue;
    dict *channelMap;

    void *ctx;
} topic;

topic *newTopic(sds name, void *ctx);
void closeTopic(topic *t);

#endif // TOPIC_H