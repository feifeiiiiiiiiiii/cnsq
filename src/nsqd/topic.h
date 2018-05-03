#ifndef TOPIC_H
#define TOPIC_H

#include "../util/sds.h"
#include "../util/queue.h"
#include "../diskqueue/diskqueue.h"

typedef struct topic {
    sds name;
    uint64_t memDepth;
    diskqueue *dq;

    ngx_queue_t memoryQueue;

    void *ctx;
} topic;

topic *newTopic(sds name);

#endif // TOPIC_H