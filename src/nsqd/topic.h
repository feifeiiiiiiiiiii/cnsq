#ifndef TOPIC_H
#define TOPIC_H

#include "../util/sds.h"
#include "../util/queue.h"

typedef struct topic {
    sds name;
    ngx_queue_t memoryQueue;
    
    void *ctx;
} topic;

topic *newTopic(sds name);

#endif // TOPIC_H