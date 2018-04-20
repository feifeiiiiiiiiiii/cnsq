#include "topic.h"
#include "common.h"

topic *newTopic(sds name) {
    topic *t = sds_malloc(sizeof(topic));
    t->name = sdsempty();
    sdscatsds(t->name, name);
    ngx_queue_init(&t->memoryQueue);
    return t;
}

int putMessage(topic *topic, NSQMessage *message) {
    return 1;
}