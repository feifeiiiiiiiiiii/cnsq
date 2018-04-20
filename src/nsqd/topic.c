#include "topic.h"
#include "common.h"
#include "../util/log.h"

topic *newTopic(sds name) {
    topic *t = sds_malloc(sizeof(topic));
    t->name = sdsempty();
    sdscatsds(t->name, name);
    ngx_queue_init(&t->memoryQueue);
    return t;
}

int putMessage(topic *topic, NSQMessage *message) {
    log_debug("msgId = %s, data = %s", message->id, message->body);
    return 1;
}