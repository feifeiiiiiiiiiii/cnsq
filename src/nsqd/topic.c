#include "topic.h"

topic *newTopic(sds name) {
    topic *t = sds_malloc(sizeof(topic));
    t->name = sdsempty();
    sdscatsds(t->name, name);
    return t;
}