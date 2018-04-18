#ifndef TOPIC_H
#define TOPIC_H

#include "../util/sds.h"

typedef struct topic {
    sds name;

    void *ctx;
} topic;

topic *newTopic(sds name);

#endif // TOPIC_H