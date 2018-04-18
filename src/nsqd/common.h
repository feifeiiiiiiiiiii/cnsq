#ifndef COMMON_H
#define COMMON_H

#include "../util/dict.h"
#include "topic.h"

typedef struct NSQD {
    void *tcpListener;
    dict *topicMap;    
} NSQD;

topic *getTopic(NSQD *n, sds topicName);

#endif //