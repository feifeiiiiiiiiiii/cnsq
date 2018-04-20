#ifndef COMMON_H
#define COMMON_H

#include "../util/dict.h"
#include "topic.h"
#include <stdarg.h>
#include <string.h>
#include <time.h>

typedef struct NSQD {
    void *tcpListener;
    dict *topicMap;    
} NSQD;

typedef struct NSQMessage {
    int64_t timestamp;
    uint16_t attempts;
    char id[16+1];
    size_t body_length;
    char *body;
} NSQMessage;

NSQMessage *nsq_decode_message(const char *data, size_t data_length);
NSQMessage *nsq_encode_message(const char *data, size_t data_length);

void free_nsq_message(NSQMessage *msg);

topic *getTopic(NSQD *n, sds topicName);
int putMessage(topic *topic, NSQMessage *message);

#endif //