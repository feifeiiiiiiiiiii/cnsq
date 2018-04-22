#ifndef COMMON_H
#define COMMON_H

#include "../util/dict.h"
#include "../util/queue.h"
#include "../util/log.h"
#include "../util/sdsalloc.h"
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
    char id[37];
    size_t body_length;
    char *body;
} NSQMessage;

NSQMessage *newMessage(const char *data, size_t data_len);
NSQMessage *nsq_decode_message(const char *data, size_t data_length);
NSQMessage *nsq_encode_message(const char *data, size_t data_length);

void free_nsq_message(NSQMessage *msg);

topic *getTopic(NSQD *n, sds topicName);
int putMessage(topic *t, NSQMessage *message);
NSQMessage *getMessage(topic *t);

#endif //