#include "topic.h"
#include "common.h"

typedef struct {
    NSQMessage *msg;
    ngx_queue_t queue;
} qitem;

topic *newTopic(sds name) {
    topic *t = s_malloc(sizeof(topic));
    t->name = sdsempty();
    sdscatsds(t->name, name);
    t->memDepth = 0;
    t->dq = New(name, "/tmp", 1024 * 1024, 28, 1024, 10);
    ngx_queue_init(&t->memoryQueue);
    return t;
}

int putMessage(topic *t, NSQMessage *msg) {
    log_debug("putMessage msgId = %s", msg->id);
    uint32_t len = MSG_HEADER_LEN + msg->body_length;
    char *buf = malloc(len + 1);

    nsq_encode_message(msg, buf);
    putData(t->dq, buf, len);
    return 0;

    /*
    qitem *item = s_malloc(sizeof(qitem));
    item->msg = message;
    ngx_queue_insert_tail(&t->memoryQueue, &item->queue);
    t->memDepth++;
    return 0;*/
}

NSQMessage *getMessage(topic *t) {
    uint32_t len;
    char *buf;
    buf = readData(t->dq, &len);
    if(buf == NULL) {
        return NULL;
    }
    NSQMessage *msg = nsq_decode_message(buf, len);

    return msg;
}