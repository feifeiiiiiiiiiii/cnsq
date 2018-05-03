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
    t->dq = New(name, DATA_PATH, MAX_BYTES_PER_FILE, MIN_MSG_SIZE, MAX_MSG_SIZE, SYNC_EVERY);
    ngx_queue_init(&t->memoryQueue);
    return t;
}

int putMessage(topic *t, NSQMessage *message) {
    log_debug("msgId = %s, data = %s", message->id, message->body);
    if(t->memDepth > MEM_QUEUE_SIZE) {
        log_debug("MEM_QUEUE_OVERFLOW");
        //putData(t->dq, (char *)message, strlen(message));
        return 0;
    }
    qitem *item = s_malloc(sizeof(qitem));
    item->msg = message;
    ngx_queue_insert_tail(&t->memoryQueue, &item->queue);
    t->memDepth++;
    return 0;
}

NSQMessage *getMessage(topic *t) {
    ngx_queue_t *q;
    NSQMessage *msg;
    qitem *item;
    if(ngx_queue_empty(&t->memoryQueue)) {
        // read file
        return NULL;
    }
    q = ngx_queue_head(&t->memoryQueue);
    item = (qitem *)ngx_queue_data(q, qitem, queue);
    ngx_queue_remove(&item->queue);
    msg = item->msg;
    t->memDepth -= 1;
    s_free(item);
    return msg;
}