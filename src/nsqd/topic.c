#include "topic.h"
#include "common.h"
#include "channel.h"

typedef struct {
    NSQMessage *msg;
    ngx_queue_t queue;
} qitem;

static uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

static int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2) {
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static dictType keyptrDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

topic *newTopic(sds name, void *ctx) {
    Option *opt = (Option *)ctx;
    topic *t = s_malloc(sizeof(topic));
    t->name = sdsdup(name);
    t->memDepth = 0;
    t->dq = New(name, opt->dataPath, opt->maxBytesPerFile, opt->minMsgSize, opt->maxMsgSize, opt->syncEvery);
    t->memDepth = opt->memQueueSize;
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

void closeTopic(topic *t) {
    if(t->name != NULL) sdsfree(t->name);
    if(t->dq != NULL) closeDq(t->dq);
    s_free(t);
}