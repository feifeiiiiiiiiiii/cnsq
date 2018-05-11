#include "channel.h"
#include "../util/queue.h"
#include "options.h"

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

channel *newChannel(sds topicName, sds channelName, void *ctx) {
    Option *opt = (Option *)ctx;
    channel *ch = s_malloc(sizeof(channel));
    if(ch == NULL) return NULL;

    ch->name = sdsdup(channelName);
    ch->topicName = sdsdup(topicName);
    ch->inFlightMessages = dictCreate(&keyptrDictType, NULL);
    ch->backendQueue = New(channelName, opt->dataPath, opt->maxBytesPerFile, opt->minMsgSize, opt->maxMsgSize, opt->syncEvery);
    ngx_queue_init(&ch->memoryQueue);
    return NULL;
}

void closeChannel(channel *ch) {
    if(ch->name != NULL) sdsfree(ch->name);
    if(ch->topicName != NULL) sdsfree(ch->topicName);
    if(ch->backendQueue != NULL) closeDq(ch->backendQueue);
    dictRelease(ch->inFlightMessages);
    s_free(ch);
}