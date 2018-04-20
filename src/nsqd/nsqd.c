#include "nsqd.h"

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

dictType keyptrDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

NSQD *build() {
    NSQD *n = s_malloc(sizeof(NSQD));
    if(n == NULL) return NULL;

    n->topicMap = dictCreate(&keyptrDictType, NULL);
    
    n->tcpListener = buildTcpServer("127.0.0.1", 6379, 128, n); 
    if(n->tcpListener == NULL) {
        s_free(n);
        return NULL;
    }
    return n;
}

static void nsqdMain(NSQD *n) {
    tcpServerRun(n->tcpListener);
}

topic *getTopic(NSQD *n, sds topicName) {
    dictEntry *entry = dictFind(n->topicMap, topicName);
    if(entry != NULL) {
        return dictGetVal(entry);
    }
    topic *t = newTopic(topicName);
    dictAdd(n->topicMap, topicName, t);
    log_debug("Topic(%s): created", topicName);
    return t;
}

int main(int argc, char **argv) {
    NSQD *n = build();
    if(n == NULL) {
        log_trace("build nsqd server failed");
        return 0;
    }
    
    nsqdMain(n);



    return 0;
}