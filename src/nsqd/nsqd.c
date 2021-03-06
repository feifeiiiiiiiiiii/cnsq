#include "nsqd.h"
#include <signal.h>

//valgrind -v --log-file=valgrind.log --tool=memcheck --leak-check=full ./nsqd.out

NSQD *n;

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

NSQD *build() {
    n = s_malloc(sizeof(NSQD));
    if(n == NULL) return NULL;

    n->topicMap = dictCreate(&keyptrDictType, NULL);
    
    n->tcpListener = buildTcpServer("0.0.0.0", 6379, 128, n); 
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
    sds cpy = sdsdup(topicName);
    topic *t = newTopic(cpy);
    dictAdd(n->topicMap, cpy, t);
    log_debug("Topic(%s): created", cpy);
    return t;
}

static void sigShutdownHandler(int sig) {
    char *msg;

    switch (sig) {
    case SIGINT:
        msg = "Received SIGINT scheduling shutdown...";
        break;
    case SIGTERM:
        msg = "Received SIGTERM scheduling shutdown...";
        break;
    default:
        msg = "Received shutdown signal, scheduling shutdown...";
    };
    log_debug("sig shut down %s", msg);
    // handler NSQD instance
    closeTcpServer(n->tcpListener);
    
    // 
    dictIterator *di;
    dictEntry *de;
    di = dictGetSafeIterator(n->topicMap);

    while((de = dictNext(di)) != NULL) {
        topic *t = dictGetVal(de);
        sds key = dictGetKey(de);
        sdsfree(key);
        closeTopic(t);
    }
    dictReleaseIterator(di);
    dictRelease(n->topicMap);
    s_free(n);
    exit(0);
}

int main(int argc, char **argv) {
    build();
    if(n == NULL) {
        log_trace("build nsqd server failed");
        return 0;
    }
    
    struct sigaction act;
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sigShutdownHandler;
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGINT, &act, NULL);

    nsqdMain(n);



    return 0;
}
