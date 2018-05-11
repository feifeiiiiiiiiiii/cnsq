#include "options.h"
#include "../util/sdsalloc.h"

Option *buildOpt() {
    Option *opt = (Option *)s_malloc(sizeof(Option));
    if(opt == NULL) return NULL;

    // hard code
    opt->dataPath = sdsnew("/tmp");
    opt->tcpAddress = sdsnew("127.0.0.1");
    opt->backlog = 128;
    opt->listenPort = 6379;
    opt->maxBytesPerFile = 1024 * 1024;
    opt->maxMsgSize = 1024;
    opt->minMsgSize = 28;
    opt->memQueueSize = 100;
    opt->syncEvery = 6;

    return opt;
}

void freeOpt(Option *opt) {
    sdsfree(opt->dataPath);
    sdsfree(opt->tcpAddress);
    s_free(opt);
}