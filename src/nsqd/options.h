#ifndef OPTIONS_H
#define OPTIONS_H

#include "../util/sds.h"
#include <stdio.h>

typedef struct Option {    
    sds tcpAddress;
    int listenPort;
    int backlog;

    sds dataPath;
    uint64_t memQueueSize;
    uint64_t maxBytesPerFile;
    uint32_t minMsgSize;
    uint32_t maxMsgSize;
    uint32_t syncEvery;
} Option;

Option *buildOpt();
void freeOpt(Option *opt);

#endif //