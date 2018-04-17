#ifndef NSQD_H
#define NSQD_H

#include "tcpServer.h"

typedef struct NSQD {
    tcpServer *tcpListener;    
} NSQD;

NSQD *build();

#endif // NSQD_H