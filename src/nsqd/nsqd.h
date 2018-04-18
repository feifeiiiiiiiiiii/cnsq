#ifndef NSQD_H
#define NSQD_H

#include "tcpServer.h"
#include "common.h"

NSQD *build();

topic *getTopic(NSQD *n, sds topicName);

#endif // NSQD_H