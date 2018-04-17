#ifndef TCPSERVER_H
#define TCPSERVER_H

#include "../net/ae.h"
#include "../net/anet.h"
#include <string.h>

typedef struct tcpServer {
    aeEventLoop *el;    // eventLoop

    int port;           // TCP listening port
    int tcp_backlog;    // TCP listen() backlog
    char ipaddr[18];       // address
    int  fd;           // tcp socket file descriptor

    void *ctx;          // context this represent NSQD Instance
} tcpServer;

tcpServer *buildTcpServer(char *ipaddr, int port, int tcp_backlog, void *context);
void tcpServerRun(tcpServer *tcpLister);

#endif //