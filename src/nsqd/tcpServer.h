#ifndef TCPSERVER_H
#define TCPSERVER_H

#include <unistd.h>
#include <errno.h>

#include "../net/ae.h"
#include "../net/anet.h"
#include "../util/sds.h"
#include "../util/log.h"

#include <string.h>

#define UNUSED(V) ((void) V)
#define PROTO_REPLY_CHUNK_BYTES (16*1024) /* 16k output buffer */
#define PROTO_IOBUF_LEN         (1024*16)  /* Generic I/O buffer size */

typedef struct client {
    int fd;
    int flags;
    int reqtype;
    sds querybuf;
    void *ctx;
} client;

typedef struct tcpServer {
    aeEventLoop *el;    // eventLoop

    int port;           // TCP listening port
    int tcp_backlog;    // TCP listen() backlog
    char ipaddr[18];       // address
    int  fd;           // tcp socket file descriptor

    int stat_numconnections;

    /* Response buffer */
    int bufpos;
    char buf[PROTO_REPLY_CHUNK_BYTES];

    void *ctx;          // context this represent NSQD Instance
} tcpServer;

tcpServer *buildTcpServer(char *ipaddr, int port, int tcp_backlog, void *context);
void tcpServerRun(tcpServer *tcpLister);

#endif //