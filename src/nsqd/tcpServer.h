#ifndef TCPSERVER_H
#define TCPSERVER_H

#include <unistd.h>
#include <errno.h>

#include "../net/ae.h"
#include "../net/anet.h"
#include "../util/sds.h"
#include "../util/log.h"
#include <string.h>

#define PROTO_INIT 1
#define PROTO_PUB 2
#define PROTO_SUB 3
#define PROTO_FIN 4
#define PROTO_POP 5

#define STATE_INIT 1
#define STATE_CLOSE 2
#define STATE_SUBSCRIBED 3

#define REDIS_CLOSE_AFTER_REPLY (1<<6)

#define C_ERR 1
#define C_OK 2

#define UNUSED(V) ((void) V)
#define PROTO_REPLY_CHUNK_BYTES (16*1024) /* 16k output buffer */
#define PROTO_IOBUF_LEN         (1024*16)  /* Generic I/O buffer size */

typedef struct client client;

typedef int protoProc(client *c, sds *tokens, int count);

typedef struct client {
    int fd;
    int flags;
    int proto_type;
    int state;
    sds querybuf;
    protoProc *execProc;
    void *ctx;

    int total_sent_bytes;  
    /* Response buffer */
    sds buf;
} client;

typedef struct tcpServer {
    aeEventLoop *el;    // eventLoop

    int port;           // TCP listening port
    int tcp_backlog;    // TCP listen() backlog
    char ipaddr[18];       // address
    int  fd;           // tcp socket file descriptor

    int stat_numconnections;

    void *ctx;          // context this represent NSQD Instance
} tcpServer;

tcpServer *buildTcpServer(char *ipaddr, int port, int tcp_backlog, void *context);
void tcpServerRun(tcpServer *tcpLister);

void addReplyString(client *c, const char *s, size_t len);
void addReplyErrorLength(client *c, const char *s, size_t len);
void addReplyError(client *c, const char *err);

#endif //