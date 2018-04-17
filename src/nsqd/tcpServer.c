#include "tcpServer.h"
#include "../util/sdsalloc.h"
 
static char neterr[256];

static void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void acceptCommonHandler(tcpServer *server, int fd, int flags, char *ip);
static client *createClient(aeEventLoop *el, int fd);
static void freeClient(client *c);
static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
static void processInputBuffer(client *c);

static void freeClient(client *c) {
	log_debug("freeClient");
	if(c->fd != -1) {
		tcpServer *listener = c->ctx;
        /* Unregister async I/O handlers and close the socket. */
        aeDeleteFileEvent(listener->el, c->fd, AE_READABLE);
        aeDeleteFileEvent(listener->el, c->fd, AE_WRITABLE);
        close(c->fd);
        c->fd = -1;
    }
	sdsfree(c->querybuf);
	s_free(c);
}

static void processInputBuffer(client *c) {
	while(sdslen(c->querybuf)) {
		if(!c->reqtype) {
			if(sdslen(c->querybuf) < 4) {
				break;
			}

			/* magic str */
			char protoMagic[5];
			memcpy(protoMagic, c->querybuf, 4);
			protoMagic[4] = '\0';

			log_debug("Get magic string (%s) %d", protoMagic, strlen(protoMagic));

			if(memcmp(protoMagic, "  V2", 4) != 0) {
				log_error("client(%d) bad protocol magic '%s'", c->fd, protoMagic);
				close(c->fd);
				freeClient(c);
				break;
			}
			log_debug("current len = %d", sdslen(c->querybuf));
			sdsrange(c->querybuf, 4, -1);
			c->reqtype = 1;
		}
		break;
	}
}

static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
	client *c = (client*) privdata;
	int nread, readlen;
	size_t qblen;
	UNUSED(el);
	UNUSED(mask);
	readlen = PROTO_IOBUF_LEN;

	qblen = sdslen(c->querybuf);
	c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);

	nread = read(fd, c->querybuf+qblen, readlen);

    if (nread == -1) {
        if (errno == EAGAIN) {
            return;
        } else {
            log_error("Reading from client: %s",strerror(errno));
            freeClient(c);
			return;
        }
    } else if (nread == 0) {
        log_error("Client closed connection");
        freeClient(c);
        return;
    }
	sdsIncrLen(c->querybuf, nread);
	log_debug("read data %d %s", nread, c->querybuf);
	// process
	processInputBuffer(c);
}

static client *createClient(aeEventLoop *el, int fd) {
    client *c = s_malloc(sizeof(client));
	if (fd != -1) {
		anetNonBlock(NULL,fd);
		anetEnableTcpNoDelay(NULL, fd);
		if (aeCreateFileEvent(el, fd, AE_READABLE,
            readQueryFromClient, c) == AE_ERR)
        {
            close(fd);
            s_free(c);
            return NULL;
        }
	}
	c->fd = fd;
	c->querybuf = sdsempty();
	return c;
}

static void acceptCommonHandler(tcpServer *server, int fd, int flags, char *ip) {
	client *c;
	if((c = createClient(server->el, fd)) == NULL) {
		log_error("Error registering fd event for the new client: %s (fd=%d)", strerror(errno),fd);
		close(fd); /* redis May be already closed, just ignore errors */
		return;
	}
	c->ctx = server;
	server->stat_numconnections++;
	c->flags |= flags;
}

static void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
	tcpServer *tcpListener = (tcpServer *)privdata;

	int cfd, cport, max = 100;
	char cip[46];

	while (max--) {
		cfd = anetTcpAccept(neterr, fd, cip, sizeof(cip), &cport);
		if (cfd == -1) {
			if(errno != EWOULDBLOCK) {
				log_debug("Accepting client connection found EWOULDBLOCK");
			}
			return;
		}
		log_trace("Accept %s %d", cip, cport);
		acceptCommonHandler(tcpListener, cfd, 0, cip);
	}
}

tcpServer *buildTcpServer(char *ipaddr, int port, int backlog, void *context) {
    tcpServer *tcpListener = s_malloc(sizeof(tcpServer));
    if(tcpListener == NULL) {
        return NULL;
    }
    tcpListener->port = port;
    tcpListener->tcp_backlog = backlog;
    memcpy(tcpListener->ipaddr, ipaddr, strlen(ipaddr));
    tcpListener->ctx = context;

    int fd;
	tcpListener->el = aeCreateEventLoop(1024);

	fd = anetTcpServer(neterr, port, ipaddr, backlog);
	if (fd == -1) {
		log_error("anetTcpServer error %s", neterr);
        goto failed;
	}
    anetNonBlock(NULL, fd);

    tcpListener->fd = fd;

	if (aeCreateFileEvent(tcpListener->el, fd, AE_READABLE, acceptTcpHandler, tcpListener) == -1) {
		log_error("aeCreateFileEvent failed");
		goto failed;
	}
    return tcpListener;   
failed:
    s_free(tcpListener);
    return NULL;
}

void tcpServerRun(tcpServer *tcpLister) {
	log_debug("nsqd server listening in %s:%d\n", tcpLister->ipaddr, tcpLister->port);
    aeMain(tcpLister->el);
}
