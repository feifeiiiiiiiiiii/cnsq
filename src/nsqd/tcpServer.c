#include "tcpServer.h"
#include "../util/sdsalloc.h"
 
static char neterr[256];

static void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void acceptCommonHandler(aeEventLoop *el, int fd, int flags, char *ip);
static client *createClient(aeEventLoop *el, int fd);
static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);

static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {}

static client *createClient(aeEventLoop *el, int fd) {
    client *c = s_malloc(sizeof(client));
	if (fd != -1) {
		anetNonBlock(NULL,fd);
		anetEnableTcpNoDelay(NULL,fd);
		if (aeCreateFileEvent(el,fd, AE_READABLE,
            readQueryFromClient, c) == AE_ERR)
        {
            close(fd);
            s_free(c);
            return NULL;
        }

	}
	return c;
}

static void acceptCommonHandler(aeEventLoop *el, int fd, int flags, char *ip) {
	client *c;
	if((c = createClient(el, fd)) == NULL) {
		return;
	}
}

static void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
	log_trace("acceptTcpHandler");

	int cfd, cport, max = 100;
	char cip[100];

	while (max--)
	{
		cfd = anetTcpAccept(neterr, fd, cip, sizeof(cip), &cport);
		if (cfd == -1)
		{
			return;
		}
		log_trace("accept success %s %d", cip, cport);
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
		log_trace("anetTcpServer error %s", neterr);
        goto failed;
	}
    anetNonBlock(NULL, fd);

    tcpListener->fd = fd;

	if (aeCreateFileEvent(tcpListener->el, fd, AE_READABLE, acceptTcpHandler, NULL) == -1) {
		log_trace("aeCreateFileEvent failed");
		goto failed;
	}
    return tcpListener;   
failed:
    s_free(tcpListener);
    return NULL;
}

void tcpServerRun(tcpServer *tcpLister) {
	log_trace("nsqd server listening in %s:%d\n", tcpLister->ipaddr, tcpLister->port);
    aeMain(tcpLister->el);
}
