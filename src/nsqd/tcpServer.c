#include "tcpServer.h"
#include "../util/log.h"
 
static char neterr[256];

static void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);

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
    tcpServer *tcpListener = malloc(sizeof(tcpServer));
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
    log_trace("build tcp server success");
    return tcpListener;   
failed:
    free(tcpListener);
    return NULL;
}

void tcpServerRun(tcpServer *tcpLister) {
    aeMain(tcpLister->el);
}
