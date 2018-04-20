#include "tcpServer.h"
#include "common.h"
#include "../util/sdsalloc.h"
 
static char neterr[256];

static void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void acceptCommonHandler(tcpServer *server, int fd, int flags, char *ip);
static client *createClient(aeEventLoop *el, int fd);
static void freeClient(client *c);
static void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask);
static void processInputBuffer(client *c);
static int prepareClientToWrite(client *c);
static int _addReplyToBuffer(client *c, const char *s, size_t len);

void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
	client *c = privdata;
    int nwritten = 0;
    size_t objmem;
	UNUSED(el);
	UNUSED(mask);
	
	while(sdslen(c->buf)) {
		nwritten = write(fd,c->buf,sdslen(c->buf));
		if (nwritten <= 0) break;

		c->total_sent_bytes += nwritten;
		sdsrange(c->buf, nwritten, -1);
	}
	if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            log_error("Error writing to client: %s", strerror(errno));
            freeClient(c);
            return;
        }
    }
	if (sdslen(c->buf) == 0) {
        aeDeleteFileEvent(el,c->fd, AE_WRITABLE);
        /* Close connection after entire reply has been sent. */
        if (c->flags & REDIS_CLOSE_AFTER_REPLY) freeClient(c);
    }
}

static int prepareClientToWrite(client *c) {
	if (c->fd <= 0) return C_ERR; /* Fake client */
	tcpServer *serv = (tcpServer *)c->ctx;

    if (sdslen(c->buf) == 0 &&
        aeCreateFileEvent(serv->el, c->fd, AE_WRITABLE,
        sendReplyToClient, c) == AE_ERR) return C_ERR;
    return C_OK;
}

static int _addReplyToBuffer(client *c, const char *s, size_t len) {
	c->buf = sdsMakeRoomFor(c->buf, len);
	sdscatlen(c->buf, s, len);
    return C_OK;
}

void addReplyString(client *c, const char *s, size_t len) {
	if (prepareClientToWrite(c) != C_OK) return;
    _addReplyToBuffer(c,s,len);
}

void addReplyErrorLength(client *c, const char *s, size_t len) {
	addReplyString(c,s,len);
}

void addReplyError(client *c, const char *err) {
    addReplyErrorLength(c, err, strlen(err));
}


int fin(client *c, sds *tokens, int count);
int sub(client *c, sds *tokens, int count);
int pub(client *c, sds *tokens, int count);

static void freeClient(client *c) {
	log_debug("freeClient: Client has sent total %d bytes", c->total_sent_bytes);
	if(c->fd != -1) {
		tcpServer *listener = c->ctx;
        /* Unregister async I/O handlers and close the socket. */
        aeDeleteFileEvent(listener->el, c->fd, AE_READABLE);
        aeDeleteFileEvent(listener->el, c->fd, AE_WRITABLE);
        close(c->fd);
        c->fd = -1;
    }
	sdsfree(c->buf);
	sdsfree(c->querybuf);
	s_free(c);
}

static void processInputBuffer(client *c) {
	char *newline = NULL;
	sds *tokens;
	int count;
	int rangeLen = 0;
	
	while(sdslen(c->querybuf)) {
		if(!c->proto_type) {
			if(sdslen(c->querybuf) < 4) {
				break;
			}
			/* magic str [space][space][V][2] */
			char protoMagic[5];
			memcpy(protoMagic, c->querybuf, 4);
			protoMagic[4] = '\0';

			log_debug("Get magic string (%s) %d", protoMagic, strlen(protoMagic));

			if(memcmp(protoMagic, "  V2", 4) != 0) {
				c->flags |= REDIS_CLOSE_AFTER_REPLY;
				addReplyError(c, "bad protocol magic");
				log_error("client(%d) bad protocol magic '%s'", c->fd, protoMagic);
				break;
			}
			log_debug("current len = %d", sdslen(c->querybuf));
			sdsrange(c->querybuf, 4, -1);
			c->proto_type = PROTO_INIT;
			c->state = STATE_INIT;
		}
		if(c->proto_type == PROTO_INIT) {
			newline = (char *)memchr(c->querybuf,'\n', sdslen(c->querybuf));
			if(newline == NULL) {
				break;
			}
			rangeLen = (newline - c->querybuf);
			tokens = sdssplitlen(c->querybuf, rangeLen, " ", 1, &count);
			if(sdscmp(tokens[0], sdsnew("SUB")) == 0) {
				c->proto_type = PROTO_SUB;
				c->execProc = sub;
			} else if(sdscmp(tokens[0], sdsnew("PUB")) == 0) {
				c->proto_type = PROTO_PUB;
				c->execProc = pub;
			} else if(sdscmp(tokens[0], sdsnew("FIN")) == 0) {
				c->proto_type = PROTO_FIN;
				c->execProc = fin;
			} else if(sdscmp(tokens[0], sdsnew("GET")) == 0) {
			} else {
				addReplyError(c, "bad command");
				log_error("client(%d) bad command '%s'", c->fd, tokens[0]);
				freeClient(c);
				break;
			}
		}
		if(c->execProc) {
			if(c->execProc(c, tokens, count) == C_OK) {
				sdsrange(c->querybuf, rangeLen+1, -1);
			} else {
				break;
			}
		} else {
			break;
		}
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
	c->total_sent_bytes = 0;
	c->proto_type = 0;
	c->flags = 0;
	c->state = 0;
	c->buf = sdsempty();
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
	int slen = strlen(ipaddr);
    tcpListener->port = port;
    tcpListener->tcp_backlog = backlog;
    memcpy(tcpListener->ipaddr, ipaddr, slen);
	tcpListener->ipaddr[slen] = '\0';
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
	log_debug("nsqd server listening in %s:%d", tcpLister->ipaddr, tcpLister->port);
    aeMain(tcpLister->el);
}

int fin(client *c, sds *tokens, int count) {
	c->execProc = NULL;
	c->proto_type = PROTO_INIT;
	return C_OK;
}

/*
 * SUB <topic_name> <channel_name>\n
 **/
int sub(client *c, sds *tokens, int count) {
	// not allow same client repeat sub
	if(c->state != STATE_INIT) {
		log_error("E_INVALID1: cannot SUB in current state");
		goto failed;
	}

	if(count < 3) {
		log_error("E_INVALID2: SUB insufficient number of parameters");
		goto failed;
	}

	tcpServer *serv = (tcpServer *)c->ctx;
	NSQD *nsqd = (NSQD *)serv->ctx;
	
	getTopic(nsqd, tokens[1]);

	c->state = STATE_SUBSCRIBED;
	c->execProc = NULL;
	c->proto_type = PROTO_INIT;
	log_debug("add reply string = ok");
	addReplyString(c, "OK", 2);
	return C_OK;
failed:
	c->proto_type = PROTO_INIT;
	c->execProc = NULL;
	addReplyError(c, "E_INVALID");
	return C_ERR;
}

/*
 *	PUB <topic_name>\n
 *	[ 4-byte size in bytes ][ N-byte binary data ]
 **/
int pub(client *c, sds *tokens, int count) { 
	if(count < 2) {
		addReplyError(c, "E_INVALID");
		log_debug("E_INVALID: PUB insufficient number of parameters");
		goto failed;
	}
	
	sds topicName = tokens[1];

	// read byte
	size_t hasread = 0;
	for(int i = 0; i < count; i++) {
		hasread += sdslen(tokens[i]);
	}
	hasread += 2; // "\n"

	if((sdslen(c->querybuf) - hasread) < 4) {
		log_debug("PUB: need more data");
		goto failed;
	}
	uint32_t *bodyLen, len;
	log_debug("read = %d %d", sdslen(c->querybuf), hasread);
	bodyLen = (uint32_t *)(c->querybuf + hasread);
	len = ntohl(*bodyLen);
	if(len > (sdslen(c->querybuf) - hasread - 4)) {
		goto failed;
	}

	tcpServer *serv = (tcpServer *)c->ctx;
	topic *t = getTopic(serv->ctx, topicName);
	NSQMessage *msg = nsq_encode_message(c->querybuf + hasread + 4, len);

	int ret = putMessage(t, msg);

	if(ret == 0) {
		addReplyString(c, "OK", 2);
	} else {
		addReplyError(c, "E_PUB_FAILED");
	}
	sdsrange(c->querybuf, 4+len, -1);
	c->execProc = NULL;
	c->proto_type = PROTO_INIT;
	return C_OK;
failed:
	c->execProc = NULL;
	c->proto_type = PROTO_INIT;
	return C_ERR;
}

/*
 * GET <topic_name> <channel_name>\n
 **/
int get(client *c, sds *tokens, int count) {
	if(c->state != STATE_SUBSCRIBED) {
		log_error("E_INVALID: cannot GET in current state");
		goto failed;
	}

	if(count < 3) {
		log_error("E_INVALID2: GET insufficient number of parameters");
		goto failed;
	}

	c->execProc = NULL;
	c->proto_type = PROTO_INIT;
	return C_OK;
failed:
	c->execProc = NULL;
	addReplyError(c, "E_INVALID");
	c->proto_type = PROTO_INIT;
	return C_ERR;
}
