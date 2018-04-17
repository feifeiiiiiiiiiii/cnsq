#include "nsqd.h"
#include "../util/log.h"

NSQD *build() {
    NSQD *n = malloc(sizeof(NSQD));
    if(n == NULL) return NULL;

    n->tcpListener = buildTcpServer("127.0.0.1", 6379, 128, n); 
    if(n->tcpListener == NULL) {
        free(n);
        return NULL;
    }
    return n;
}

static void nsqdMain(NSQD *n) {
    tcpServerRun(n->tcpListener);
}

int main(int argc, char **argv) {
    NSQD *n = build();
    if(n == NULL) {
        log_trace("build nsqd server failed");
        return 0;
    }
    
    nsqdMain(n);

    return 0;
}