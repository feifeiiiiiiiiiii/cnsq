OBJS = tcpServer.o nsqd.o
LIBS = ../net/libnet.a ../util/libutil.a
EXECS = nsqd

all: ${OBJS} ${NET_OBJS}
	gcc -o nsqd ${OBJS} ${LIBS}

tcpServer.o: tcpServer.h
	gcc -c tcpServer.c

nsqd.o: nsqd.h
	gcc -c nsqd.c

clean:
	rm -f ${EXECS} *.a *.o *.exe
