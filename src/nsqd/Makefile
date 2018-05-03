OBJS = tcpServer.o nsqd.o topic.o message.o
LIBS = ../net/libnet.a ../util/libutil.a ../diskqueue/libdiskqueue.a
EXECS = nsqd.out

all: ${OBJS} ${NET_OBJS}
	gcc -o nsqd.out ${OBJS} ${LIBS}

tcpServer.o: tcpServer.h
	gcc -c tcpServer.c

nsqd.o: nsqd.h
	gcc -c nsqd.c

topic.o: topic.h  common.h
	gcc -c topic.c

message.o: common.h
	gcc -c message.c

clean:
	rm -f ${EXECS} *.a *.o *.exe
