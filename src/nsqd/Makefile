OBJS = tcpServer.o nsqd.o topic.o message.o channel.o
LIBS = ../net/libnet.a ../util/libutil.a ../diskqueue/libdiskqueue.a
EXECS = nsqd.out

all: ${OBJS} ${NET_OBJS}
	gcc -o nsqd.out ${OBJS} ${LIBS} -g3

tcpServer.o: tcpServer.h
	gcc -c tcpServer.c -std=gnu99

nsqd.o: nsqd.h
	gcc -c nsqd.c -std=gnu99

topic.o: topic.h  common.h
	gcc -c topic.c -std=gnu99

message.o: common.h
	gcc -c message.c -std=gnu99

channel.o: channel.h
	gcc -c channel.c -std=gnu99

clean:
	rm -f ${EXECS} *.a *.o *.exe
