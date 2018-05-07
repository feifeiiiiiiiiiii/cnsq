OBJS = anet.o ae.o zmalloc.o
EXECS = test

all: $(OBJS)
	ar -cru ./libnet.a ${OBJS}

ae.o: ae.h
	gcc -c ae.c

anet.o: anet.h
	gcc -c anet.c

zmalloc.o: zmalloc.h
	gcc -c zmalloc.c

test: all
	gcc -o test.out test.c ${OBJS}

clean:
	rm -f ${EXECS} *.a *.o *.exe
