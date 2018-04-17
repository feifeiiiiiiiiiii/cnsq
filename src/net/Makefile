OBJS = anet.o ae.o
EXECS = test

all: $(OBJS)
	ar -cru ./libnet.a ${OBJS}

ae.o: ae.h
	gcc -c ae.c

anet.o: anet.h
	gcc -c anet.c

test: all
	gcc -o test.out test.c ${OBJS}

clean:
	rm -f ${EXECS} *.a *.o *.exe
