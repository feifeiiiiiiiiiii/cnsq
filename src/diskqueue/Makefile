OBJS = diskqueue.o

all: $(OBJS)
	ar -cru ./libdiskqueue.a ${OBJS}

diskqueue.o: diskqueue.h
	gcc -c diskqueue.c

clean:
	rm -f *.a *.o *.exe
