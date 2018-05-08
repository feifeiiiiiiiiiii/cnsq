OBJS = diskqueue.o

all: $(OBJS)
	ar -cru ./libdiskqueue.a ${OBJS}

diskqueue.o: diskqueue.h
	gcc -c diskqueue.c -std=gnu99

clean:
	rm -f *.a *.o *.exe
