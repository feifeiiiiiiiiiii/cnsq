OBJS = log.o
all: $(OBJS)
	ar -cru ./libutil.a ${OBJS}

log.o: log.h
	gcc -c log.c

clean:
	rm -f *.a *.o *.exe
