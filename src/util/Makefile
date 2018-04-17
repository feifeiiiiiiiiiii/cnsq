OBJS = log.o sds.o
all: $(OBJS)
	ar -cru ./libutil.a ${OBJS}

log.o: log.h
	gcc -c log.c

sds.o: sds.h
	gcc -c sds.c

clean:
	rm -f *.a *.o *.exe
