OBJS = log.o sds.o dict.o siphash.o uuid.o number.o
all: $(OBJS)
	ar -cru ./libutil.a ${OBJS}

log.o: log.h
	gcc -c log.c

sds.o: sds.h
	gcc -c sds.c

dict.o: dict.h
	gcc -c dict.c

siphash.o:
	gcc -c siphash.c

uuid.o: uuid.h
	gcc -c uuid.c

number.o: number.h
	gcc -c number.c

clean:
	rm -f *.a *.o *.exe
